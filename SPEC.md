# LinearJC Specification

**Version**: 0.8.0-draft
**Last Updated**: 2026-01-13

---

## Overview

LinearJC is a lightweight batch control system inspired by mainframe batch processing (JCL/COBOL). It runs job modules that declare the named data registers they read and write, then uses those data contracts to validate ownership, protect shared data, and derive predictable execution order.

### Why LinearJC Exists

Modern orchestration tools (Airflow, Nomad, Kubernetes) optimize for flexibility and scale. LinearJC optimizes for **simplicity and correctness** by removing freedoms:

- **Explicit data contracts**: Jobs declare exactly which registers they read and write
- **Register-driven execution**: Data readiness is the primary dependency model
- **Single writer rule**: Each data register has exactly one owner job
- **Immutable registry**: Data locations are defined once, never change
- **Minimal coordinator**: Intelligence lives in tooling, not runtime

### Design Principles

1. **Constraints over flexibility** - Fewer choices means fewer mistakes
2. **Validate early** - Catch errors in `ljc` before deployment
3. **Dumb coordinator** - Once stable, coordinator code is frozen
4. **Self-contained packages** - Jobs bundle everything they need

### System Components

```
┌─────────────────────────────────────────────────────────────────┐
│  ljc (Developer Tool)                                           │
│  • Single source of truth for jobs and registry                 │
│  • Full validation before deployment                            │
│  • Builds self-contained .ljc packages                          │
│  • Local testing with `ljc test` (uses linearjc-core)           │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ push registry + deploy packages
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Coordinator                                                    │
│  • Receives validated artifacts                                 │
│  • Builds execution trees from register ownership and chains     │
│  • Schedules jobs, manages register locks                       │
│  • Dispatches work to executors                                 │
└─────────────────────────────────────────────────────────────────┘
                              │
                              │ MQTT job requests
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Executor(s)                                                    │
│  • Runs on plain Linux (not containerized)                      │
│  • Thin wrapper around linearjc-core                            │
│  • Executes jobs with optional isolation (Landlock, cgroups)    │
│  • Reports progress and results via MQTT                        │
└─────────────────────────────────────────────────────────────────┘
```

### Shared Core Architecture

The security-critical execution code is shared between `ljc` and the executor:

```
┌─────────────────────────────────────────────────────────────────┐
│  linearjc-core (shared Rust crate)                              │
│  ══════════════════════════════════                             │
│  • workdir.rs    - Workdir creation and setup                   │
│  • isolation.rs  - Landlock, cgroups, network namespaces        │
│  • execution.rs  - Fork, setuid, exec                           │
│  • archive.rs    - Tar/gzip handling                            │
│                                                                 │
│  Small, auditable, security-critical code                       │
└─────────────────────────────────────────────────────────────────┘
                    │                           │
          ┌────────┴────────┐         ┌────────┴────────┐
          ▼                 ▼         ▼                 ▼
┌─────────────────────┐   ┌─────────────────────────────────────┐
│  Executor           │   │  ljc                                │
│  ═════════          │   │  ═══                                │
│  • MQTT client      │   │  • Repository management            │
│  • Input download   │   │  • Validation                       │
│  • Output upload    │   │  • Package building                 │
│  • Progress report  │   │  • MQTT deployment                  │
│                     │   │  • `ljc test` command               │
│  Thin, minimal      │   │                                     │
└─────────────────────┘   └─────────────────────────────────────┘
```

**Benefits:**
- **Single source of truth** for isolation logic - same Landlock rules in dev and prod
- **Executor stays minimal** - just MQTT + I/O, easy to audit
- **Identical behavior** - `ljc test --isolation strict` behaves exactly like production
- **Iterative hardening** - test with `--isolation none`, progressively tighten

### Mainframe Mental Model

LinearJC should be understood as a small batch operations system, not as a general workflow canvas.

| Mainframe concept | LinearJC concept | Notes |
|-------------------|------------------|-------|
| Cataloged dataset | Register | Named data location with declared ownership |
| Temporary dataset | Temp register | Run-scoped MinIO-backed intermediate data |
| Program / job step | Job module | Script or binary package run by an executor |
| Job net / application | Batch group | A set of modules that produce one batch result |
| GDG generation | Batch generation | One scheduled or manual run of a batch group |
| ENQ / DEQ | Register lock | Shared read locks and exclusive write locks |
| JES spool | Execution archive | Historical record of logs, inputs, outputs, and events |

The primary rule is:

> A job is eligible when all `reads:` registers are ready for the current batch generation and all `writes:` registers can be exclusively locked.

Current releases implement this through linear chains and fan-out trees. The intended evolution for multi-input work is register-driven barrier jobs: a module can read results from multiple writer modules, and it runs only when all those registers are ready for the same batch generation.

### Data Model: The Register Map

LinearJC uses a **register map** model inspired by mainframe datasets:

- **Named registers**: Each data location has a unique name
- **Three types**: `fs` (filesystem), `temp` (chain-only MinIO), `minio` (permanent MinIO)
- **Single writer**: Exactly one job can write to each register
- **Multiple readers**: Any job can read from any register
- **ENQ/DEQ locking**: Readers-writer locks prevent corruption (shared for reads, exclusive for writes)
- **Protected inputs**: External data sources marked `protect: true` cannot be written by jobs

Typical data flow:
```
[fs protected input] → job1 → [temp intermediate] → job2 → [fs output]
```

**Register Types:**

| Type | Storage | Persistence | Locking | Use Case |
|------|---------|-------------|---------|----------|
| `fs` | Filesystem | Permanent | ENQ/DEQ | Managed outputs, external inputs |
| `temp` | MinIO temp | Chain duration | None (unique path) | Intermediates between jobs |
| `minio` | MinIO bucket | Permanent | ENQ/DEQ | Large objects, cross-system |

Temp registers enable parallel fan-out by giving each tree execution its own MinIO path. They're automatically cleaned when the tree completes.

### Batch Generations

A batch generation is one scheduled or manual execution of a batch group. Temp registers are scoped to that generation so downstream modules never mix outputs from different runs.

Example:

```
generation daily.close-20260113-000001
├── sales_extract       from extract.sales
├── inventory_extract   from extract.inventory
└── daily_report        from build.report
```

For future barrier jobs, generation scoping is what makes multi-input consolidation safe: `build.report` must consume `sales_extract` and `inventory_extract` from the same generation, not whichever register was most recently written.

---

## Part 1: ljc (Developer Tool)

### Repository Structure

```
my-jobs/
├── registry.yaml        # All register definitions
├── jobs/
│   └── <job-id>/
│       ├── job.yaml     # Job definition
│       ├── script.sh    # Entry script
│       ├── bin/         # Optional bundled binaries
│       ├── data/        # Optional bundled static data
│       └── test/        # Optional test inputs (excluded from package)
├── dist/                # Built .ljc packages
└── .ljcconfig           # Tool configuration
```

### Registry (registry.yaml)

The registry defines all data registers using a compact YAML format.

```yaml
registry:
  # Protected filesystem input (external, read-only)
  source_db:     {type: fs, path: /data/source.db, kind: file, protect: true}
  config_dir:    {type: fs, path: /data/config/, kind: dir, protect: true}

  # Managed filesystem outputs (written by jobs)
  daily_report:  {type: fs, path: /data/reports/daily.csv, kind: file}
  backup_dir:    {type: fs, path: /data/backups/, kind: dir}

  # Temporary registers (chain-only, MinIO)
  intermediate:  {type: temp, kind: file}
  parsed_data:   {type: temp, kind: dir}

  # Permanent MinIO registers (large objects)
  archive:       {type: minio, bucket: job-artifacts, prefix: archives/, kind: dir}
```

**Fields by type:**

| Type | Required Fields | Optional | Description |
|------|-----------------|----------|-------------|
| `fs` | `path`, `kind` | `protect` | Filesystem path. `protect: true` = read-only external input |
| `temp` | `kind` | - | Chain-only MinIO storage. Auto-cleaned on completion |
| `minio` | `bucket`, `kind` | `prefix` | Permanent MinIO storage |

**`kind` field (all types):**
- `file`: Single file (archive must contain exactly one file)
- `dir`: Directory with contents (preserves structure)

**`protect` flag (fs only):**
- `protect: true`: External input that jobs cannot write to
  - Validated at startup (must exist)
  - Jobs can read but cannot declare in `writes:`
  - Use for databases, configs managed by other systems
- `protect: false` (default): Managed by LinearJC jobs

**Rules:**
- Register names must be unique
- Filesystem paths must not overlap
- Protected registers must exist at startup
- Each register has exactly one writer job

### Job Definition (job.yaml)

Each job has a single `job.yaml` containing all metadata:

```yaml
job:
  id: process.daily
  version: 1.0.0

  reads:  [sensor_raw, sensor_config]    # Input registers
  writes: [sensor_parsed]                 # Output register (this job owns it)

  depends: []                             # Current chain predecessor; data contracts are primary

  schedule:
    min_daily: 1                          # Minimum executions per 24h
    max_daily: 5                          # Maximum executions per 24h

  run:
    user: nobody                          # Execution user
    timeout: 300                          # Seconds
    entry: script.sh                      # Entry script (default: script.sh)
    binaries: [bin/processor]             # Optional bundled binaries

    # Process isolation (all optional, shown with defaults)
    isolation: none                       # strict | relaxed | none
    network: true                         # Allow network access
    # extra_read_paths: []                # For relaxed mode only
    # limits:                             # Resource limits (cgroups v2)
    #   cpu_percent: 100
    #   memory_mb: 512
    #   processes: 100
```

**Field Reference:**

| Field | Required | Description |
|-------|----------|-------------|
| `id` | Yes | Unique job identifier (e.g., `backup.daily`) |
| `version` | Yes | Semantic version (X.Y.Z) |
| `reads` | Yes | List of input register names |
| `writes` | Yes | List of output register names (job owns these) |
| `depends` | No | Current chain predecessor field (max 1 for linear chains). Future barrier execution derives dependencies from `reads:`/`writes:` and may use `depends` only as a validation assertion. |
| `schedule.min_daily` | Yes | Minimum runs per 24h sliding window |
| `schedule.max_daily` | Yes | Maximum runs per 24h sliding window |
| `run.user` | Yes | Unix user to run script as |
| `run.timeout` | Yes | Maximum execution time in seconds |
| `run.entry` | No | Entry script filename (default: `script.sh`) |
| `run.binaries` | No | List of bundled binary paths |
| `run.isolation` | No | Isolation mode: `strict`, `relaxed`, `none` (default: `none`) |
| `run.network` | No | Network access (default: `true`) |
| `run.extra_read_paths` | No | Additional read paths for `relaxed` mode |
| `run.limits.cpu_percent` | No | CPU limit as percentage of one core |
| `run.limits.memory_mb` | No | Memory limit in megabytes |
| `run.limits.processes` | No | Maximum process count |

### Dependency Chains, Fan-Out, and Barrier Direction

The current implementation builds execution trees from explicit `depends:` links. This supports linear chains and fan-out while preserving simple operational behavior.

Jobs can form **linear chains** where one job depends on another:

```yaml
# Linear chain: A → B → C
- id: job.a
  depends: []
  writes: [output_a]

- id: job.b
  depends: [job.a]     # Runs after job.a completes
  reads: [output_a]
  writes: [output_b]

- id: job.c
  depends: [job.b]     # Runs after job.b completes
  reads: [output_b]
```

**Fan-Out Support:**

If multiple jobs depend on the same job, separate execution trees are created:

```yaml
# Fan-out: A → B and A → C (two trees)
- id: process.data
  depends: []
  writes: [processed]

- id: analyze.spending
  depends: [process.data]  # Tree 1: [process.data, analyze.spending]

- id: export.report
  depends: [process.data]  # Tree 2: [process.data, export.report]
```

In this example, `process.data` **runs twice** - once for each consuming tree. This is useful when:
- The upstream job is fast/cheap
- You want failure isolation between branches
- Downstream jobs have different schedules

**Parallel Fan-Out with Temp Registers:**

Using `type: temp` for the shared output enables true parallel execution:

```yaml
registry:
  processed: {type: temp, kind: file}  # Not type: fs!

jobs:
  - id: process.data
    writes: [processed]   # Each tree gets unique MinIO path

  - id: analyze.spending
    depends: [process.data]
    reads: [processed]    # Reads from tree-1's MinIO path

  - id: export.report
    depends: [process.data]
    reads: [processed]    # Reads from tree-2's MinIO path
```

With `type: temp`, both fan-out trees can execute in parallel because each gets a unique MinIO path. With `type: fs`, trees would serialize (one holds exclusive lock, other waits).

**Constraints:**
- Each job can depend on **at most one** other job (no merge points)
- Cycles are not allowed (A → B → A)
- Tree schedules use the most restrictive min/max from all jobs in the chain

### Planned: Multi-Input Barrier Jobs

Some batch results are produced from multiple upstream modules. The user-facing model should remain register-driven: the downstream job declares the registers it reads, and the coordinator infers that it must wait for the modules that own those registers.

```yaml
registry:
  source_sales:       {type: fs, path: /data/source/sales.csv, kind: file, protect: true}
  source_inventory:   {type: fs, path: /data/source/inventory.csv, kind: file, protect: true}
  sales_extract:      {type: temp, kind: file}
  inventory_extract:  {type: temp, kind: file}
  daily_report:       {type: fs, path: /data/reports/daily.csv, kind: file}

jobs:
  - id: extract.sales
    reads: [source_sales]
    writes: [sales_extract]

  - id: extract.inventory
    reads: [source_inventory]
    writes: [inventory_extract]

  - id: build.report
    reads: [sales_extract, inventory_extract]
    writes: [daily_report]
```

`build.report` is a barrier job. It is eligible only when:

1. `sales_extract` is ready for the current batch generation.
2. `inventory_extract` is ready for the current batch generation.
3. `daily_report` can be exclusively locked.

This avoids two sources of truth. Users should not have to write both `reads: [sales_extract, inventory_extract]` and `depends: [extract.sales, extract.inventory]` as primary configuration. If retained, multi-value `depends:` should be treated as an assertion that the inferred register dependencies are what the author intended.

### Workdir Structure

When a job executes, the executor creates a **workdir** with a fixed, predictable structure:

```
{workdir}/
├── script.sh            # Entry script (from package)
├── bin/                 # Bundled binaries (from package)
│   └── processor
├── data/                # Bundled static data (from package)
│   └── template.json
├── in/                  # Inputs (populated by executor, read-only)
│   ├── sensor_raw/      # Directory input (kind: dir)
│   └── sensor_config    # File input (kind: file)
├── out/                 # Outputs (job writes here)
└── tmp/                 # Scratch space (job can use freely)
```

**Key design points:**
- Script always executes from workdir root
- All paths are **relative** - no environment variables needed for I/O
- Register names become paths: `in/{register_name}`, `out/{register_name}`
- With Landlock isolation, script cannot access anything outside workdir (except system libs)

### Entry Script (script.sh)

The entry script is the job's executable entry point.

**Requirements:**
- Must have a valid shebang (`#!/bin/sh`, `#!/usr/bin/python3`, etc.)
- Must exit 0 on success, non-zero on failure
- Must validate outputs exist before exiting 0

**Environment Variables (minimal, for identification only):**

| Variable | Description |
|----------|-------------|
| `LINEARJC_JOB_ID` | Job identifier (e.g., `process.daily`) |
| `LINEARJC_EXECUTION_ID` | Unique execution ID (e.g., `process.daily-20251125-143022-a1b2`) |

**Note:** No path environment variables. Scripts use relative paths (`in/`, `out/`, `tmp/`, `bin/`, `data/`).

**Example script:**

```bash
#!/bin/sh
set -e

# Read inputs (relative paths)
CONFIG=$(cat in/sensor_config)

# Process using bundled binary
./bin/processor \
    --config in/sensor_config \
    --input in/sensor_raw \
    --output out/sensor_parsed

# Use bundled data if needed
./bin/processor --template data/template.json ...

# Use scratch space
cp in/sensor_raw/* tmp/
# ... process in tmp/ ...

# Validate output exists
[ -d out/sensor_parsed ] || exit 1
exit 0
```

**Why relative paths?**
1. Simpler scripts - no env var ceremony
2. Works naturally with Landlock - strict mode prevents leaving workdir anyway
3. Predictable structure - same in development and production
4. Like Docker containers - fixed workdir layout, scripts just use it

### Process Isolation

Jobs can opt into kernel-level isolation for security:

**Isolation Modes:**

| Mode | Filesystem Access | Description |
|------|-------------------|-------------|
| `none` | Full system | No restrictions. For trusted jobs (backups, system tasks) |
| `relaxed` | Workdir + extras | Landlock allows workdir + `extra_read_paths` from config |
| `strict` | Workdir only | Landlock allows only workdir + system libs |

**Landlock Rules (strict mode):**

| Path | Access | Notes |
|------|--------|-------|
| `{workdir}/` | Read | Script, bin/, data/ |
| `{workdir}/in/` | Read | Inputs |
| `{workdir}/out/` | Read+Write | Outputs |
| `{workdir}/tmp/` | Read+Write | Scratch |
| `/lib*`, `/usr/lib*` | Read | System libraries |
| `/bin`, `/usr/bin` | Read | System binaries (for shebang interpreters) |
| Everything else | Denied | Kernel enforced |

**Additional Controls:**

| Setting | Description |
|---------|-------------|
| `network: false` | Network namespace isolation (no network access) |
| `limits.cpu_percent` | CPU limit (cgroups v2) |
| `limits.memory_mb` | Memory limit (cgroups v2) |
| `limits.processes` | Process count limit (prevents fork bombs) |

**Example with full isolation:**

```yaml
run:
  user: nobody
  timeout: 300
  isolation: strict
  network: false
  limits:
    cpu_percent: 50
    memory_mb: 512
    processes: 10
```

**Example with relaxed mode (needs extra paths):**

```yaml
run:
  user: nobody
  timeout: 300
  isolation: relaxed
  extra_read_paths:
    - /usr/local/share/geoip    # GeoIP database
    - /etc/ssl/certs            # SSL certificates
  network: true
```

**Implementation Note:** Isolation validation code should be shared between `ljc` and the executor to ensure consistent behavior during local testing and production execution.

### Package Format (.ljc)

The `.ljc` package is a gzip-compressed tar archive containing:

```
package.ljc (tar.gz)
├── job.yaml             # Job definition (for coordinator)
├── script.sh            # Entry script (executable)
├── bin/                 # Optional bundled binaries
│   └── processor
└── data/                # Optional bundled static data
    └── template.json
```

**Extraction to workdir:**

The executor extracts the package and adds I/O directories:

```
Package contents     →    Workdir
─────────────────         ───────────────────
script.sh            →    script.sh
bin/                 →    bin/
data/                →    data/
job.yaml             →    (not extracted, coordinator only)
                          in/       (created by executor)
                          out/      (created by executor)
                          tmp/      (created by executor)
```

**Excluded from package:**
- `test/` directory (local testing data)
- `job.yaml` is included in package but not extracted to workdir (coordinator metadata)

### Commands

#### Repository Management

```bash
ljc init [path]                    # Initialize new repository
ljc list                           # List all jobs with status
ljc info <job-id>                  # Show job details
```

#### Registry Management

```bash
ljc registry list                  # Show all registers
ljc registry add <name> \          # Add new register
    --type fs \
    --path /var/share/foo \
    --kind file
ljc registry add <name> \          # Add MinIO register
    --type minio \
    --bucket linearjc \
    --prefix foo/
ljc registry push                  # Push registry to coordinator
```

#### Job Development

```bash
ljc new <job-id> \                 # Create new job
    --reads reg1,reg2 \
    --writes reg3
ljc validate [job-id | --all]      # Validate job(s)
ljc build <job-id>                 # Build .ljc package
ljc bump <major|minor|patch> \     # Bump version
    <job-id>
```

#### Testing

```bash
ljc test <job-id>                  # Run job locally with test/ inputs
ljc test <job-id> --isolation none # Override isolation (iterative hardening)
ljc test <job-id> --verbose        # Show script stdout/stderr
ljc extract <package.ljc>          # Extract package to workdir (manual testing)
```

**`ljc test` - Local Job Execution**

Runs a job locally using the same `linearjc-core` code as the production executor.

**Basic usage:**
```bash
# Use job.yaml defaults
ljc test process.daily

# Iterative hardening workflow
ljc test process.daily --isolation none      # Start: no restrictions
ljc test process.daily --isolation relaxed   # Add Landlock with extras
ljc test process.daily --isolation strict    # Full lockdown
ljc test process.daily --network no          # Disable network
```

**Override flags** (override job.yaml settings for testing):

| Flag | Description |
|------|-------------|
| `--isolation <mode>` | Override isolation: `strict`, `relaxed`, `none` |
| `--network <yes\|no>` | Override network access |
| `--timeout <seconds>` | Override timeout |
| `--user <user>` | Override execution user |
| `--cpu <percent>` | Override CPU limit |
| `--memory <mb>` | Override memory limit |
| `--verbose` | Show script stdout/stderr |
| `--keep` | Keep workdir after execution (for debugging) |
| `--validate` | Compare `out/` against `test/expected/` |

**What `ljc test` does:**

1. Read `jobs/<job-id>/job.yaml`
2. Create temporary workdir:
   ```
   {tempdir}/
   ├── script.sh      ← from jobs/<job-id>/
   ├── bin/           ← from jobs/<job-id>/bin/
   ├── data/          ← from jobs/<job-id>/data/
   ├── in/            ← from jobs/<job-id>/test/in/
   ├── out/           ← created empty
   └── tmp/           ← created empty
   ```
3. Apply isolation (Landlock, cgroups, namespaces) using `linearjc-core`
4. Execute script from workdir
5. Report: exit code, runtime, files created
6. If `--validate`: diff `out/` against `test/expected/`
7. Cleanup workdir (unless `--keep`)

**Test directory structure:**

Jobs include a `test/` directory with sample inputs (excluded from package):

```
jobs/process.daily/
├── job.yaml
├── script.sh
├── bin/
├── data/
└── test/                          # Excluded from .ljc package
    ├── in/                        # Sample inputs (must match job.reads)
    │   ├── sensor_raw/            # Directory register
    │   │   └── sample.csv
    │   └── sensor_config          # File register
    └── expected/                  # Optional: expected outputs
        └── sensor_parsed/
            └── output.json
```

**Platform requirements:**

| Feature | Requirement |
|---------|-------------|
| Basic execution | Any Unix (Linux, macOS) |
| `--isolation strict/relaxed` | Linux 5.13+ (Landlock) |
| `--network no` | Linux (network namespaces) |
| Resource limits | Linux (cgroups v2) |

On unsupported platforms, `ljc test` warns and skips unavailable features.

**`ljc extract` - Manual Testing**

For manual inspection or debugging:

```bash
ljc extract dist/process.daily.ljc -o ./workdir
cd ./workdir
# Manually populate in/ with test data
./script.sh
# Inspect out/
```

#### Deployment

```bash
ljc deploy <package.ljc> \         # Deploy to coordinator
    --to <coordinator-host>
```

### Validation Rules

`ljc validate` performs comprehensive pre-deployment validation:

**Registry Validation:**
- All entries have valid type and required fields
- No duplicate register names
- Filesystem paths do not overlap

**Job Validation:**
- All `reads` registers exist in registry
- All `writes` registers exist in registry
- No two jobs write to same register (single writer rule)
- Dependencies reference existing jobs
- Dependency graph is acyclic
- Script has valid shebang
- Declared binaries exist
- Version is valid semantic version

**Cross-Job Validation:**
- No output register conflicts across all jobs
- Dependency chains are satisfiable

### Configuration (.ljcconfig)

```yaml
coordinator:
  host: 192.0.2.10
  mqtt_port: 1884

auth:
  shared_secret: ${LINEARJC_SECRET}    # From environment
```

---

## Part 2: Coordinator

### Overview

The coordinator is the central scheduler. It receives job packages from `ljc`, manages the registry, and dispatches work to executors.

**Design principle:** The coordinator is "dumb" - it stores validated artifacts, enforces schedules, and moves data. Complex logic lives in `ljc` (validation) and `linearjc-core` (execution).

### Target Register-Driven Scheduling

Inspired by mainframe JCL, LinearJC's target model is **register-driven scheduling**:

- Jobs declare what data they `read` and `write`
- Scheduler infers dependencies from register ownership
- Jobs run when: schedule permits AND inputs available AND outputs unlocked

Current releases implement linear chains and fan-out with explicit `depends:` links. The target model below removes duplicated dependency declarations: fan-out and multi-input barriers emerge from `reads:`/`writes:`.

```yaml
# Fan-out: multiple jobs read same register
ingest:
  writes: [raw_data]
  schedule: {min_daily: 24}

process_a:
  reads: [raw_data]      # Waits for ingest
  writes: [output_a]
  schedule: {min_daily: 24}

process_b:
  reads: [raw_data]      # Also waits for ingest, can run parallel with process_a
  writes: [output_b]
  schedule: {min_daily: 24}

# Multi-input barrier: job reads from multiple sources
build_report:
  reads: [output_a, output_b]   # Waits for BOTH
  writes: [daily_report]
  schedule: {min_daily: 24}
```

### Scheduling Logic

```
For each job, check if runnable:

1. SCHEDULE CHECK
   - Time since last run >= min_interval (24h / max_daily)?
   - Executions in last 24h < max_daily?
   → If no: skip (not time yet)

2. INPUT CHECK
   - All registers in `reads` have data?
   - All registers in `reads` are unlocked (no active writer)?
   → If no: skip (inputs not ready)

3. OUTPUT CHECK
   - All registers in `writes` are unlocked?
   → If no: skip (would conflict)

4. If all checks pass: job is READY

Ready jobs dispatched to available executors.
```

### Job Lifecycle

```
┌─────────────────────────────────────────────────────────────────┐
│  Job States                                                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  IDLE ──schedule──► READY ──dispatch──► ASSIGNED                │
│    ▲                                        │                   │
│    │                                        ▼                   │
│    │                                   DOWNLOADING              │
│    │                                        │                   │
│    │                                        ▼                   │
│    │                                    RUNNING                 │
│    │                                        │                   │
│    │                      ┌─────────────────┼─────────────────┐ │
│    │                      ▼                 ▼                 ▼ │
│    └───────────────── COMPLETED          FAILED           TIMEOUT│
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Register Locking (ENQ/DEQ)

LinearJC uses mainframe-style ENQ/DEQ locking for data integrity:

**Lock Types:**
| Mode | Compatibility | Use |
|------|---------------|-----|
| **Shared (S)** | Compatible with other Shared | `reads:` registers |
| **Exclusive (E)** | Incompatible with all | `writes:` registers |

**Compatibility Matrix:**
| Held \ Requested | Shared | Exclusive |
|------------------|--------|-----------|
| **None** | Grant | Grant |
| **Shared** | Grant | Wait |
| **Exclusive** | Wait | Wait |

**Lock Acquisition:**
- **All-or-nothing**: Tree acquires ALL locks or NONE (prevents partial holds)
- **Sorted ordering**: Locks acquired in canonical path order (prevents deadlock)
- **FIFO queue**: Waiting trees served in arrival order (prevents starvation)

**Lock Scope by Type:**
| Register Type | Locking |
|---------------|---------|
| `fs` | ENQ/DEQ (exclusive for writes, shared for reads) |
| `fs` + `protect` | Shared only (exclusive not allowed) |
| `temp` | None (unique MinIO path per tree execution) |
| `minio` | ENQ/DEQ (exclusive for writes, shared for reads) |

**Lock Lifecycle:**
1. **Tree start:** Acquire all locks (reads=shared, writes=exclusive)
2. **During execution:** Hold locks for entire tree duration
3. **Tree completion:** Release all locks (DEQ)
4. **Tree failure/timeout:** Release all locks, cleanup

**Executor Crash Detection:**
- Executors send heartbeat every 30 seconds
- After 3 missed heartbeats (90s): executor presumed dead
- All locks held by dead executor's trees are immediately released
- This prevents hour-long lock holds when executor crashes early

```
Register: /data/reports/daily.csv
├── State: EXCLUSIVE
├── Locked by: report.daily-20260113-143022-a1b2
├── Acquired: 2026-01-13T14:30:22Z
└── Queue: [analyze.data-... (waiting shared)]
```

### Input/Output Flow

**Before execution (coordinator prepares inputs):**

```
For each register in job.reads:
  If type: fs
    → Archive path to tar.gz
    → Upload to MinIO temp bucket
    → Generate presigned GET URL
  If type: minio
    → Generate presigned GET URL directly
  If type: temp (chain job)
    → Generate presigned GET URL from MinIO temp
```

**Before execution (coordinator prepares outputs):**

```
For each register in job.writes:
  → Generate presigned PUT URL (MinIO temp bucket)
```

**Write-Through Collection (after EACH job in chain):**

```
For each register in completed_job.writes:
  If type: fs
    → Download archive from MinIO temp
    → Extract atomically (temp dir + os.replace)
    → Keep MinIO data as cache for chain
  If type: minio
    → Server-side copy to permanent bucket
    → Keep temp data as cache for chain
  If type: temp
    → No action (stays in MinIO for chain)
```

**Cache-Aware Chain Input Preparation:**

```
For next job in chain reading from previous job's output:
  If type: temp
    → Read from MinIO temp (no fallback)
  If type: fs or minio
    → Try MinIO cache (fast path)
    → Fallback to storage if cache miss
```

**Tree Finalization (after last job):**

```
1. Release all ENQ/DEQ locks
2. Delete MinIO temp objects for this tree
3. Clean up work directory
```

This write-through design ensures permanent outputs are safely on disk after each job, while temp outputs provide fast intermediate storage.

### Job Dispatch

Coordinator sends to executor via MQTT:

```json
{
  "job_execution_id": "process-20251125-143022-a1b2c3d4",
  "package_url": "http://minio:9000/packages/process.ljc?signature=...",
  "inputs": {
    "raw_data": {
      "url": "http://minio:9000/temp/raw_data.tar.gz?signature=...",
      "kind": "dir"
    }
  },
  "outputs": {
    "processed": {
      "url": "http://minio:9000/temp/processed-a1b2c3d4.tar.gz?signature=...",
      "kind": "dir"
    }
  }
}
```

**Note:** Executor config (user, timeout, isolation) is inside the .ljc package. Coordinator doesn't need to send it separately.

### MQTT Topics

| Direction | Topic | Purpose |
|-----------|-------|---------|
| Coordinator → Executor | `linearjc/jobs/dispatch/{executor_id}` | Job assignment |
| Executor → Coordinator | `linearjc/jobs/progress/{job_execution_id}` | State updates |
| Developer → Coordinator | `linearjc/deploy/request` | Package deployment |
| Coordinator → Developer | `linearjc/deploy/response/{client_id}` | Deploy result |
| Coordinator → All | `linearjc/coordinator/online` | Coordinator startup (triggers heartbeats) |
| Executor → Coordinator | `linearjc/heartbeat/{executor_id}` | Periodic capability push (30s) |
| ljc → Coordinator | `linearjc/tools/version/request/{coordinator_id}` | Tool version query |
| Coordinator → ljc | `linearjc/tools/version/response/{client_id}` | Version info + download URL |
| Coordinator → Executor | `linearjc/executors/{executor_id}/update` | Executor auto-update command |

All messages signed with HMAC-SHA256 (shared secret).

### Package Storage

Coordinator stores .ljc packages received via `ljc deploy`:

```
/var/lib/linearjc/
├── packages/
│   ├── ingest.ljc
│   ├── process.ljc
│   └── backup.ljc
├── registry.yaml           # Pushed from ljc
└── state/
    └── schedules.yaml      # Last run times, execution counts
```

**On package install:**
1. Receive .ljc via MQTT (presigned upload to MinIO)
2. Download and validate
3. Extract job.yaml, parse for: id, version, reads, writes, schedule
4. Store .ljc in packages/
5. Update internal job index
6. SIGHUP: reload without restart (with safety validation)

### Hot Reload (SIGHUP)

The coordinator supports hot reload via SIGHUP signal. Registry and job changes are validated before applying.

**Safety Validation (Phase 15):**

| Change Type | Active Locks | Behavior |
|-------------|--------------|----------|
| Add new register | Any | ✅ Allowed |
| Remove register | Has lock | ❌ Blocked |
| Change path | Has lock | ⚠️ Warning (orphaned lock) |
| Change type | Has lock | ❌ Blocked |
| Add `protect: true` | Has exclusive | ❌ Blocked (writer mid-write) |
| Add `protect: true` | Has shared | ✅ Allowed |
| Remove `protect: true` | Has shared | ❌ Blocked (readers expect immutable) |
| Remove `protect: true` | Has exclusive | ✅ Allowed |

**Process:**
1. Load new registry to temporary variable
2. Validate changes against current lock state
3. If blocked: abort reload, log errors
4. If warnings: log and proceed
5. Apply registry, rebuild trees, preserve scheduling state

**Coordinator restart** (clean slate): All locks cleared, MinIO temp cleaned, executors reconnect.

### Coordinator Configuration

```yaml
coordinator:
  id: linearjc-coordinator

  mqtt:
    broker: 192.0.2.10
    port: 1883
    keepalive: 60

  minio:
    endpoint: 192.0.2.10:9000
    access_key: ${MINIO_ACCESS_KEY}
    secret_key: ${MINIO_SECRET_KEY}
    bucket: linearjc
    temp_prefix: temp/
    packages_prefix: packages/

  storage:
    packages_dir: /var/lib/linearjc/packages
    registry_file: /var/lib/linearjc/registry.yaml
    state_file: /var/lib/linearjc/state/schedules.yaml

  scheduling:
    loop_interval: 10        # Seconds between scheduler runs
    lock_timeout: 3600       # Auto-release locks after 1h

  signing:
    shared_secret: ${LINEARJC_SECRET}

  security:
    allowed_data_roots:      # fs registers must be under these paths
      - /var/share
      - /data
```

### CLI Commands

```bash
# Main operation
linearjc-coordinator run                    # Start scheduler loop

# Management
linearjc-coordinator status                 # Show jobs, schedules, locks
linearjc-coordinator install <package.ljc>  # Manual package install

# Maintenance
linearjc-coordinator cleanup --age 24h      # Remove old temp files
```

### Key Responsibilities Summary

| Responsibility | Description |
|----------------|-------------|
| Package storage | Store .ljc files, extract job.yaml metadata |
| Registry management | Store registry.yaml, validate references |
| Schedule enforcement | min/max_daily, track last execution times |
| Data availability | Track which registers have data |
| Register locking | Prevent concurrent writes |
| Input preparation | Archive fs → MinIO, generate presigned URLs |
| Output collection | MinIO → extract to fs, validate kind |
| Executor dispatch | Send job assignments via MQTT |
| Progress tracking | Handle state updates, detect timeouts |

### What Coordinator Does NOT Do

| Not responsible | Why |
|-----------------|-----|
| Job validation | Done by `ljc validate` before deploy |
| Isolation enforcement | Done by executor using `linearjc-core` |
| Script execution | Done by executor |
| Complex scheduling logic | Register-driven, emergent from reads/writes |

---

## Part 3: Executor

*Placeholder - to be specified after ljc review*

### Overview

The executor is a **thin wrapper** around `linearjc-core`. It runs on plain Linux hosts (not containerized) and handles I/O while delegating execution to shared code.

**Design principle:** The executor should be minimal and auditable. All security-critical code (isolation, execution) lives in `linearjc-core`, shared with `ljc test`.

```
Executor = linearjc-core + MQTT + I/O
```

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Executor Binary                                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────┐    ┌─────────────────────────────────────┐ │
│  │  MQTT Client    │    │  linearjc-core (shared crate)       │ │
│  │  • Subscribe    │    │  • Workdir setup                    │ │
│  │  • Publish      │    │  • Isolation (Landlock, cgroups)    │ │
│  │  • Progress     │    │  • Execution (fork, setuid, exec)   │ │
│  └─────────────────┘    │  • Archive handling                 │ │
│                         └─────────────────────────────────────┘ │
│  ┌─────────────────┐                                            │
│  │  I/O Layer      │    Same code as `ljc test`                │
│  │  • Download in  │                                            │
│  │  • Upload out   │                                            │
│  │  • Package cache│                                            │
│  └─────────────────┘                                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Key Responsibilities

**Executor-specific (thin layer):**
- MQTT communication with coordinator
- Package download and caching
- Input preparation (download from registry to `in/`)
- Output archiving (upload from `out/` to registry)
- Progress reporting

**Delegated to linearjc-core (shared):**
- Workdir creation and structure
- Process isolation (Landlock, cgroups, network namespaces)
- Script execution (fork, setuid, exec)
- Timeout enforcement

### Isolation Implementation

Isolation is implemented in `linearjc-core` and shared with `ljc`:

- **Same code path** in development (`ljc test`) and production (executor)
- **Same Landlock rules** - no behavior differences
- **Auditable** - security code in one place

### Output Capture

Job output (stdout/stderr) is captured by the executor and included in progress messages.

**Design principle:** Output is part of the execution result, not a separate logging concern.

#### Capture Modes

| Mode | When | Behavior |
|------|------|----------|
| **Buffered** | No active followers | Capture to buffer, send tail on completion |
| **Streaming** | `ljc tail` / `ljc exec --follow` active | Send chunks periodically during execution |

The coordinator notifies the executor when a developer client attaches. The executor only streams chunks when someone is listening - otherwise it just buffers and sends the tail at completion.

#### Progress Messages

**During execution (streaming mode):**

```json
{
  "execution_id": "build.musl-toolchain-20251204-191836",
  "state": "running",
  "elapsed_ms": 125000,
  "output": {
    "chunk": "  CC ldmain.o\n  CC ldwrite.o\n  CC ldexp.o\n",
    "total_lines": 2341
  }
}
```

**On completion (any terminal state):**

```json
{
  "execution_id": "build.musl-toolchain-20251204-191836",
  "state": "failed",
  "exit_code": 1,
  "duration_ms": 212952,
  "output": {
    "tail": "...\n/bin/sh: rsync: not found\nmake: *** [Makefile:1361: headers_install] Error 127\nmake: *** [Makefile:182: all] Error 2\n",
    "tail_lines": 100,
    "total_lines": 5432
  }
}
```

#### Chunking Strategy

Chunks are sent when any trigger fires:

| Trigger | Default | Description |
|---------|---------|-------------|
| Time | 3 seconds | Maximum time between chunks |
| Lines | 50 lines | Maximum lines per chunk |
| Size | 32 KB | Maximum chunk size |

#### Output Fields

| Field | Present | Description |
|-------|---------|-------------|
| `output.chunk` | During execution | Lines since last progress message |
| `output.tail` | On completion | Last N lines of total output |
| `output.tail_lines` | On completion | Number of lines in tail (default: 100) |
| `output.total_lines` | Always | Total lines produced so far |

#### Coordinator Behavior

1. **Forward chunks**: When developer client is attached, forward progress messages with output chunks
2. **Store tail**: On completion, store output tail in execution history
3. **Prune history**: Keep last N executions per job (default: 10)

#### Developer Commands

| Command | Source | Description |
|---------|--------|-------------|
| `ljc exec <job> --follow` | Streaming chunks | Trigger execution, stream output until completion |
| `ljc tail <job>` | Streaming chunks | Attach to running execution, stream output |
| `ljc logs <job>` | Stored tails | Show execution history with output tails |

### Configuration

```yaml
executor:
  id: executor-host-executor-01

  mqtt:
    broker: 192.0.2.10
    port: 1883

  minio:
    endpoint: 192.0.2.10:9000
    access_key: ${MINIO_ACCESS_KEY}
    secret_key: ${MINIO_SECRET_KEY}

  storage:
    work_dir: /var/lib/linearjc/work
    packages_dir: /var/lib/linearjc/packages

  output:
    tail_lines: 100           # Lines to include in completion message
    chunk_interval_ms: 3000   # Max time between streaming chunks
    chunk_max_lines: 50       # Max lines per chunk
    chunk_max_bytes: 32768    # Max bytes per chunk

  signing:
    shared_secret: ${LINEARJC_SECRET}
```

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| Register | Named data location in the registry |
| Registry | Collection of all register definitions |
| Job | Unit of work with defined inputs, outputs, and schedule |
| Job Tree | Current execution tree built from explicit chain dependencies |
| Batch Generation | One scheduled or manual execution of a batch group |
| Barrier Job | Future multi-input job that waits for all input registers in the same batch generation |
| Execution | Single run of a job |
| Package | Self-contained .ljc archive for deployment |
| Workdir | Temporary directory where job executes (contains in/, out/, tmp/) |
| Coordinator | Central scheduler (dumb, receives validated artifacts) |
| Executor | Worker that runs jobs on Linux hosts (thin wrapper around linearjc-core) |
| linearjc-core | Shared Rust crate containing security-critical execution code |
| ljc | Developer CLI tool for job development, testing, and deployment |

## Appendix B: Migration Guide

### From v0.5.x to v0.8.x (Phase 15 Register Model)

**Registry Changes:**

1. **`kind` is now required for all types** (was only fs)
   ```yaml
   # Before (v0.5.x)
   my_minio: {type: minio, bucket: data, prefix: foo/}

   # After (v0.8.x)
   my_minio: {type: minio, bucket: data, prefix: foo/, kind: dir}
   ```

2. **New `temp` type for intermediates**
   ```yaml
   # Before: Used fs for intermediates (unnecessary disk I/O)
   intermediate: {type: fs, path: /data/temp/intermediate.json, kind: file}

   # After: Use temp (MinIO only, auto-cleaned)
   intermediate: {type: temp, kind: file}
   ```

3. **New `protect` flag for external inputs**
   ```yaml
   # Before: No protection, jobs could accidentally overwrite
   source_db: {type: fs, path: /data/source.db, kind: file}

   # After: Protected, jobs cannot write
   source_db: {type: fs, path: /data/source.db, kind: file, protect: true}
   ```

**Behavioral Changes:**

| Aspect | v0.5.x | v0.8.x |
|--------|--------|--------|
| Output collection | After leaf job only | After EACH job (write-through) |
| Locking | Simple exclusive | ENQ/DEQ (shared+exclusive) |
| Executor crash | Lock held until timeout | Released after 90s (heartbeat) |
| Hot reload | No validation | Safety checks block dangerous changes |

**Migration Steps:**

1. Add `kind` to all minio registry entries
2. Consider converting intermediates to `type: temp`
3. Add `protect: true` to external input registers
4. Test with `ljc validate` before deploying

**Backward Compatibility:**

- Jobs with `type: fs` continue to work unchanged
- Registry entries without `protect` default to `protect: false`
- Existing chains work; they just benefit from write-through caching

---

## Appendix C: Crate Structure

```
submodules/linearjc/
├── crates/
│   └── linearjc-core/              # Shared, auditable, security-critical
│       ├── Cargo.toml
│       └── src/
│           ├── lib.rs
│           ├── workdir.rs          # Workdir creation and setup
│           ├── isolation.rs        # Landlock, cgroups, network namespaces
│           ├── execution.rs        # Fork, setuid, exec, timeout
│           └── archive.rs          # Tar/gzip handling
├── src/
│   ├── coordinator/                # Python - scheduling, MQTT dispatch
│   └── executor/                   # Rust - thin wrapper around linearjc-core
│       ├── Cargo.toml              # Depends on linearjc-core
│       └── src/
│           ├── main.rs
│           ├── mqtt.rs             # MQTT client
│           └── io.rs               # Registry I/O (download/upload)
└── tools/
    └── ljc/                        # Rust - developer tool
        ├── Cargo.toml              # Depends on linearjc-core
        └── src/
            ├── main.rs
            ├── commands/
            │   ├── test.rs         # Uses linearjc-core for execution
            │   ├── build.rs
            │   ├── validate.rs
            │   └── ...
            └── ...
```

---

## Appendix D: Future Considerations

### Single-Machine Mode

`ljc run <job-id>` - Execute job directly on local machine without coordinator/executor.

**Use case:** Simpler deployments where MQTT/MinIO infrastructure is overkill.

**Constraints:**
- Only `type: fs` registers supported (no MinIO)
- No scheduling (user triggers via cron/systemd/manual)
- Dependencies checked (upstream outputs must exist) but not enforced

**Implementation:**
- Uses `linearjc-core` (same isolation as distributed mode)
- Reads inputs from actual registry paths → workdir `in/`
- Writes outputs from workdir `out/` → actual registry paths

**Compatibility:** Additive change, does not break distributed mode.

**Evolution path:**
```
Phase 1 (current):  ljc test → build → deploy → coordinator → executor
Phase 2 (future):   ljc run (single-machine, fs-only, immediate)
Phase 3 (optional): ljc daemon (local scheduler)
```

### Coordinator Interaction (Implemented)

Developer commands for interacting with the coordinator:

| Command | Status | Description |
|---------|--------|-------------|
| `ljc exec <job-id> [--follow]` | ✅ Implemented | Trigger immediate execution, optionally stream output |
| `ljc status [job-id] [--all]` | ✅ Implemented | Query job scheduling status |
| `ljc tail <job-id\|exec-id>` | ✅ Implemented | Attach to running execution, stream output |
| `ljc logs <job-id> [--last N]` | 🚧 In Progress | Show execution history with output tails |
| `ljc ps [--all]` | ✅ Implemented | List active executions |
| `ljc kill <exec-id> [--force]` | ✅ Implemented | Cancel running execution |
| `ljc self-update [--check]` | ✅ Implemented | Update ljc to latest version from coordinator |

See **Part 3: Executor → Output Capture** for the output streaming specification.

### Tool Self-Update

ljc can update itself to the latest version from the coordinator:

```bash
# Check for updates without installing
ljc self-update --check

# Download and install update
ljc self-update
```

**Coordinator configuration:**
```yaml
coordinator:
  tools_registry: /var/lib/linearjc/tools-registry.yaml
```

**Tools registry format:**
```yaml
tools:
  ljc:
    x86_64-unknown-linux-musl:
      version: "0.2.0"
      checksum_sha256: "a1b2c3d4..."
      size_bytes: 4521984
      path: "tools/ljc-0.2.0-x86_64-linux-musl"
```

Artifacts are stored in the MinIO temp bucket and served via presigned URLs.

### Mainframe-Inspired Features

Features from JCL/JES that would add operational resilience:

#### Generation Data Groups (GDG)

Automatic versioning of registers with rolling retention:

```yaml
registry:
  daily_report:
    type: fs
    path: /var/share/reports/daily
    kind: dir
    generations: 7          # Keep last 7 versions
```

```yaml
job:
  reads: [daily_report(0)]    # Current generation
  writes: [daily_report(+1)]  # Create next generation
  # reads: [daily_report(-1)] # Previous generation (for comparison)
```

**Value:** Audit trail, rollback capability, automatic retention management.

#### Conditional Execution

Run jobs based on previous job's exit code:

```yaml
job:
  id: report.daily
  reads: [processed_data]
  writes: [report]
  condition:
    when: processed_data.exit_code <= 4    # Run even on warnings (1-4)
    skip_on: processed_data.exit_code > 4  # Skip on errors
```

**Value:** Graceful handling of partial success, skip expensive steps on warnings.

#### Dataset Disposition (DISP)

Declarative cleanup behavior on success/failure:

```yaml
job:
  writes:
    temp_work:
      register: work_area
      on_success: delete      # Cleanup temp data
      on_failure: keep        # Preserve for debugging
    final_output:
      register: report
      on_success: catalog     # Make available
      on_failure: delete      # Don't leave partial data
```

**Value:** Atomic success/failure handling, automatic cleanup.

#### Checkpoint/Restart

For long-running jobs, save progress and allow restart:

```yaml
job:
  id: big.migration
  run:
    timeout: 14400            # 4 hours
    checkpoint:
      enabled: true
      interval: 300           # Every 5 minutes
      register: migration_checkpoint
```

On failure: `ljc restart big.migration --from-checkpoint`

**Value:** Don't lose hours of work on late-stage failure.

#### Procedure Templates (PROC)

Reusable job templates with parameters:

```yaml
# templates/etl.yaml
template:
  id: standard-etl
  parameters:
    source: {required: true}
    dest: {required: true}
    format: {default: csv}
  job:
    reads: [${source}]
    writes: [${dest}]
    run:
      entry: etl.sh
      env:
        FORMAT: ${format}
```

```bash
ljc new daily.sales --template standard-etl --source=raw_sales --dest=processed_sales
```

**Value:** Standardization, DRY principle, organizational patterns.

### Priority Assessment

| Feature | Effort | Value | Priority |
|---------|--------|-------|----------|
| Generation Data Groups | Medium | High | P1 |
| Conditional execution | Low | Medium | P2 |
| DISP semantics | Low | Medium | P2 |
| Checkpoint/restart | High | High | P3 (for long jobs only) |
| PROC templates | Medium | Medium | P3 |

### Other

- Additional register types (database, API endpoints)
- `ljc watch` - Real-time TUI dashboard of job activity
- Coordinator REST/gRPC interface (hide MQTT/MinIO complexity)
