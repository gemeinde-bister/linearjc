# LinearJC Specification

**Version**: 0.9.0-draft
**Last Updated**: 2026-05-10

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
+-----------------------------------------------------------------+
|  ljc (Developer Tool)                                           |
|  - Single source of truth for jobs and registry                 |
|  - Full validation before deployment                            |
|  - Builds self-contained .ljc packages                          |
|  - Local testing with ljc test (uses linearjc-core)             |
+-----------------------------------------------------------------+
                              |
                              | push registry + deploy packages
                              v
+-----------------------------------------------------------------+
|  Coordinator                                                    |
|  - Receives validated artifacts                                 |
|  - Builds execution trees from register ownership and chains    |
|  - Schedules jobs, manages register locks                       |
|  - Dispatches work to executors                                 |
+-----------------------------------------------------------------+
                              |
                              | MQTT job requests
                              v
+-----------------------------------------------------------------+
|  Executor(s)                                                    |
|  - Runs on plain Linux (not containerized)                      |
|  - Thin wrapper around linearjc-core                            |
|  - Executes jobs with optional isolation (Landlock, cgroups)    |
|  - Reports progress and results via MQTT                        |
+-----------------------------------------------------------------+
```

### Shared Core Architecture

The security-critical execution code is shared between `ljc` and the executor:

```
+-----------------------------------------------------------------+
|  linearjc-core (shared Rust crate)                              |
|  - workdir.rs    - Workdir creation and setup                   |
|  - isolation.rs  - Landlock, cgroups, network namespaces        |
|  - execution.rs  - Fork, setuid, exec                           |
|  - archive.rs    - Tar/gzip handling                            |
|                                                                 |
|  Small, auditable, security-critical code                       |
+-----------------------------------------------------------------+
          |                                     |
          v                                     v
+---------------------+   +-------------------------------------+
|  Executor           |   |  ljc                                |
|  - MQTT client      |   |  - Repository management            |
|  - Input download   |   |  - Validation                       |
|  - Output upload    |   |  - Package building                 |
|  - Progress report  |   |  - MQTT deployment                  |
|                     |   |  - ljc test command                 |
|  Thin, minimal      |   |                                     |
+---------------------+   +-------------------------------------+
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

Current releases implement this through linear chains and fan-out trees with explicit `depends:` links. The planned evolution is register-driven barrier execution: the coordinator infers dependencies from register ownership, removing the need for explicit `depends:` as the primary mechanism. See ROADMAP.md for motivation and priorities.

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
[fs protected input] -> job1 -> [temp intermediate] -> job2 -> [fs output]
```

#### Register Types

| Type | Storage | Persistence | Locking | Use Case |
|------|---------|-------------|---------|----------|
| `fs` | Filesystem | Permanent | ENQ/DEQ | Managed outputs, external inputs |
| `fs` + `protect` | Filesystem | Permanent | Shared only | External read-only inputs |
| `temp` | MinIO temp | Tree duration | None (unique path) | Intermediates between jobs |
| `minio` | MinIO bucket | Permanent | ENQ/DEQ | Large objects, cross-system |

**`type: fs`** - Filesystem register:
- `path`: Absolute filesystem path (required)
- `kind`: `file` (single file) or `dir` (directory)
- `protect`: If `true`, register is read-only (external input, must exist at startup)
- Write-through semantics: job output -> MinIO temp -> collected to filesystem path

**`type: temp`** - Temporary register:
- No filesystem path - lives only in MinIO during tree execution
- Automatically cleaned when tree completes or fails
- Enables parallel fan-out (each tree gets unique MinIO path)
- MinIO path convention: `jobs/{tree_exec_id}/output_{registry_key}.tar.gz`

**`type: minio`** - Permanent MinIO register:
- `bucket`: MinIO bucket name (required, S3 naming rules)
- `prefix`: Object prefix within bucket (optional)
- `kind`: `file` or `dir`
- Write-through: server-side copy from temp to permanent bucket

**`kind` field (all types):**
- `file`: Single file (archive must contain exactly one file, validated at extraction)
- `dir`: Directory with contents (preserves structure)

### Batch Generations

A batch generation is one scheduled or manual execution of a batch group. Temp registers are scoped to that generation so downstream modules never mix outputs from different runs.

Example:
```
generation daily.close-20260113-000001
  sales_extract       from extract.sales
  inventory_extract   from extract.inventory
  daily_report        from build.report
```

Generation scoping is what makes multi-input consolidation safe: a barrier job must consume inputs from the same generation, not whichever register was most recently written.

**Note**: Generation tracking is not yet implemented at runtime. The current implementation uses tree execution IDs for temp register scoping within a single chain, but does not enforce cross-chain generation consistency. See ROADMAP.md priority 2.

---

## Part 1: ljc (Developer Tool)

### Repository Structure

```
my-jobs/
  registry.yaml        # All register definitions
  jobs/
    <job-id>/
      job.yaml          # Job definition
      script.sh         # Entry script
      bin/               # Optional bundled binaries
      data/              # Optional bundled static data
      test/              # Optional test inputs (excluded from package)
  dist/                  # Built .ljc packages
  .ljcconfig             # Tool configuration
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
  writes: [sensor_parsed]                # Output register (this job owns it)

  depends: []                            # Chain predecessor (current execution model)

  schedule:
    min_daily: 1                         # Minimum executions per 24h
    max_daily: 5                         # Maximum executions per 24h

  run:
    user: nobody                         # Execution user
    timeout: 300                         # Seconds
    entry: script.sh                     # Entry script (default: script.sh)
    binaries: [bin/processor]            # Optional bundled binaries

    # Process isolation (all optional, shown with defaults)
    isolation: none                      # strict | relaxed | none
    network: true                        # Allow network access
    # extra_read_paths: []               # For relaxed mode only
    # limits:                            # Resource limits (cgroups v2)
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
| `depends` | No | Chain predecessor (max 1 for current linear chains). Planned to become a validation assertion when register-driven scheduling replaces explicit dependencies. |
| `schedule.min_daily` | Yes | Minimum runs per 24h sliding window (must be >= 1) |
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

### Current Execution Model: Chains and Fan-Out

The current implementation builds execution trees from explicit `depends:` links. This supports linear chains and fan-out.

**Linear chains** where one job depends on another:

```yaml
# Linear chain: A -> B -> C
- id: job.a
  depends: []
  writes: [output_a]

- id: job.b
  depends: [job.a]
  reads: [output_a]
  writes: [output_b]

- id: job.c
  depends: [job.b]
  reads: [output_b]
```

**Fan-out** where multiple jobs depend on the same job creates separate trees:

```yaml
# Fan-out: A -> B and A -> C (two separate trees)
- id: process.data
  depends: []
  writes: [processed]

- id: analyze.spending
  depends: [process.data]  # Tree 1: [process.data, analyze.spending]

- id: export.report
  depends: [process.data]  # Tree 2: [process.data, export.report]
```

In fan-out, `process.data` **runs once per consuming tree**. This is a known limitation for expensive root jobs - see ROADMAP.md priority 1.

**Constraints:**
- Each job can depend on **at most one** other job (no merge points)
- Cycles are not allowed
- Tree schedules use the most restrictive min/max from all jobs in the chain

### Planned: Register-Driven Barrier Execution

The planned execution model replaces explicit `depends:` with dependency inference from register ownership. Multi-input barriers emerge naturally:

```yaml
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

`build.report` is a barrier job. The coordinator infers that it must wait for `extract.sales` and `extract.inventory` because they own the registers that `build.report` reads. Each job runs once per batch generation. No fan-out duplication.

This is not implemented. See ROADMAP.md for the architectural transition plan.

### Workdir Structure

When a job executes, the executor creates a **workdir** with a fixed, predictable structure:

```
{workdir}/
  script.sh            # Entry script (from package)
  bin/                  # Bundled binaries (from package)
    processor
  data/                 # Bundled static data (from package)
    template.json
  in/                   # Inputs (populated by executor, read-only)
    sensor_raw/          # Directory input (kind: dir)
    sensor_config        # File input (kind: file)
  out/                  # Outputs (job writes here)
  tmp/                  # Scratch space (job can use freely)
```

**Key design points:**
- Script always executes from workdir root
- All paths are **relative** - scripts use `in/`, `out/`, `tmp/`, `bin/`, `data/`
- Register names become paths: `in/{register_name}`, `out/{register_name}`
- With Landlock isolation, script cannot access anything outside workdir (except system libs)

**Environment Variables (identification only):**

| Variable | Description |
|----------|-------------|
| `LINEARJC_JOB_ID` | Job identifier (e.g., `process.daily`) |
| `LINEARJC_EXECUTION_ID` | Unique execution ID |

**Note:** No path environment variables. Scripts use relative paths.

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

# Validate output exists
[ -d out/sensor_parsed ] || exit 1
exit 0
```

### Process Isolation

Jobs can opt into kernel-level isolation for security:

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

### Package Format (.ljc)

The `.ljc` package is a gzip-compressed tar archive containing:

```
package.ljc (tar.gz)
  job.yaml             # Job definition (for coordinator)
  script.sh            # Entry script (executable)
  bin/                  # Optional bundled binaries
  data/                 # Optional bundled static data
```

The executor extracts the package to the workdir and adds I/O directories (`in/`, `out/`, `tmp/`). The `test/` directory is excluded from packages.

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
ljc registry push                  # Push registry to coordinator
```

#### Job Development

```bash
ljc new <job-id>                   # Create new job
ljc validate [job-id | --all]      # Validate job(s)
ljc build <job-id>                 # Build .ljc package
ljc bump <major|minor|patch> \     # Bump version
    <job-id>
```

#### Testing

```bash
ljc test <job-id>                  # Run job locally with test/ inputs
ljc test <job-id> --isolation none # Override isolation
ljc test <job-id> --verbose        # Show script stdout/stderr
ljc extract <package.ljc>          # Extract package to workdir
```

`ljc test` runs a job locally using the same `linearjc-core` code as the production executor. Override flags: `--isolation`, `--network`, `--timeout`, `--user`, `--cpu`, `--memory`, `--keep` (preserve workdir), `--validate` (diff against `test/expected/`).

#### Deployment and Operations

```bash
ljc deploy <package.ljc> --to <host>  # Deploy to coordinator
ljc exec <job-id> [--follow]          # Trigger immediate execution
ljc status [job-id]                   # Query scheduling status
ljc ps                                # List active executions
ljc tail <job-id>                     # Follow running execution
ljc logs <job-id>                     # Execution history
ljc kill <exec-id>                    # Cancel execution
ljc self-update [--check]             # Update ljc from coordinator
```

### Validation Rules

`ljc validate` checks before deployment:

| Rule | Description |
|------|-------------|
| All `reads` registers exist in registry | Missing input |
| All `writes` registers exist in registry | Missing output |
| No `writes` to protected registers | Cannot overwrite external inputs |
| No duplicate writes across jobs | Single writer rule |
| Script has valid shebang | Executor uses shebang directly |
| Version is valid semver | Required for deployment |
| `min_daily >= 1` | Use `ljc exec` for on-demand |

### Configuration (.ljcconfig)

```yaml
coordinator:
  host: 192.0.2.10
  mqtt_port: 1883
signing:
  secret_env: LINEARJC_SECRET
```

---

## Part 2: Coordinator

### Overview

The coordinator is the central scheduler. It receives job packages from `ljc`, manages the registry, and dispatches work to executors.

**Design principle:** The coordinator is "dumb" - it stores validated artifacts, enforces schedules, and moves data. Complex logic lives in `ljc` (validation) and `linearjc-core` (execution).

### Current Scheduling Model

The current coordinator builds execution trees from explicit `depends:` links, then schedules trees as units:

1. Discover jobs from `jobs_dir`, build dependency trees
2. For each tree, check schedule constraints (min/max daily, sliding window)
3. Acquire register locks (all-or-nothing, sorted order)
4. Find capable executor via heartbeat registry
5. Dispatch root job to executor
6. On completion, dispatch next job in chain (chain continuation)
7. On tree completion, collect outputs (write-through) and release locks

### Planned: Register-Driven Scheduling

The target model infers dependencies from register ownership:

```
For each job, check if runnable:

1. SCHEDULE CHECK
   - Time since last run >= min_interval (24h / max_daily)?
   - Executions in last 24h < max_daily?

2. INPUT CHECK
   - All registers in reads have data?
   - All registers in reads are unlocked (no active writer)?

3. OUTPUT CHECK
   - All registers in writes are unlocked?

4. If all checks pass: job is READY
```

This removes the need for explicit `depends:` as the primary mechanism. Fan-out and multi-input barriers emerge from register ownership. Each job runs once per batch generation. Not yet implemented - see ROADMAP.md.

### Register Locking (ENQ/DEQ)

LinearJC uses mainframe-style ENQ/DEQ locking for data integrity.

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

**Hazard Detection:**

| Hazard | Scenario | Resolution |
|--------|----------|------------|
| RAW (Read-After-Write) | Tree B reads register X while Tree A writes X | B waits for A |
| WAW (Write-After-Write) | Tree B writes register X while Tree A writes X | B waits for A |
| WAR (Write-After-Read) | Tree B writes register X while Tree A reads X | B waits for A |

**Lock Acquisition:**
- **All-or-nothing**: Tree acquires ALL locks or NONE (prevents partial holds)
- **Sorted ordering**: Locks acquired in canonical path order (prevents deadlock)
- **FIFO queue**: Waiting trees served in arrival order (prevents starvation)
- **Shared batching**: Consecutive shared waiters in queue are granted together

**Lock Scope by Type:**

| Register Type | Locking |
|---------------|---------|
| `fs` | ENQ/DEQ (exclusive for writes, shared for reads) |
| `fs` + `protect` | Shared only (exclusive not allowed) |
| `temp` | None (unique MinIO path per tree execution) |
| `minio` | ENQ/DEQ (exclusive for writes, shared for reads) |

**Lock Path Resolution:**
- `fs`: resolved absolute filesystem path
- `minio`: `minio:{bucket}/{prefix}`
- `temp`: no lock path (returns None)

**Lock Lifecycle:**
1. **Tree start:** Acquire all locks (reads=shared, writes=exclusive)
2. **During execution:** Hold locks for entire tree duration
3. **Tree completion:** Release all locks (DEQ), process wait queue
4. **Tree failure/timeout:** Release all locks, cleanup

**Executor Crash Detection:**
- Executors send heartbeat every 30 seconds
- After 3 missed heartbeats (90s): executor presumed dead
- All locks held by dead executor's trees are immediately released

**Coordinator Restart:**
- Lock state is purely in-memory
- Restart clears all locks and MinIO temp data (clean slate)
- Running jobs on executors become orphaned; executors detect absence and stop
- Simpler than lock reconstruction, acceptable for rare restart events

### Input/Output Flow

**Before execution (coordinator prepares inputs):**

```
For each register in job.reads:
  If type: fs    -> Archive path to tar.gz, upload to MinIO temp, generate presigned GET URL
  If type: minio -> Generate presigned GET URL directly
  If type: temp  -> Generate presigned GET URL from MinIO temp
```

**Before execution (coordinator prepares outputs):**

```
For each register in job.writes:
  -> Generate presigned PUT URL (MinIO temp bucket)
```

**Write-Through Collection (after EACH job in chain):**

```
For each register in completed_job.writes:
  If type: fs    -> Download archive from MinIO temp, extract atomically to path
  If type: minio -> Server-side copy to permanent bucket
  If type: temp  -> No action (stays in MinIO for chain)
```

**Cache-Aware Chain Input Preparation:**

```
For next job in chain reading from previous job's output:
  If type: temp       -> Read from MinIO temp (no fallback)
  If type: fs or minio -> Try MinIO cache first, fallback to storage
```

**Tree Finalization:**

```
1. Release all ENQ/DEQ locks
2. Delete MinIO temp objects for this tree
3. Clean up work directory
```

### Job Lifecycle

```
IDLE --schedule--> READY --dispatch--> ASSIGNED
  ^                                       |
  |                                       v
  |                                  DOWNLOADING
  |                                       |
  |                                       v
  |                                   RUNNING
  |                                       |
  |                     +-----------------+-----------------+
  |                     v                 v                 v
  +---------------- COMPLETED          FAILED           TIMEOUT
```

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

Executor config (user, timeout, isolation) is inside the .ljc package. Coordinator does not send it separately.

### MQTT Topics

| Direction | Topic | Purpose |
|-----------|-------|---------|
| Coordinator -> Executor | `linearjc/jobs/dispatch/{executor_id}` | Job assignment |
| Executor -> Coordinator | `linearjc/jobs/progress/{job_execution_id}` | State updates |
| Developer -> Coordinator | `linearjc/deploy/request` | Package deployment |
| Coordinator -> Developer | `linearjc/deploy/response/{client_id}` | Deploy result |
| Coordinator -> All | `linearjc/coordinator/online` | Coordinator startup |
| Executor -> Coordinator | `linearjc/heartbeat/{executor_id}` | Periodic heartbeat (30s) |
| ljc -> Coordinator | `linearjc/tools/version/request/{coordinator_id}` | Tool version query |
| Coordinator -> ljc | `linearjc/tools/version/response/{client_id}` | Version info |
| Coordinator -> Executor | `linearjc/executors/{executor_id}/update` | Auto-update command |

All messages signed with HMAC-SHA256 (shared secret).

### Package Storage

```
/var/lib/linearjc/
  packages/
    ingest.ljc
    process.ljc
  registry.yaml           # Pushed from ljc
  state/
    schedules.yaml         # Last run times, execution counts
```

### Hot Reload (SIGHUP)

The coordinator supports hot reload. Registry and job changes are validated before applying:

| Change Type | Active Locks | Behavior |
|-------------|--------------|----------|
| Add new register | Any | Allowed |
| Remove register | Has lock | Blocked |
| Change path | Has lock | Warning (orphaned lock) |
| Change type | Has lock | Blocked |
| Add `protect: true` | Has exclusive | Blocked |
| Add `protect: true` | Has shared | Allowed |
| Remove `protect: true` | Has shared | Blocked |
| Remove `protect: true` | Has exclusive | Allowed |

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
    loop_interval: 10
    lock_timeout: 3600

  signing:
    shared_secret: ${LINEARJC_SECRET}

  security:
    allowed_data_roots:
      - /var/share
      - /data

  tools_registry: /var/lib/linearjc/tools-registry.yaml
```

### Key Responsibilities

| Responsibility | Description |
|----------------|-------------|
| Package storage | Store .ljc files, extract job.yaml metadata |
| Registry management | Store registry.yaml, validate references |
| Schedule enforcement | min/max_daily, track last execution times |
| Register locking | ENQ/DEQ with all-or-nothing acquisition |
| Input preparation | Archive fs -> MinIO, generate presigned URLs |
| Output collection | MinIO -> extract to fs (write-through) |
| Executor dispatch | Send job assignments via MQTT |
| Progress tracking | Handle state updates, detect timeouts |

### What Coordinator Does NOT Do

| Not responsible | Why |
|-----------------|-----|
| Job validation | Done by `ljc validate` before deploy |
| Isolation enforcement | Done by executor using `linearjc-core` |
| Script execution | Done by executor |

---

## Part 3: Executor

### Overview

The executor is a **thin wrapper** around `linearjc-core`. It runs on plain Linux hosts (not containerized) and handles I/O while delegating execution to shared code.

**Design principle:** The executor should be minimal and auditable. All security-critical code (isolation, execution) lives in `linearjc-core`, shared with `ljc test`.

```
Executor = linearjc-core + MQTT + I/O
```

### Architecture

```
+-----------------------------------------------------------------+
|  Executor Binary                                                |
+-----------------------------------------------------------------+
|                                                                 |
|  +------------------+    +------------------------------------+ |
|  |  MQTT Client     |    |  linearjc-core (shared crate)      | |
|  |  - Subscribe     |    |  - Workdir setup                   | |
|  |  - Publish       |    |  - Isolation (Landlock, cgroups)    | |
|  |  - Progress      |    |  - Execution (fork, setuid, exec)  | |
|  +------------------+    |  - Archive handling                 | |
|                          +------------------------------------+ |
|  +------------------+                                           |
|  |  I/O Layer       |    Same code as ljc test                  |
|  |  - Download in   |                                           |
|  |  - Upload out    |                                           |
|  |  - Package cache |                                           |
|  +------------------+                                           |
+-----------------------------------------------------------------+
```

### Key Responsibilities

**Executor-specific (thin layer):**
- MQTT communication with coordinator
- Package download and caching
- Input preparation (download from registry to `in/`)
- Output archiving (upload from `out/` to registry)
- Progress reporting
- Heartbeat publishing (every 30s with version and capabilities)

**Delegated to linearjc-core (shared):**
- Workdir creation and structure
- Process isolation (Landlock, cgroups, network namespaces)
- Script execution (fork, setuid, exec)
- Timeout enforcement

### Output Capture

Job output (stdout/stderr) is captured by the executor and included in progress messages.

| Mode | When | Behavior |
|------|------|----------|
| **Buffered** | No active followers | Capture to buffer, send tail on completion |
| **Streaming** | `ljc tail` / `ljc exec --follow` active | Send chunks periodically |

**Chunking triggers (streaming mode):**

| Trigger | Default | Description |
|---------|---------|-------------|
| Time | 3 seconds | Maximum time between chunks |
| Lines | 50 lines | Maximum lines per chunk |
| Size | 32 KB | Maximum chunk size |

**Completion message includes:**
- `output.tail`: Last N lines (default: 100)
- `output.total_lines`: Total lines produced
- `exit_code`: Script exit code
- `duration_ms`: Total execution time

### Configuration

Executor reads environment variables:

| Variable | Description |
|----------|-------------|
| `EXECUTOR_ID` | Executor identity |
| `MQTT_BROKER`, `MQTT_PORT` | MQTT connection |
| `MQTT_SHARED_SECRET` | HMAC signing secret |
| `MINIO_ENDPOINT`, `MINIO_SECURE` | MinIO connection |
| `JOBS_DIR` | Package cache directory |
| `WORK_DIR` | Working directory for job execution |
| `CAPABILITIES` | Comma-separated capability types |

---

## Appendix A: Glossary

| Term | Definition |
|------|------------|
| Register | Named data location in the registry |
| Registry | Collection of all register definitions |
| Job | Unit of work with defined inputs, outputs, and schedule |
| Job Tree | Current execution unit built from explicit `depends:` links |
| Batch Group | Planned: set of jobs that produce one batch result |
| Batch Generation | One scheduled or manual execution of a batch group |
| Barrier Job | Planned: job that waits for multiple input registers from the same generation |
| Execution | Single run of a job |
| Package | Self-contained .ljc archive for deployment |
| Workdir | Temporary directory where job executes (in/, out/, tmp/) |
| Coordinator | Central scheduler |
| Executor | Worker that runs jobs (thin wrapper around linearjc-core) |
| linearjc-core | Shared Rust crate with security-critical execution code |
| ljc | Developer CLI tool |

## Appendix B: Migration Guide

### From v0.5.x to v0.8.x (Register Model)

**Registry Changes:**

1. **`kind` is now required for all types** (was only fs)
   ```yaml
   # Before
   my_minio: {type: minio, bucket: data, prefix: foo/}
   # After
   my_minio: {type: minio, bucket: data, prefix: foo/, kind: dir}
   ```

2. **New `temp` type for intermediates**
   ```yaml
   # Before: Used fs for intermediates
   intermediate: {type: fs, path: /data/temp/intermediate.json, kind: file}
   # After: Use temp (MinIO only, auto-cleaned)
   intermediate: {type: temp, kind: file}
   ```

3. **New `protect` flag for external inputs**
   ```yaml
   # Before: No protection
   source_db: {type: fs, path: /data/source.db, kind: file}
   # After: Protected
   source_db: {type: fs, path: /data/source.db, kind: file, protect: true}
   ```

**Behavioral Changes:**

| Aspect | v0.5.x | v0.8.x |
|--------|--------|--------|
| Output collection | After leaf job only | After EACH job (write-through) |
| Locking | Simple exclusive | ENQ/DEQ (shared+exclusive) |
| Executor crash | Lock held until timeout | Released after 90s (heartbeat) |
| Hot reload | No validation | Safety checks block dangerous changes |

**Backward Compatibility:**
- Jobs with `type: fs` continue to work unchanged
- `protect` defaults to `false`
- Existing chains benefit from write-through caching automatically

## Appendix C: Crate Structure

```
linearjc/
  src/
    linearjc-core/              # Rust - shared, auditable, security-critical
      src/
        lib.rs
        workdir.rs              # Workdir creation and setup
        isolation.rs            # Landlock, cgroups, network namespaces
        execution.rs            # Fork, setuid, exec, timeout
        archive.rs              # Tar/gzip handling
        package.rs              # Package extraction and metadata
        signing.rs              # HMAC message signing
        job.rs                  # Job model types
    coordinator/                # Python - shared utilities (models, signing, archive)
    coordinator_v2/             # Python - async coordinator (aiomqtt)
    executor/                   # Rust - thin wrapper around linearjc-core
      Cargo.toml                # Depends on linearjc-core
      src/
        main.rs
  tools/
    ljc/                        # Rust - developer CLI
      Cargo.toml                # Depends on linearjc-core
      src/
        main.rs
        commands/
          self_update.rs
          ...
  tests/
    unit/                       # Python unit tests
    integration/                # Integration tests
  examples/                     # Example configs and jobs
```
