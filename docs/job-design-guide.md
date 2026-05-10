# Job Design Guide

## Mental Model

LinearJC follows a mainframe-style register-driven batch model:

```
Input Registers → Job Module → Output Registers → Downstream Modules
```

**Key principle**: Write-once, read-many
- Each data source has exactly one writer (job output or external system)
- Multiple jobs can read from the same source
- The data registry manages these named sources/sinks

This is similar to JCL (Job Control Language) on mainframes, where programs process named datasets and the scheduler protects dataset ownership. The important dependency is data readiness: a job can run when all of its input registers are ready for the current batch generation and its output registers can be locked.

| Mainframe concept | LinearJC concept |
|-------------------|------------------|
| Cataloged dataset | Register |
| Temporary dataset | Temp register |
| Program / job step | Job module |
| Job net / application | Batch group |
| GDG generation | Batch generation |
| ENQ / DEQ | Register lock |
| JES spool | Execution archive |

## Job Script Requirements

### 1. Shebang Line (Required)

Every script must declare its interpreter:

```bash
#!/bin/sh              # POSIX shell
#!/bin/bash            # Bash shell
#!/usr/bin/python3     # Python 3
```

The executor uses the shebang directly - no hardcoded interpreters.

### 2. Exit Code (Mandatory)

**Jobs must exit non-zero on failure:**

```bash
# Validate outputs exist
if [ ! -f "out/output_file/result.txt" ]; then
    echo "ERROR: Expected output not created"
    exit 1
fi

# Success
exit 0
```

**Why**: LinearJC uses exit codes to determine job success/failure. Exiting 0 when outputs fail creates false success reports.

### 3. Workdir and Paths

Scripts execute from a workdir with a fixed structure. All I/O uses **relative paths**:

```
{workdir}/
  script.sh       # Your entry script
  bin/             # Bundled binaries (from package)
  data/            # Bundled static data (from package)
  in/              # Inputs (populated by executor, read-only)
  out/             # Outputs (job writes here)
  tmp/             # Scratch space
```

**Environment variables** (identification only, not paths):

- `LINEARJC_JOB_ID`: Job identifier (e.g., `backup.daily`)
- `LINEARJC_EXECUTION_ID`: Unique execution (e.g., `backup.daily-20251116-140530-a1b2c3d4`)

### 4. Output Structure

Create subdirectories under `out/` matching your job definition output names:

**For directory outputs (`kind: dir`):**
```bash
# Job YAML defines: writes: [website]
# Registry defines: kind: dir
mkdir -p out/website
echo "<html>..." > out/website/index.html
echo "body { }" > out/website/style.css
# Multiple files allowed
```

**For single file outputs (`kind: file`):**
```bash
# Job YAML defines: writes: [daily_report]
# Registry defines: kind: file
mkdir -p out/daily_report
echo "Date,Value" > out/daily_report/report.csv
# Only ONE file in the directory - validated at extraction
```

**Important:** The output subdirectory name must match the register key in your job's `writes:` section. The registry's `kind` determines whether one or many files are allowed.

## Job Definition (Coordinator)

Create YAML file in coordinator's jobs directory:

```yaml
job:
  id: backup.daily
  version: "1.0.0"

  # Registry references (Phase 15 format)
  reads: [sensor_data, config]      # Input registers
  writes: [backup_archive]          # Output registers

  # Current linear-chain dependency field.
  # Register reads/writes are the primary data contract.
  depends: []

  # Schedule: min/max executions per 24h sliding window
  schedule:
    min_daily: 1    # At least once
    max_daily: 2    # At most twice

  # Execution configuration
  run:
    user: backup           # User to run script as
    timeout: 300           # Seconds (5 minutes)
    entry: script.sh       # Entry script
    isolation: strict      # strict | relaxed | none
    network: true          # Allow network access
    limits:                # Optional resource limits
      cpu_percent: 50
      memory_mb: 512
```

**Key fields:**
- `reads`: List of registry keys this job reads from
- `writes`: List of registry keys this job writes to (must not be protected)
- `depends`: Current chain predecessor field. Prefer `reads:`/`writes:` as the primary dependency contract; future barrier execution should infer multi-input waits from register ownership.

## Data Registry Pattern

### Write-Once, Read-Many

The data registry (`registry.yaml`) defines named data locations called **registers**. The Phase 15 register model supports three types:

| Type | Storage | Persistence | Use Case |
|------|---------|-------------|----------|
| `fs` | Filesystem | Permanent | Managed outputs, external inputs |
| `temp` | MinIO only | Chain duration | Intermediate data between jobs |
| `minio` | MinIO bucket | Permanent | Large objects, cross-system sharing |

**Registry Format:**
```yaml
registry:
  # External input (protected - cannot be written by jobs)
  source_db: {type: fs, path: /data/source.db, kind: file, protect: true}

  # Managed outputs (written by jobs)
  daily_report: {type: fs, path: /data/reports/daily.csv, kind: file}
  backup_dir:   {type: fs, path: /data/backups/daily/, kind: dir}

  # Temporary (chain-only, MinIO storage)
  intermediate: {type: temp, kind: file}

  # MinIO permanent storage
  large_data: {type: minio, bucket: job-artifacts, prefix: outputs/, kind: dir}
```

### Register Types

**`type: fs` - Filesystem Register**
```yaml
my_output: {type: fs, path: /absolute/path, kind: file|dir}
my_input:  {type: fs, path: /absolute/path, kind: file|dir, protect: true}
```

- `path`: Absolute filesystem path (required)
- `kind`: `file` (single file) or `dir` (directory)
- `protect`: If `true`, register is read-only (external input that jobs cannot write to)

Protected registers:
- Must exist at coordinator startup (validated)
- Jobs can read but cannot declare in `writes:`
- Use for external data sources (databases, configs from other systems)

**`type: temp` - Temporary Register**
```yaml
intermediate: {type: temp, kind: file|dir}
```

- No filesystem path - lives only in MinIO during chain execution
- Automatically cleaned when tree completes or fails
- Enables parallel fan-out (each tree gets unique MinIO path)
- Use for data passed between chain jobs that doesn't need persistence

**`type: minio` - MinIO Register**
```yaml
large_object: {type: minio, bucket: my-bucket, prefix: data/, kind: file|dir}
```

- `bucket`: MinIO bucket name (required, S3 naming rules)
- `prefix`: Object prefix within bucket (optional)
- `kind`: `file` or `dir`
- Use for large objects or data accessed from multiple systems

### Kind: file vs dir

- **`kind: file`** - Single file
  - Archive must contain exactly one file
  - Validated at extraction time
  - Example: CSV reports, JSON configs

- **`kind: dir`** - Directory with contents
  - Archive can contain multiple files
  - Preserves directory structure
  - Example: Website builds, multi-file datasets

### Design Rules

1. Each register has exactly ONE writer job (enforced)
2. Multiple jobs can read from the same register
3. Jobs declare `reads:/writes:` by registry name, not filesystem paths
4. Protected registers (`protect: true`) cannot be written to
5. Temp registers don't persist after chain completion

### Validation Errors

**Error: Job writes to protected register**
```
SecurityError: Job 'my.job' cannot write to protected register 'source_db'
Protected registers are external inputs (protect: true) that cannot be overwritten by jobs.
```
**Solution:** Use a different register for output, or remove `protect: true` if the job should manage this data.

**Error: Protected register missing at startup**
```
SecurityError: Protected registers not found (protect: true must exist):
  - source_db: /data/missing.db
```
**Solution:** Ensure the external data source exists before starting the coordinator.

**Error: Multiple files when file expected**
```
ArchiveError: Archive contains 3 items but kind='file' requires exactly one file.
```
**Solution:** Change registry to `kind: dir` or modify job to create single file.

### Locking (ENQ/DEQ)

The coordinator uses mainframe-style locking (Phase 15):

- **Exclusive lock**: Acquired for `writes:` registers
- **Shared lock**: Acquired for `reads:` registers (multiple readers allowed)
- **Protected**: Always shared-only (no exclusive locks possible)
- **Temp**: No locking (unique MinIO path per tree execution)

Lock acquisition is all-or-nothing: a tree gets all locks or waits in queue holding none. This prevents deadlock and ensures fair scheduling.

## Example: Linear Chain

**Scenario**: Sensor data → Daily compaction → Weekly rollup → Archive

```yaml
# Registry
registry:
  sensor_live: {type: fs, path: /data/sensors/live, kind: dir, protect: true}
  daily_data:  {type: temp, kind: file}  # Intermediate - no persistence needed
  weekly_data: {type: temp, kind: file}  # Intermediate
  archive:     {type: fs, path: /data/archive/sensors.tar.gz, kind: file}

# Job 1: compact.daily (chain root)
job:
  id: compact.daily
  version: "1.0.0"
  reads: [sensor_live]
  writes: [daily_data]
  depends: []
  schedule: {min_daily: 1, max_daily: 1}
  run: {user: sensor, timeout: 600}

# Job 2: compact.weekly (chain middle)
job:
  id: compact.weekly
  version: "1.0.0"
  reads: [daily_data]
  writes: [weekly_data]
  depends: [compact.daily]
  schedule: {min_daily: 1, max_daily: 1}
  run: {user: sensor, timeout: 1200}

# Job 3: archive (chain leaf)
job:
  id: archive.sensors
  version: "1.0.0"
  reads: [weekly_data]
  writes: [archive]
  depends: [compact.weekly]
  schedule: {min_daily: 1, max_daily: 1}
  run: {user: sensor, timeout: 600}
```

**Execution flow**:
1. `compact.daily` reads protected `sensor_live`, writes to temp `daily_data`
2. `compact.weekly` reads temp `daily_data` (cache hit from MinIO), writes to temp `weekly_data`
3. `archive.sensors` reads temp `weekly_data`, writes to permanent `archive` (write-through to filesystem)
4. Tree completes → temp data cleaned from MinIO

## Example: Multi-Input Barrier Job (Planned)

**Scenario**: Two extraction modules produce temporary results. A report module waits until both results exist for the same batch generation.

```yaml
# Registry
registry:
  source_sales:      {type: fs, path: /data/source/sales.csv, kind: file, protect: true}
  source_inventory:  {type: fs, path: /data/source/inventory.csv, kind: file, protect: true}
  sales_extract:     {type: temp, kind: file}
  inventory_extract: {type: temp, kind: file}
  daily_report:      {type: fs, path: /data/reports/daily.csv, kind: file}

# Job 1: extract.sales
job:
  id: extract.sales
  version: "1.0.0"
  reads: [source_sales]
  writes: [sales_extract]
  depends: []
  schedule: {min_daily: 1, max_daily: 1}
  run: {user: batch, timeout: 300}

# Job 2: extract.inventory
job:
  id: extract.inventory
  version: "1.0.0"
  reads: [source_inventory]
  writes: [inventory_extract]
  depends: []
  schedule: {min_daily: 1, max_daily: 1}
  run: {user: batch, timeout: 300}

# Job 3: build.report
job:
  id: build.report
  version: "1.0.0"
  reads: [sales_extract, inventory_extract]
  writes: [daily_report]
  # Future barrier mode should infer this wait from reads/writes.
  depends: []
  schedule: {min_daily: 1, max_daily: 1}
  run: {user: batch, timeout: 600}
```

**Execution flow**:
1. `extract.sales` writes `sales_extract` for generation `daily.close-...`
2. `extract.inventory` writes `inventory_extract` for the same generation
3. `build.report` becomes eligible only after both temp registers are ready
4. `daily_report` is locked exclusively and committed as the durable output

This is a barrier, not a free-form workflow graph. Users declare module inputs and outputs; the controller derives the wait from register ownership.

## Docker-Based Jobs

Jobs can spawn containers for isolation:

```bash
#!/bin/sh
# Job runs as root (needs Docker socket)
# Container provides isolation boundary

WORKDIR="$(pwd)"

docker run --rm \
  --mount "type=bind,source=${WORKDIR}/in,target=/inputs,readonly" \
  --mount "type=bind,source=${WORKDIR}/out/result,target=/outputs" \
  alpine:latest \
  /bin/sh -c 'process /inputs/* > /outputs/result.txt'

# Validate output created
[ -f "out/result/result.txt" ] || exit 1
exit 0
```

**Security note**: Container runs as root for file permissions. Isolation happens at container boundary (standard practice: GitHub Actions, GitLab CI, Jenkins).

## Compiled Binaries (Go, Rust, C, COBOL)

Compiled programs can be integrated via shell wrapper scripts:

```bash
#!/bin/sh
# Wrapper for compiled binary

# Example: Using a standard binary (could be custom Go/Rust/C program)
cat in/data/input.txt | \
    tr '[:lower:]' '[:upper:]' | \
    tee out/result/output.txt

# Validate output exists
if [ ! -f "out/result/output.txt" ]; then
    echo "ERROR: Binary failed to create output"
    exit 1
fi

exit 0
```

**Pattern for custom binaries:**

```bash
#!/bin/sh
# Job runs pre-compiled program

# Execute binary (reads LINEARJC_* environment variables)
./my-program

# Check exit code
if [ $? -ne 0 ]; then
    echo "ERROR: Program failed"
    exit 1
fi

# Validate expected outputs
[ -f "out/result/data.out" ] || exit 1

exit 0
```

**Why use wrappers:**
- Validates inputs before execution
- Checks outputs after execution
- Handles errors explicitly
- Maintains consistent shebang pattern
- Provides logging context

## Best Practices

1. **Validate outputs**: Always check expected files exist before `exit 0`
2. **Atomic writes**: Write to temp file, then move to final location
3. **Idempotent**: Jobs should produce same output given same input
4. **Logging**: Write to stdout/stderr (captured in executor logs)
5. **Cleanup**: Remove temp files on failure (or rely on executor cleanup)
6. **Choose correct `kind`**:
   - Use `file` for single-file outputs (CSVs, JSON, single logs)
   - Use `dir` for multi-file outputs (websites, datasets, archives)
   - Declare explicitly - don't rely on naming conventions
7. **Registry as contract**: Treat data registry as the contract between jobs - if you change `kind`, dependent jobs may break

## When NOT to Use LinearJC

- Arbitrary DAG authoring or UI-driven workflow design
- Dynamic workflows (runtime-determined steps)
- Interactive workflows with approvals
- Sub-second scheduling requirements

Use Airflow/Prefect/Dagu for these cases.

**Note**: Fan-out is supported (one job → multiple parallel chains via temp registers). Multi-input barrier jobs are planned as a register-driven extension, not as a shift toward general DAG orchestration.

## When to Use LinearJC

- ETL pipelines (extract → transform → load)
- Data compaction chains
- Sequential report generation
- Multi-module batch consolidation where outputs are joined by register readiness
- Backup workflows
- Cron job replacement with dependencies
- Low operational overhead requirements
- Single-coordinator deployments

LinearJC trades scheduling flexibility for operational simplicity and a small external service set.
