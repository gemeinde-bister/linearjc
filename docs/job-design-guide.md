# Job Design Guide

## Mental Model

LinearJC follows a mainframe-style sequential processing model:

```
Input Data → Job Script → Output Data → Next Job Script → ...
```

**Key principle**: Write-once, read-many
- Each data source has exactly one writer (job output or external system)
- Multiple jobs can read from the same source
- The data registry manages these named sources/sinks

This is similar to JCL (Job Control Language) on mainframes, where COBOL programs processed sequential data sets in linear chains.

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
if [ ! -f "$LINEARJC_OUTPUT_DIR/output_file/result.txt" ]; then
    echo "ERROR: Expected output not created"
    exit 1
fi

# Success
exit 0
```

**Why**: LinearJC uses exit codes to determine job success/failure. Exiting 0 when outputs fail creates false success reports.

### 3. Environment Variables

Available in all job scripts:

- `LINEARJC_JOB_ID`: Job identifier (e.g., `backup.daily`)
- `LINEARJC_EXECUTION_ID`: Unique execution (e.g., `backup.daily-20251116-140530-a1b2c3d4`)
- `LINEARJC_INPUT_DIR`: Extracted input artifacts directory
- `LINEARJC_OUTPUT_DIR`: Directory for output artifacts

### 4. Output Structure

Create subdirectories matching your job definition output names:

**For directory outputs (path_type: directory):**
```bash
# Job YAML defines: outputs: { website: website_build }
# Registry defines: path_type: directory
mkdir -p "$LINEARJC_OUTPUT_DIR/website"
echo "<html>..." > "$LINEARJC_OUTPUT_DIR/website/index.html"
echo "body { }" > "$LINEARJC_OUTPUT_DIR/website/style.css"
# Multiple files allowed
```

**For single file outputs (path_type: file):**
```bash
# Job YAML defines: outputs: { report: daily_report }
# Registry defines: path_type: file
mkdir -p "$LINEARJC_OUTPUT_DIR/report"
echo "Date,Value" > "$LINEARJC_OUTPUT_DIR/report/report.csv"
# Only ONE file in the directory - validated at extraction
```

**Important:** The output subdirectory name must match the key in your job's `outputs:` section. The registry's `path_type` determines whether one or many files are allowed.

## Job Definition (Coordinator)

Create YAML file in coordinator's jobs directory:

```yaml
job:
  id: backup.daily
  version: "1.0.0"

  # Linear dependencies (runs after these complete)
  depends_on: []

  # Schedule: min/max executions per 24h sliding window
  schedule:
    min_daily: 1    # At least once
    max_daily: 2    # At most twice

  # Executor configuration
  executor:
    user: root      # User to run script as
    timeout: 300    # Seconds (5 minutes)

  # Data registry references
  inputs:
    source_data: raw_sensor_data

  outputs:
    archive: backup_storage
```

## Data Registry Pattern

### Write-Once, Read-Many

The data registry (`data_registry.yaml`) defines named data locations with explicit type declarations:

**Compact YAML format (recommended):**
```yaml
registry:
  # Single file inputs/outputs
  sensor_config: {type: filesystem, path_type: file, path: /data/sensors/config.json, readable: true, writable: false}
  daily_report: {type: filesystem, path_type: file, path: /data/reports/daily.csv, readable: true, writable: true}

  # Directory outputs (multiple files)
  raw_sensor_data: {type: filesystem, path_type: directory, path: /data/sensors/live, readable: true, writable: false}
  backup_storage: {type: filesystem, path_type: directory, path: /data/backups/daily, readable: true, writable: true}

  # MinIO storage
  intermediate_data: {type: minio, bucket: job-artifacts, prefix: temp/, retention_days: 7}
```

**Path Types (required for filesystem entries):**

- **`path_type: file`** - Single file
  - Archive must contain exactly one file
  - Validated at extraction time
  - Example: CSV reports, JSON configs, single log files

- **`path_type: directory`** - Directory with contents
  - Archive can contain multiple files
  - Preserves directory structure
  - Example: Website builds, multi-file datasets, log directories

**Design Philosophy:**
Following mainframe JCL principles - explicitly declare what you're creating (like `DSORG=PS` vs `DSORG=PO`), and LinearJC validates it matches. No guessing from file extensions or runtime inspection.

**Rules:**
1. Each location has exactly ONE writer job
2. Multiple jobs can read from the same location
3. Jobs declare inputs/outputs by registry name, not filesystem paths
4. Coordinator validates no two jobs write to the same location
5. **Path type must match what job produces** (enforced at runtime)

### Validation Errors

LinearJC validates outputs match their declared `path_type`:

**Error: Multiple files when file expected**
```
ArchiveError: Archive contains 3 items but path_type='file' requires exactly one file.
Items: ['data.txt', 'summary.txt', 'log.txt']
```
**Solution:** Change registry to `path_type: directory` or modify job to create single file.

**Error: Directory when file expected**
```
ArchiveError: Archive contains directory 'results' but path_type='file' requires a file
```
**Solution:** Job created subdirectory - flatten output or change to `path_type: directory`.

**Error: Empty archive**
```
ArchiveError: Archive is empty but path_type='file' requires one file
```
**Solution:** Job script didn't create output - fix job logic.

### Locking (Advanced)

For filesystem data sources, the coordinator tracks active writes:
- Job A outputs to `backup_storage` → location locked
- Job B tries to output to `backup_storage` → validation fails
- Jobs complete → lock released

This prevents concurrent writes to the same location.

## Example: Linear Chain

**Scenario**: Sensor data → Daily compaction → Weekly rollup → Archive

```yaml
# Job 1: compact.daily
depends_on: []
inputs:
  live: sensor_live_data
outputs:
  daily: sensor_daily_data

# Job 2: compact.weekly
depends_on: [compact.daily]
inputs:
  daily: sensor_daily_data
outputs:
  weekly: sensor_weekly_data

# Job 3: archive
depends_on: [compact.weekly]
inputs:
  weekly: sensor_weekly_data
outputs:
  archive: long_term_storage
```

Execution order: 1 → 2 → 3 (sequential, deterministic)

## Docker-Based Jobs

Jobs can spawn containers for isolation:

```bash
#!/bin/sh
# Job runs as root (needs Docker socket)
# Container provides isolation boundary

docker run --rm \
  --mount "type=bind,source=${LINEARJC_INPUT_DIR},target=/inputs,readonly" \
  --mount "type=bind,source=${LINEARJC_OUTPUT_DIR}/result,target=/outputs" \
  alpine:latest \
  /bin/sh -c 'process /inputs/* > /outputs/result.txt'

# Validate output created
[ -f "$LINEARJC_OUTPUT_DIR/result/result.txt" ] || exit 1
exit 0
```

**Security note**: Container runs as root for file permissions. Isolation happens at container boundary (standard practice: GitHub Actions, GitLab CI, Jenkins).

## Compiled Binaries (Go, Rust, C, COBOL)

Compiled programs can be integrated via shell wrapper scripts:

```bash
#!/bin/sh
# Wrapper for compiled binary

# Example: Using a standard binary (could be custom Go/Rust/C program)
cat "$LINEARJC_INPUT_DIR/data/input.txt" | \
    tr '[:lower:]' '[:upper:]' | \
    tee "$LINEARJC_OUTPUT_DIR/result/output.txt"

# Validate output exists
if [ ! -f "$LINEARJC_OUTPUT_DIR/result/output.txt" ]; then
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
[ -f "$LINEARJC_OUTPUT_DIR/result/data.out" ] || exit 1

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
6. **Choose correct path_type**:
   - Use `file` for single-file outputs (CSVs, JSON, single logs)
   - Use `directory` for multi-file outputs (websites, datasets, archives)
   - Declare explicitly - don't rely on naming conventions
7. **Registry as contract**: Treat data registry as the contract between jobs - if you change path_type, dependent jobs may break

## When NOT to Use LinearJC

- Complex DAGs with branching/merging
- Dynamic workflows (runtime-determined steps)
- High fan-out parallelism
- Interactive workflows with approvals

Use Airflow/Prefect/Dagu for these cases.

## When to Use LinearJC

- ETL pipelines (extract → transform → load)
- Data compaction chains
- Sequential report generation
- Backup workflows
- Cron job replacement with dependencies
- Low operational overhead requirements

LinearJC trades scheduling flexibility for operational simplicity.
