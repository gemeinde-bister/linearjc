# Example Job Configurations

This directory contains example job configurations for LinearJC.

## Data Registry (Phase 15 Register Model)

LinearJC uses a mainframe-inspired register model with three types:

| Type | Storage | Persistence | Use Case |
|------|---------|-------------|----------|
| `fs` | Filesystem | Permanent | Managed outputs, external inputs |
| `temp` | MinIO | Chain duration | Intermediates between jobs |
| `minio` | MinIO bucket | Permanent | Large objects |

**Example registry.yaml:**
```yaml
registry:
  # Protected input (read-only)
  source_data: {type: fs, path: /data/source.csv, kind: file, protect: true}

  # Managed outputs
  daily_report: {type: fs, path: /data/reports/daily.csv, kind: file}
  archive_dir:  {type: fs, path: /data/archives/, kind: dir}

  # Temporary (chain-only)
  intermediate: {type: temp, kind: file}
```

**kind: file vs dir**
- `kind: file` - Single file. Archive must contain exactly one file.
- `kind: dir` - Directory with contents. Preserves structure.

**protect: true (fs only)**
- External inputs that jobs cannot write to
- Must exist at coordinator startup
- Use for databases, configs from other systems

**Design Philosophy:**
Following mainframe JCL principles - explicitly declare what you're creating (like `DSORG=PS` vs `DSORG=PO`), and the system validates it matches. No guessing from file extensions or runtime inspection.

## Example Jobs

### hello.world.yaml
Simple test job that demonstrates basic LinearJC functionality.

```yaml
job:
  id: hello.world
  version: "1.0.0"
  reads: [hello_input]      # Registry keys for inputs
  writes: [hello_output]    # Registry keys for outputs
  depends: []               # Root job (no dependencies)
  schedule:
    min_daily: 32
    max_daily: 48
  run:
    user: nobody
    timeout: 300
```

- **Duration**: ~1 second
- **Reads**: `hello_input` (from data registry)
- **Writes**: `hello_output` (to data registry)

### hello.followup.yaml
Demonstrates job dependencies and data flow between jobs.

```yaml
job:
  id: hello.followup
  version: "1.0.0"
  reads: [hello_output]       # Reads previous job's output
  writes: [followup_output]
  depends: [hello.world]      # Runs after hello.world
  schedule:
    min_daily: 32
    max_daily: 48
  run:
    user: nobody
    timeout: 300
```

- **Depends**: `hello.world` (waits for it to complete)
- **Reads**: `hello_output` (output from hello.world)
- **Writes**: `followup_output` (to data registry)

## Creating a Timeout Test Job

If you want to test timeout handling, create a job like this:

```yaml
job:
  id: test.timeout
  version: "1.0.0"
  reads: [some_input]
  writes: [timeout_output]
  depends: []
  schedule:
    min_daily: 1
    max_daily: 10
  run:
    user: youruser
    timeout: 30  # 30 second timeout
```

Then create a job script that intentionally runs longer than the timeout (e.g., sleep 60 seconds).

**Expected coordinator warnings when timeout occurs:**
```
[ERROR] [job_tracker] Job test.timeout-YYYYMMDD-HHMMSS-UUID timed out
[WARNING] [job_tracker] Received progress for unknown job: test.timeout-YYYYMMDD-HHMMSS-UUID
```

This is normal - the coordinator times out and stops tracking the job, but the executor continues running and sends completion updates for a job the coordinator has already given up on.

## Using Temp Registers for Chains

For linear chains, use `type: temp` for intermediate data:

```yaml
# registry.yaml
registry:
  source_data: {type: fs, path: /data/source.csv, kind: file, protect: true}
  intermediate: {type: temp, kind: file}  # Chain-only, auto-cleaned
  final_output: {type: fs, path: /data/output.csv, kind: file}

# job1 (root)
job:
  id: process.data
  reads: [source_data]
  writes: [intermediate]  # Goes to MinIO temp
  depends: []

# job2 (depends on job1)
job:
  id: export.result
  reads: [intermediate]   # Reads from MinIO temp (fast)
  writes: [final_output]  # Write-through to filesystem
  depends: [process.data]
```

Temp registers:
- Live only in MinIO during chain execution
- Enable parallel fan-out (each tree gets unique path)
- Auto-cleaned when tree completes or fails
