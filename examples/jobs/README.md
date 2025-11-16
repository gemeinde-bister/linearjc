# Example Job Configurations

This directory contains example job configurations for LinearJC.

## Example Jobs

### hello.world.yaml
Simple test job that demonstrates basic LinearJC functionality.

- **Duration**: ~1 second
- **Timeout**: 300 seconds
- **Schedule**: 32-48 runs/day
- **Dependencies**: None (root job)
- **Inputs**: `hello_input` (from data registry)
- **Outputs**: `hello_output` (to data registry)

This job reads an input file, processes it, and writes output.

### hello.followup.yaml
Demonstrates job dependencies and data flow between jobs.

- **Duration**: ~1 second
- **Timeout**: 300 seconds
- **Schedule**: 32-48 runs/day
- **Dependencies**: `hello.world` (waits for it to complete)
- **Inputs**: `hello_output` (output from hello.world)
- **Outputs**: `followup_output` (to data registry)

This job runs after `hello.world` completes and processes its output.

## Creating a Timeout Test Job

If you want to test timeout handling, create a job like this:

```yaml
job:
  id: test.timeout
  version: "1.0.0"
  depends_on: []

  schedule:
    min_daily: 1
    max_daily: 10

  executor:
    user: youruser
    timeout: 30  # 30 second timeout

  inputs:
    input_file: some_input

  outputs:
    output_file: timeout_output
```

Then create a job script that intentionally runs longer than the timeout (e.g., sleep 60 seconds).

**Expected coordinator warnings when timeout occurs:**
```
[ERROR] [job_tracker] Job test.timeout-YYYYMMDD-HHMMSS-UUID timed out
[WARNING] [job_tracker] Received progress for unknown job: test.timeout-YYYYMMDD-HHMMSS-UUID
```

This is normal - the coordinator times out and stops tracking the job, but the executor continues running and sends completion updates for a job the coordinator has already given up on.
