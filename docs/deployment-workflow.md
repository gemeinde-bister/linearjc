# LinearJC Deployment Workflow

This document describes the end-to-end workflow for deploying jobs to a LinearJC production environment.

## Prerequisites

1. **job-repo repository** - A local ljc repository with `.ljcconfig` pointing to your coordinator
2. **LINEARJC_SECRET** - Shared secret for signing MQTT messages (stored in a deployment vault or secret manager)
3. **Coordinator running** on coordinator-host with MQTT and MinIO
4. **Executor running** on executor-host

## Complete Workflow

### 1. Create Output Registry Entry

First, define where your job's output will be stored:

```bash
cd job-repo

# Add a new registry entry
./ljc registry add musl_toolchain \
    --type fs \
    --path /var/lib/linearjc/work/outputs/musl_toolchain.tar.xz \
    --kind file

# Push to coordinator
./ljc registry push
```

The coordinator will save the new entry and it becomes immediately available.

### 2. Create Job

```bash
# Create job scaffold
./ljc new build.musl-toolchain

# Edit job.yaml with correct settings
# Edit script.sh with job logic
```

### 3. Job Configuration (job.yaml)

Key fields to configure correctly:

```yaml
job:
  id: build.musl-toolchain
  version: 1.0.0

  reads: []                    # Input registers (empty if git clone is source)
  writes: [musl_toolchain]     # Output registers

  schedule:
    min_daily: 1               # MUST be >= 1 (0 causes validation error)
    max_daily: 1               # Maximum runs per 24h

  run:
    user: builder              # Non-root user for builds
    timeout: 36000             # Long timeout for big builds (10 hours)
    isolation: none            # Required for git/network access
    network: true              # Required for git clone
```

**Important Constraints:**
- `min_daily` must be >= 1 (use `ljc exec` for on-demand execution)
- `timeout` in seconds - set appropriately for long builds
- `isolation: none` required for jobs that need git, network, or system tools
- `user` should be a real user on the executor (not `nobody` for builds)

### 4. Validate

```bash
./ljc validate build.musl-toolchain
```

This checks:
- All referenced registers exist
- No output conflicts with other jobs
- Script has valid shebang
- Required fields present

### 5. Build Package

```bash
./ljc build build.musl-toolchain
```

Creates `dist/build.musl-toolchain.ljc` (~1KB for simple jobs).

### 6. Deploy

```bash
./ljc deploy dist/build.musl-toolchain.ljc --to 192.0.2.10
```

This:
1. Uploads package to MinIO
2. Coordinator downloads and installs
3. **Coordinator auto-reloads** (no restart needed!)
4. Executor receives job via on-demand distribution
5. Job starts executing if schedule permits

### 7. Version Updates

When updating a deployed job:

```bash
# Bump version (required for re-deployment)
./ljc bump patch build.musl-toolchain

# Rebuild and redeploy
./ljc build build.musl-toolchain
./ljc deploy dist/build.musl-toolchain.ljc --to 192.0.2.10
```

**Note:** The coordinator rejects packages with the same version as already installed.

## Monitoring

```bash
# Check active executions
./ljc ps

# Follow job progress
./ljc tail build.musl-toolchain

# View execution history
./ljc logs build.musl-toolchain

# Trigger immediate execution (bypass scheduler)
./ljc exec build.musl-toolchain --follow
```

## Auto-Reload Behavior

The coordinator automatically reloads jobs after installation:
- Sets `_reload_requested = True` after `install_package_from_api()`
- Scheduler loop checks this flag every `loop_interval` seconds (default: 10s)
- Calls `reload_jobs()` which re-discovers and rebuilds trees

**No manual restart required** for:
- New job deployments
- Job version upgrades
- Registry pushes (registry is saved immediately)

Manual restart only needed for:
- Coordinator code updates
- Configuration file changes

## Common Issues

### "min_daily must be >= 1"

The coordinator validates `min_daily >= 1`. For on-demand jobs:
```yaml
schedule:
  min_daily: 1    # Set to 1, use ljc exec for manual triggers
  max_daily: 1
```

### "Version already installed"

Bump the version before redeploying:
```bash
./ljc bump patch <job-id>
```

### Job not discovered after deploy

Check coordinator logs for validation errors:
```bash
docker logs linearjc-coordinator 2>&1 | grep -i "validation\|error"
```

Common causes:
- Missing `run` section in job.yaml
- Invalid schedule values
- Unknown register references

### Executor doesn't have job

The coordinator uses heartbeat-based discovery and on-demand distribution:
1. Executor heartbeats advertise installed job capabilities
2. If job is not cached, coordinator uploads it to MinIO and announces it
3. Executor downloads and installs the job package
4. Coordinator retries dispatch

This typically completes within 10-15 seconds.

### "Output conflict detected" after failed job

**Symptom:**
```
Error: Output conflict detected for tree 'my-job':
  • Path: /var/lib/linearjc/work/outputs/my_output
    Conflict with: tree 'my-job' (currently active)
```

**Cause:** A previous execution may still be active or may not have released its register locks cleanly.

**Workaround:** Inspect active executions first; if the lock is stale, restart the coordinator:
```bash
docker restart linearjc-coordinator
```

Coordinator v2 is expected to clean this up automatically after failed or timed-out executions.

## Production Environment

| Component | Host | Port |
|-----------|------|------|
| Coordinator | coordinator-host (192.0.2.10) | - |
| MQTT Broker | coordinator-host | 1884 |
| MinIO | coordinator-host | 9000 |
| Executor | executor-host (192.0.2.20) | - |

## Wrapper Script

The `job-repo/ljc` wrapper can source the shared secret from your deployment vault:

```bash
#!/bin/bash
set -e
REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
LJC_BIN="$REPO_ROOT/submodules/linearjc/tools/ljc/target/x86_64-unknown-linux-musl/release/ljc"
LINEARJC_SECRET="$("$REPO_ROOT/scripts/read-linearjc-secret")"
export LINEARJC_SECRET
exec "$LJC_BIN" "$@"
```

This allows using `./ljc` commands without manually exporting the secret.
