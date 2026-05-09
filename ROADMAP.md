---
# LinearJC Project Roadmap

**Last Updated**: 2026-01-12
**Current Version**: v0.3.1 (Coordinator/Executor) | v0.2.0-alpha (ljc tool)
**Status**: ✅ Phases 3,7,8,11,12,14.1-14.5 Complete | Coordinator v2 in Production

---

## 🚀 Start Here (New Session Quick Start)

**Phase 15 Complete** ✅ (2026-01-12)

Fan-out support and automatic cleanup:
- ✅ Fan-out: Jobs with multiple dependents create separate trees (A→B, A→C supported)
- ✅ Automatic cleanup: Intermediate MinIO objects cleaned after chain completion
- ✅ Startup cleanup: Orphaned data (>24h) cleaned on coordinator start
- ✅ Periodic cleanup: Hourly orphan cleanup in maintenance loop
- ✅ 129 tests passing

**Reference**: `docs/linearjc-coordinator-rewrite-proposal.md`

**Completed Phases:**
- Phase 3 (Job Creation) ✅ - `ljc new` scaffolding command
- Phase 7 (Process Isolation) ✅ - Landlock, cgroups v2, network namespaces
- Phase 8 (linearjc-core) ✅ - shared crate + `ljc test` command
- Phase 11 (Developer Commands) ✅ - status, ps, logs, kill
- Phase 12 (Chain Execution) ✅ - multi-job dependencies with fan-out support
- Phase 14.1 (Heartbeat Protocol) ✅ - Push-based executor discovery
- Phase 14.4 (Legacy Cleanup) ✅ - Removed poll-based discovery, old coordinator entry points
- Phase 14.5 (Tool Self-Update) ✅ - `ljc self-update` command for centralized version management
- Phase 15 (Fan-out & Cleanup) ✅ - Fan-out support, automatic intermediate cleanup

**Phase 14 - Coordinator Async Rewrite** ✅ Complete

Fixed critical threading issues with single-threaded async architecture:
- **Fixed Deadlock**: `_execute_chain_job()` no longer blocks MQTT thread
- **Fixed Race conditions**: Single-threaded async eliminates all race conditions by design
- **Fixed Blocking discovery**: Heartbeat-based executor discovery is non-blocking

**Solution**: Single-threaded async architecture with aiomqtt + heartbeat-based discovery.

See `docs/linearjc-coordinator-rewrite-proposal.md` for full technical spec.
Pre-rewrite snapshot: local history revision before coordinator-v2 rewrite

**Phase 14.1: Heartbeat Protocol** ✅ (2026-01-12)

Implemented push-based executor discovery (parallel to existing poll-based):

| Component | Change |
|-----------|--------|
| **Executor** | `publish_heartbeat()` every 30s, subscribe to `coordinator/online` |
| **Coordinator** | Subscribe to `heartbeat/+`, `handle_heartbeat()`, `prune_stale_executors()` |
| **Protocol** | New topics: `linearjc/heartbeat/{executor_id}`, `linearjc/coordinator/online` |

Deployed to production, validated with `export.spending` job execution.

**Phase 14.2: Coordinator Async Rewrite** ✅ (Complete - 2026-01-12)

Core components implemented in `src/coordinator_v2/`:

| Component | Status | Description |
|-----------|--------|-------------|
| `EventRouter` | ✅ | Pattern-based MQTT message routing |
| `ExecutorRegistry` | ✅ | Heartbeat-based executor tracking (no locks) |
| `JobStateMachine` | ✅ | Explicit state transitions with validation |
| `JobTracker` | ✅ | Job execution registry with state machines |
| `TreeValidator` | ✅ | Output conflict detection |
| `TreeTracker` | ✅ | Chain execution state with WAITING_EXECUTOR |
| `Coordinator` | ✅ | Main async class with aiomqtt |
| Developer API | ✅ | All 9 handlers implemented (deploy, registry, exec, tail, status, ps, logs, kill) |
| CLI commands | ✅ | All 7 commands (run, cleanup, install, status, monitor, test-job, timeline) |
| Security validation | ✅ | `validate_security()` at startup |
| Path validation | ✅ | `validate_path()` for registry entries |
| Structured logging | ✅ | Correlation IDs, `log_with_fields()`, `log_duration()` |
| Unit tests | ✅ | 119 tests passing |
| Integration test | ✅ | Tested with Rust executor - 7 tests passing |

Security validation includes:
- `validate_shared_secret()` - Minimum length/entropy check
- `validate_minio_credentials()` - Credential format validation
- `allowed_data_roots` existence check
- Registry path validation against allowed roots

Developer API handlers complete:
- Deploy request/complete: Presigned URLs, package installation with version checking
- Registry sync/push: Pull and merge registry entries with atomic save
- Exec request: Immediate job execution (async, no threading needed)
- Tail request: Attach to running execution for progress updates
- Status/PS/Logs/Kill: Job queries and control

Fixes applied during integration testing:
- MinioManager API: Fixed method name mismatches (presign_* → generate_presigned_*)
- MinioManager init: Fixed to pass config object instead of individual parameters
- build_trees(): Fixed to use correct single-argument signature

**Phase 14.3: Production Cutover** ✅ (Complete - 2026-01-12)

Coordinator v2 deployed to production:
1. ✅ Phase 14.2 complete (all gaps addressed)
2. ✅ Deploy coordinator_v2 to coordinator-host
3. ✅ Verify existing jobs work correctly (ljc exec tested)
4. ✅ Remove old coordinator code (completed in Phase 14.4)
5. ✅ Update documentation

**Config changes applied**:
- Added `aiomqtt` to production venv
- Added `/data` to `allowed_data_roots`

**Critical bugs fixed**:
1. `_prepare_fs_input()` was only generating presigned URLs without actually archiving and uploading files to MinIO. Fixed to properly create archive and upload before generating URL.
2. `_collect_outputs()` was a stub that didn't actually download and extract outputs. Fixed to properly download from MinIO and extract to filesystem.

**Phase 14.4: Legacy Cleanup** ✅ (Complete - 2026-01-12)

Removed deprecated poll-based executor discovery:

| Component | Change |
|-----------|--------|
| **Executor** | Removed `linearjc/query/capabilities` subscription and response handler |
| **Coordinator** | Removed `src/coordinator/main.py` and `mqtt_client.py` (old entry points) |
| **SPEC.md** | Removed legacy `linearjc/executors/query` and `linearjc/executors/announce` topics |

Code cleanup:
- Executor now uses heartbeat-only discovery (no polling fallback)
- Old coordinator entry points removed (shared utility modules retained for tests)
- All 119 unit tests passing

**Phase 14.5: Tool Self-Update** ✅ (Complete - 2026-01-12)

Centralized version management for ljc CLI and executor auto-update:

| Component | Change |
|-----------|--------|
| **Coordinator** | Added `_load_tools_registry()`, `_on_tools_version_request()`, `_check_executor_update()` |
| **ljc** | Added `self-update` command with version comparison, download, checksum verification |
| **Executor** | Added `executor_version` and `platform` to heartbeat, self-update handler |
| **Config** | Added `tools_registry` field to CoordinatorConfig |

**ljc Self-Update:**
```bash
ljc self-update --check    # Check for updates
ljc self-update            # Download and install
```

**Executor Auto-Update Flow:**
1. Executor sends heartbeat with `executor_version` and `platform`
2. Coordinator compares with `tools-registry.yaml`
3. If update needed, coordinator publishes to `linearjc/executors/{id}/update`
4. Executor downloads new binary, verifies checksum, replaces itself, exits
5. Init system restarts executor with new version

**MQTT Topics:**
- `linearjc/tools/version/request/{coord_id}` - ljc version query
- `linearjc/tools/version/response/{client_id}` - Version info + download URL
- `linearjc/executors/{executor_id}/update` - Executor update command

**Tools registry format:**
```yaml
tools:
  ljc:
    x86_64-unknown-linux-musl:
      version: "0.2.0"
      checksum_sha256: "..."
      path: "tools/ljc-0.2.0-x86_64-linux-musl"
  linearjc-executor:
    x86_64-unknown-linux-musl:
      version: "0.1.1"
      checksum_sha256: "..."
      path: "tools/linearjc-executor-0.1.1-x86_64-linux-musl"
```

**✅ Executor Auto-Update Production Tested (2026-01-12)**

Tested on production: coordinator-host detected executor v0.1.0, sent update to v0.1.1, executor downloaded from MinIO, verified checksum, replaced binary, restarted successfully.

**✅ ljc Self-Update Production Tested (2026-01-12)**

Tested on production: ljc v0.1.0 queried coordinator for updates, detected v0.1.1 available, downloaded from MinIO, verified checksum, replaced binary successfully. `ljc --version` confirmed update.

**✅ Production Deployment Validated**

| Environment | Purpose | VMs |
|-------------|---------|-----|
| **LOCAL** | Development & testing | Your workstation (pytest spawns all services) |
| **PRODUCTION** | Real workloads | coordinator-host (coordinator), executor-host (executor) |

Production files:
- `/var/local/pdata/linearjc/tools-registry.yaml` - Tool version registry
- `/var/local/pdata/linearjc/config.yaml` - Coordinator config (includes `tools_registry` path)

See `docs/deployment-workflow.md` for complete guide.

**Phase 15: Fan-Out Support & Automatic Cleanup** ✅ (Complete - 2026-01-12)

Extended chain execution with fan-out and comprehensive cleanup:

**Fan-Out Support:**
Jobs with multiple dependents now create separate trees (each executes the shared prefix):

```
Supported patterns:
✅ A → B  and  A → C     → Trees: [A,B], [A,C]  (A runs twice)
✅ A → B → C  and  A → B → D  → Trees: [A,B,C], [A,B,D]  (A,B run twice)

Not supported:
❌ A → C  and  B → C     → Merge points (C depends on both A and B)
```

**Example job definitions:**
```yaml
# process.data runs TWICE (once per consuming tree)
- id: process.data
  depends: []
  writes: [processed.output]

- id: analyze.spending
  depends: [process.data]  # Tree 1: [process.data, analyze.spending]

- id: export.report
  depends: [process.data]  # Tree 2: [process.data, export.report]
```

**Automatic Cleanup:**
| Trigger | Age Threshold | What's Cleaned |
|---------|---------------|----------------|
| Chain completion | Immediate | `jobs/{tree_exec_id}/` MinIO objects + work dir |
| Chain failure | Immediate | `jobs/{tree_exec_id}/` MinIO objects + work dir |
| Job timeout | Immediate | `jobs/{tree_exec_id}/` MinIO objects + work dir |
| Coordinator startup | 24 hours | All orphaned `jobs/*` objects + work dirs |
| Periodic (hourly) | 24 hours | All orphaned `jobs/*` objects + work dirs |

**Configuration:**
```yaml
coordinator:
  cleanup_age_hours: 24  # Optional, default 24 hours
```

**Test Results:** 129 unit tests passing (including 3 new fan-out tests)

**Completed Work** (Phase 11 - Developer Runtime Commands):
1. ✅ `ljc exec` command - immediate job execution via coordinator
2. ✅ `--follow` / `--wait` modes - progress forwarding working (race condition fixed)
3. ✅ `ljc tail` command - follow job execution progress in real-time
4. ✅ `ljc status` command - query job scheduling status
5. ✅ `ljc ps` command - list active job executions
6. ✅ `ljc logs` command - show execution history
7. ✅ `ljc kill` command - cancel running jobs

**✅ Multi-Job Chain Support (Phase 12 Complete)**

Job dependency chains (`a → b → c`) now fully execute:
- Tree building: `depends: [job_a]` correctly builds ordered `tree.jobs[]`
- Chain execution: Coordinator triggers subsequent jobs on completion
- Intermediate data: Passed via MinIO between chain jobs
- Progress forwarding: All jobs in chain forward to `ljc exec --follow` / `ljc tail`

| Tree Type | Status |
|-----------|--------|
| Single job (`a`) | ✅ Works |
| Chain (`a → b → c`) | ✅ Works |

**Tracking**: Phase 12 - Complete (2025-12-03)

**Current codebase**:
- Executor: `src/executor/src/main.rs` (job execution logic)
- Coordinator: `src/coordinator/main.py` (job scheduling)
- Tool: `tools/ljc/` (developer CLI)

**Work from**: `/path/to/ansible/` (repo root)

---

## Local Test Infrastructure

**All testing happens locally using spawned services:**

```bash
# Run all tests
cd submodules/linearjc
pytest tests/ -v

# Run specific test levels
pytest tests/unit/ -v           # Model validation (fast, no services)
pytest tests/integration/ -v    # MQTT + MinIO connectivity
pytest tests/e2e/ -v -s         # Full coordinator + executor flow
```

**Test Infrastructure Components** (auto-spawned by pytest fixtures):

| Component | Local Test | Production |
|-----------|------------|------------|
| MQTT Broker | Spawned Mosquitto (random port) | coordinator-host:1883 |
| MinIO | Spawned MinIO (test credentials) | coordinator-host MinIO |
| Coordinator | Python subprocess | Docker on coordinator-host |
| Executor | Rust binary subprocess | Service on executor-host |

**Key fixtures** (`tests/conftest.py`):
- `mosquitto_server` - Session-scoped MQTT broker
- `minio_server` - Session-scoped S3-compatible storage
- `temp_work_dir` - Per-test temporary directory

**E2E fixtures** (`tests/e2e/conftest.py`):
- `coordinator_process` - Session-scoped spawned coordinator
- `executor_process` - Session-scoped spawned executor binary
- `coordinator_ready` - Readiness check (polls instead of sleeping)
- `wait_for_coordinator()` - Helper function for polling readiness
- Automatic log capture (last 2000 chars on failure)

**Performance**: Session-scoped fixtures share services across ALL test modules, eliminating redundant startup. Tests use readiness polling instead of fixed sleeps.

**Test Coverage** (137 passed, 8 skipped):
- ✅ Unit: Model validation, tree building (43 tests)
- ✅ Integration: MQTT pub/sub, MinIO upload/download (8 tests)
- ✅ E2E: Full job execution flow (coordinator → executor → output)
- ✅ E2E: `ljc deploy` command (6 tests)
- ✅ E2E: `ljc exec/tail` commands (11 tests)
- ✅ E2E: `ljc status/ps/logs/kill` commands (27 tests)

---

## Production Environment (Deploy Only After Local Tests Pass)

**DO NOT deploy untested code to these machines.**

| VM | Role | IP | Status |
|----|------|-----|--------|
| coordinator-host | Coordinator + MQTT + MinIO | 192.0.2.10 | 🟢 Production |
| executor-host | Executor (Phase 7 isolation) | 192.0.2.20 | 🟢 Production |

**Production deployment** (only after `pytest tests/ -v` passes):
- See `docs/hosts/executor-host.md` for executor deployment guide
- Kernel 6.12.37 with Landlock, cgroups v2, network namespaces

---

## Recent Updates

### **2025-12-04: Phase 13 - Job Output Visibility (Design Complete)** 🚧

**Status**: SPEC updated, implementation pending

**Problem Identified**: When `build.musl-toolchain` job failed, `ljc logs` only showed timestamp - not WHY it failed. Had to SSH to executor and grep through logs to find:
```
/bin/sh: rsync: not found
make: *** [Makefile:1361: headers_install] Error 127
```

**Design Decision**: Job output is part of the execution result, not a separate logging concern.

**Solution**:
1. Executor captures stdout/stderr to buffer
2. On completion: include tail (last 100 lines) in progress message
3. During execution: stream chunks every 3s (only when `ljc tail` attached)
4. Coordinator stores tail in execution history
5. `ljc logs` displays stored tails, `ljc tail` displays streaming chunks

**SPEC Updated**: `SPEC.md` → Part 3: Executor → Output Capture
**ROADMAP Updated**: Phase 13 implementation tasks defined (~9h)

**No new infrastructure** - just enhanced progress messages.

---

### **2025-12-04: E2E Tests for Developer Commands + Performance Optimization** ✅

**Status**: E2E test coverage complete for all Phase 11 commands

**What Was Added**:

**E2E Tests** (27 new tests):
- `tests/e2e/test_status.py` - 7 tests (query job scheduling state)
- `tests/e2e/test_ps.py` - 6 tests (list active executions)
- `tests/e2e/test_logs.py` - 8 tests (execution history)
- `tests/e2e/test_kill.py` - 6 tests (job cancellation with signal verification)

**Test Infrastructure Performance Optimization**:
- Changed fixtures from `scope="module"` to `scope="session"` - services shared across ALL test modules
- Added `wait_for_coordinator()` readiness polling helper - replaces `time.sleep(2)`
- Added `coordinator_ready` fixture - verifies readiness once per session

**Performance Results**:
| Metric | Before | After | Improvement |
|--------|--------|-------|-------------|
| 4 new test files | 68s | 12.6s | **5.4x faster** |
| Full test suite | ~150s | ~65s | **2.3x faster** |
| Per-test time | 2.25s | 0.25s | **9x faster** |

**Root causes fixed**:
1. Defensive `time.sleep(2)` in every test (now uses polling)
2. Module-scoped fixtures respawning services per test file (now session-scoped)

---

### **2025-12-04: Phase 11 - Developer Runtime Commands** ✅

**Status**: Phase 11 Complete - All developer commands implemented

**What Was Added**:

**LJC CLI (Rust)**:
- `tools/ljc/src/commands/status.rs` - Query job scheduling status
- `tools/ljc/src/commands/ps.rs` - List active job executions
- `tools/ljc/src/commands/logs.rs` - Show execution history
- `tools/ljc/src/commands/kill.rs` - Cancel running jobs with wait option
- `tools/ljc/src/main.rs` - Added 4 command variants + match arms
- `tools/ljc/src/commands/mod.rs` - Registered 4 new modules

**Coordinator (Python)**:
- `src/coordinator/developer_api.py` - Added `handle_status_request()`, `handle_ps_request()`, `handle_logs_request()`, `handle_kill_request()` handlers
- `src/coordinator/mqtt_client.py` - Added 4 new handler lists, subscriptions, registration methods
- `src/coordinator/main.py` - Wired 4 new handlers with lambda closures

**Executor (Rust)**:
- `src/executor/src/main.rs` - Added `ACTIVE_JOBS` static HashMap for PID tracking, cancel topic subscription, cancel message handler, PID registration/unregistration in fork section

**Commands**:
```bash
ljc status <job-id>          # Query specific job status
ljc status --all             # Query all jobs
ljc status --all --json      # JSON output
ljc ps                       # List active executions
ljc ps --all                 # Include completed
ljc ps --executor <id>       # Filter by executor
ljc logs <job-id>            # Show execution history
ljc logs <job-id> --last 20  # Last 20 executions
ljc kill <exec-id>           # Send SIGTERM
ljc kill <exec-id> --force   # Send SIGKILL
ljc kill <exec-id> --wait    # Wait for termination
```

**MQTT Topics** (consistent with existing pattern):
```
linearjc/dev/status/request/{coordinator_id}
linearjc/dev/ps/request/{coordinator_id}
linearjc/dev/logs/request/{coordinator_id}
linearjc/dev/kill/request/{coordinator_id}
linearjc/executors/{executor_id}/cancel  # Cancel signal to executor
```

**Test Results**: 72 unit tests + 8 integration tests pass, both binaries compile

---

### **2025-12-03: Phase 12 - Multi-Job Chain Execution** ✅

**Status**: Phase 12 Complete - Multi-job dependency chains now fully execute

**What Was Added**:
- `src/coordinator/job_tracker.py` - `job_index` field, `find_by_tree_execution_id()` method
- `src/coordinator/main.py` - `_execute_chain_job()`, chain continuation in progress handler
- `src/coordinator/job_executor.py` - `prepare_chain_job_inputs()`, `_prepare_intermediate_input()`
- `tests/unit/test_chain_execution.py` - 14 unit tests for chain logic
- `tests/e2e/test_chain.py` - E2E test structure
- `tests/e2e/jobs/chain-step1/`, `tests/e2e/jobs/chain-step2/` - Test chain jobs

**Features**:
- Job dependency chains `a → b → c` execute sequentially
- Intermediate data passed via MinIO (same `tree_execution_id`)
- Chain continuation on job completion in progress handler
- Progress forwarding works for all jobs in chain
- Failed jobs terminate chain and unregister tree

**Architecture**:
- `JobExecution.job_index` tracks position in `tree.jobs[]`
- On completion: `_handle_progress_update()` checks for next job
- Chain jobs use `prepare_chain_job_inputs()` for MinIO intermediate data
- External inputs still fetched from filesystem registry
- Only leaf job outputs written to filesystem

**Test Results**: 87 passed, 8 skipped (49 unit tests including 14 new chain tests)

---

### **2025-12-03: Phase 11 - `ljc tail` Command Implemented** ✅

**Status**: Phase 11 (Developer Runtime Commands) - tail command fully functional

**What Was Added**:
- `tools/ljc/src/progress.rs` - Shared progress streaming module (extracted from exec.rs)
- `tools/ljc/src/commands/tail.rs` - Full implementation (~230 lines)
- `src/coordinator/developer_api.py` - `handle_tail_request()` handler
- `src/coordinator/job_tracker.py` - `find_active_by_job_id()`, `attach_dev_client()` methods
- `src/coordinator/mqtt_client.py` - `register_developer_tail_handler()`, topic routing
- `tests/e2e/test_tail.py` - 5 E2E tests (all passing)

**Features**:
- `ljc tail <job-id>` - Attach to active execution of job
- `ljc tail <execution-id>` - Attach directly by execution ID
- Auto-detects job-id vs execution-id format
- Real-time progress streaming (shared code with exec)
- Colored state display with timestamps
- Auto-exit on terminal state (completed, failed, timeout)

**Architecture**:
- ljc sends signed MQTT request to `linearjc/dev/tail/request/{coordinator_id}`
- Coordinator looks up active execution (by job-id) or direct (by execution-id)
- Attaches `dev_client_id` to existing `JobExecution` for progress forwarding
- Progress forwarding via unified topic `linearjc/dev/progress/{client_id}`

**Refactoring**:
- Extracted `stream_progress()` and `format_state()` to shared `progress.rs` module
- Both `exec --follow` and `tail` now use identical progress streaming code
- Unified progress topic from `linearjc/dev/exec/progress/` to `linearjc/dev/progress/`

**Test Results**: 73 passed, 4 skipped (full suite)

---

### **2025-12-03: Phase 11 - `ljc exec` Command Complete with Progress Forwarding** ✅

**Status**: Phase 11 (Developer Runtime Commands) - exec command fully functional

**What Was Added**:
- `tools/ljc/src/commands/exec.rs` - Full implementation (~300 lines)
- `src/coordinator/developer_api.py` - `handle_exec_request()` (progress forwarding via JobTracker)
- `src/coordinator/job_tracker.py` - Added `dev_client_id` field to `JobExecution` model
- `src/coordinator/mqtt_client.py` - `register_developer_exec_handler()`, new topic routing
- `tests/e2e/test_exec.py` - 6 E2E tests (all passing)

**Features**:
- `ljc exec <job-id>` - Trigger immediate job execution via coordinator
- `--follow` - Stream real-time progress updates ✅
- `--wait` - Block until completion, exit with job exit code ✅
- `--timeout` - Override timeout for waiting

**Architecture**:
- ljc sends signed MQTT request to `linearjc/dev/exec/request/{coordinator_id}`
- Coordinator validates, finds tree, executes immediately (bypass scheduler)
- `dev_client_id` stored in `JobExecution` BEFORE MQTT publish (no race condition)
- Progress forwarding via `_forward_progress_to_dev_client()` in main.py

**Test Results**: 68 passed, 4 skipped (full suite)

---

### **2025-12-04: linearjc-core Consolidation - Unified Job/Package Parsing** ✅

**Status**: Eliminated code duplication between executor and ljc

**Problem Found**: Despite Phase 8 being "complete", significant code was still duplicated:
- `RunSpec` struct defined 3 times (executor, ljc test, ljc extract)
- `JobSpec` struct defined 4+ times with different field types
- Package handling duplicated between executor and ljc
- `manifest.yaml` code existed but SPEC doesn't use it (legacy dead code)

**What Was Added to linearjc-core**:
- `src/job.rs` - Unified `JobFile`, `JobSpec`, `RunSpec`, `LimitsSpec` parsing
- `src/package.rs` - Package handling (`read_package_metadata`, `extract_package_to_workdir`, `compare_versions`)
- `src/execution.rs` - Split into `spawn_isolated()` + `poll_job()` + `cleanup_job()` for executor's non-blocking MQTT loop

**What Was Removed**:
- ~300 lines from executor (duplicate structs, package handling, fork/exec)
- ~100 lines from ljc (duplicate structs in test.rs, extract.rs)
- `PackageManifest` - SPEC doesn't use manifest.yaml, only job.yaml

**Benefits**:
- Single source of truth for job.yaml parsing
- Bugs caught in `ljc test` will now surface before production
- Same code path for executor and ljc ensures consistent behavior
- Clean foundation for Phase 13 output capture

**Test Results**: 137 passed, 8 skipped (145 total)

---

### **2025-12-03: Phase 8 Complete - `ljc test` Command Implemented** ✅

**Status**: Phase 8 (linearjc-core) now complete with `ljc test` command

**What Was Added**:
- `tools/ljc/src/commands/test.rs` - Full implementation (~430 lines)
- Test job `tests/phase7-e2e/jobs/simple-test/` for validation
- Test data convention: `jobs/{job-id}/test/in/{registry_key}/`

**Features**:
- Run jobs locally with `ljc test <job-id>`
- `--no-isolation` for faster iteration (skips isolation setup)
- `--verbose` to show script output
- `--keep` to preserve workdir for debugging
- `--timeout` override from command line
- `--isolation <strict|relaxed|none>` mode override
- `--network <true|false>` override
- Automatic test input copying from `test/in/`
- Output file listing with sizes
- Exit code propagation

**Usage**:
```bash
ljc test backup.pool                    # Run with defaults
ljc test backup.pool --no-isolation -v  # Fast + verbose
ljc test backup.pool --keep             # Debug output files
```

**Test Results**: All ljc tests pass (13 tests)

---

### **2025-12-03: Version Upgrade Complete (Coordinator + Executor)** ✅

**Status**: Both coordinator and executor now use proper semantic version comparison

**What Was Added**:
- `src/coordinator/main.py` - `compare_versions()` utility + version checking in install
- `src/executor/src/main.rs` - `compare_versions()` + fixed Airdrop version comparison
- `tests/e2e/test_deploy.py` - 3 new tests in `TestVersionUpgrade` class

**Coordinator Behavior** (deploy via `ljc deploy`):
- **Upgrade allowed**: v1.0.0 → v2.0.0 ✓
- **Same version rejected**: v1.0.0 → v1.0.0 ✗ "already installed"
- **Downgrade rejected**: v2.0.0 → v1.0.0 ✗ "cannot downgrade"

**Executor Behavior** (Airdrop distribution):
- **Upgrade**: Downloads and installs new version
- **Same/older version**: Skips ("already installed")
- **Bug fixed**: Was using string comparison (broken for "1.10.0" vs "1.9.0")

**Test Results**: 62 passed, 4 skipped (full suite)

---

### **2025-12-03: ljc sync Command Complete** ✅

**Status**: Registry sync from coordinator now works (6 E2E tests passing)

**What Was Added**:
- `tools/ljc/src/commands/sync.rs` - Full sync implementation (~280 lines)
- `src/coordinator/developer_api.py` - Registry sync handler
- `src/coordinator/mqtt_client.py` - Topic subscription and routing
- `tests/e2e/test_sync.py` - 6 E2E tests

**Features**:
- Fetch registry from coordinator via MQTT
- Show diff: added/removed/changed entries
- Detect "up to date" state
- Error handling for missing secret, not in repo, connection timeout

**Usage**:
```bash
export LINEARJC_SECRET="<set-a-strong-shared-secret>"
ljc sync --from coordinator-host
```

**Test Results**: 59 passed, 4 skipped (full suite)

---

### **2025-12-03: ljc deploy E2E Tests Added** ✅

**Status**: Full deployment workflow now tested locally (6 new tests)

**What Was Added**:
- `tests/e2e/test_deploy.py` - 6 tests covering full `ljc deploy` workflow
- New fixtures in `tests/e2e/conftest.py`:
  - `ljc_binary` - Finds ljc binary (release or debug)
  - `ljc_env` - Configures env vars for ljc (MQTT_BROKER, MQTT_PORT, etc.)
  - `deploy_test_package` - Creates test .ljc package dynamically

**Tests Added**:
1. `test_ljc_binary_exists` - Verifies ljc binary available
2. `test_deploy_package_created` - Verifies package creation
3. `test_ljc_deploy_to_coordinator` - Full deploy workflow via MQTT+MinIO
4. `test_deployed_job_executes` - Verifies deployed job runs successfully
5. `test_deploy_nonexistent_package` - Error handling
6. `test_deploy_without_secret` - Auth error handling

**Key Fixes**:
- Pre-registered deploy test registry entries (per SPEC.md design)
- Added `CAPABILITIES=pool` to executor for on-demand distribution
- Added `MINIO_ENDPOINT` to executor for package download

**Test Results**: 53 passed, 4 skipped (root-only tests)

---

### **2025-12-03: Phase 7 E2E Tests PASSING - All Issues Resolved** ✅

**Status**: Phase 7 Complete - E2E smoke test passes (4/4 tests)

**Issues Fixed This Session**:

1. **Executor Package Handling** (Session 2 - prior session)
   - Changed from caching bare `.sh` scripts to full `.ljc` packages
   - Added `serde_yaml` for reading job.yaml from packages
   - Executor now extracts packages to work_dir before execution
   - Added `LJC_IN`, `LJC_OUT`, `LJC_TMP`, `LJC_JOB_ID`, `LJC_EXECUTION_ID` env vars

2. **Privilege Handling for Non-Root Testing** (Session 3 - this session)
   - Added `getuid()` check to skip `chown`/`setuid` when already running as target user
   - Enables E2E tests to run without root privileges
   - File: `src/executor/src/main.rs`

3. **SPEC.md v0.5.0 Compatibility** (Session 3 - this session)
   - Coordinator `archive_handler.py` now accepts `kind: dir` (short form) in addition to `path_type: directory`
   - Test fixtures dynamically set job user to current user via `getpass.getuser()`
   - File: `src/coordinator/archive_handler.py`, `tests/e2e/conftest.py`

**Test Results**:
```
tests/e2e/test_smoke.py::TestSmoke::test_coordinator_starts PASSED
tests/e2e/test_smoke.py::TestSmoke::test_executor_starts PASSED
tests/e2e/test_smoke.py::TestSmoke::test_job_execution_flow PASSED
tests/e2e/test_smoke.py::TestSmoke::test_output_integrity PASSED
============================== 4 passed in 13.46s ==============================
```

**What E2E Test Verifies**:
- ✅ Coordinator ↔ Executor MQTT communication
- ✅ Job scheduling and capability discovery
- ✅ Input download from MinIO
- ✅ Package extraction and script execution
- ✅ Output upload to MinIO
- ✅ Output collection to filesystem

**E2E Test Coverage** (all passing):
- ✅ `ljc deploy` command (6 tests in `test_deploy.py`)
- ✅ `ljc exec/tail` commands (11 tests)
- ✅ `ljc status/ps/logs/kill` commands (27 tests)
- ✅ On-demand job distribution via Airdrop
- ✅ Full job execution flow (coordinator → executor → output)

**To run tests**:
```bash
pytest tests/ -v  # Full suite (~65s)
```

---

### **2025-11-23: Phase 7 Smoke Test Complete + 2 Critical Bugs Fixed** ✅

**Progress**: 85% complete (28h / 33h) - Smoke test passed, comprehensive testing remains

**Critical Bugs Fixed During Smoke Test:**
1. **Executor Output Archiving** - Always treated outputs as directories, failed for file outputs
   - Added `create_tar_gz_from_file()` for single files
   - Check `path_type` field to determine archiving method
   - File: `src/executor/src/main.rs`
2. **Coordinator Missing path_type** - Wasn't sending path_type in job requests
   - Added `path_type: registry_entry.path_type` to all 3 input/output prep functions
   - File: `src/coordinator/job_executor.py`

**Smoke Test Result** (2025-11-23 17:20 UTC): ✅ **PASSED**
- hello.pool job executed end-to-end successfully
- Job progression: assigned → downloading → ready → running → uploading → completed
- Phase 7 features verified: new directory layout, file archiving, process isolation

### **2025-11-23: Phase 7 Tasks 1-7 Complete - Coordinator Updated** ✅

**Initial Progress**: 82% complete (27h / 33h)

**Task 1: Standardized Directory Layout** ✅
- ✅ Changed from `inputs/outputs` to `in/out/tmp/.work`
- ✅ Added `path_type` field to `DataSpec` (file vs directory)
- ✅ Smart extraction: files go directly to `in/{label}`, directories to `in/{label}/`
- ✅ Metadata generation: `in/.meta/registry.json` for job introspection
- ✅ New env vars: `LINEARJC_IN_DIR`, `LINEARJC_OUT_DIR`, `LINEARJC_TMP_DIR`
- ✅ Backward compat: Old env vars still set (deprecated)
- ✅ Code compiled successfully with zero errors

**Task 2: Landlock Filesystem Isolation** ✅
- ✅ Created `src/executor/src/isolation.rs` module (350+ lines)
- ✅ Three isolation modes: `strict`, `relaxed`, `none`
- ✅ Kernel-level filesystem restrictions (Linux 5.13+)
- ✅ Read-only: `/in`, system libraries (`/lib*`, `/usr/lib*`, `/bin`, `/usr/bin`)
- ✅ Read-write: `/out`, `/tmp`
- ✅ Extra read paths support for relaxed mode
- ✅ Clean error handling with `anyhow::Context`
- ✅ Added dependency: `landlock = "0.4.0"`
- ✅ Code compiles successfully

**Task 3: cgroups v2 Resource Limits** ✅
- ✅ CPU limits (CFS quota/period mechanism, percentage-based)
- ✅ Memory limits (byte limit with OOM killer)
- ✅ Process limits (prevents fork bombs)
- ✅ I/O limits (placeholder with warning)
- ✅ Graceful fallback when controllers not available
- ✅ Added dependency: `cgroups-rs = "0.3.4"`
- ✅ Code compiles successfully

**Task 4: Network Namespace Isolation** ✅
- ✅ Implemented `isolate_network()` function
- ✅ Uses `unshare(CLONE_NEWNET)` for network isolation
- ✅ Simple boolean flag: `true` = host network, `false` = isolated
- ✅ Added `sched` feature to `nix` crate
- ✅ Code compiles successfully

**Production Executor (deploy only after local tests pass)**:
- **executor-host** (192.0.2.20) - Production executor
- Kernel 6.12.37 with Landlock, cgroups v2, network namespaces
- Executor configured with Phase 7 directory layout

**Task 5: Integration into Job Execution** ✅
- ✅ Added `IsolationSpec` and `ResourceLimitsSpec` structs with defaults
- ✅ Default: `filesystem: none` (opt-in), `network: true`
- ✅ Implemented `parse_isolation_config()` helper function
- ✅ Apply cgroup limits BEFORE fork (parent process)
- ✅ Child process: Network isolation → Landlock → setuid → exec
- ✅ Parent process: Cleanup cgroup after job completion
- ✅ Comprehensive logging and error handling
- ✅ Code compiles successfully with zero errors

**Task 6: ljc Extract Command** ✅
- ✅ Created `extract.rs` command (350+ lines)
- ✅ Extracts .ljc to executor-like directory structure
- ✅ Creates `work/test-execution-id/in/out/tmp/`
- ✅ Generates placeholder inputs based on registry
- ✅ Handles file vs directory path types
- ✅ Creates `run.sh` helper for local testing
- ✅ Added tempfile dependency
- ✅ Registered in CLI (between bump and sync)
- ✅ Code compiles successfully

**Task 7: Coordinator Config Parsing** ✅
- ✅ Added `ResourceLimitsSpec` model to coordinator
- ✅ Added `IsolationSpec` model with filesystem mode validation
- ✅ Updated `JobExecutor` model to include optional isolation field
- ✅ Updated `build_job_request()` to serialize isolation config for executor
- ✅ Coordinator now passes isolation config via MQTT to executor
- ✅ Python code compiles without errors

**Files Modified:**
- `src/executor/src/main.rs` - Added isolation structs, integration logic
- `src/executor/src/isolation.rs` - Complete isolation implementation (Tasks 2-4)
- `src/executor/Cargo.toml` - Added landlock, cgroups-rs dependencies
- `tools/ljc/src/commands/extract.rs` - NEW - Extract command
- `tools/ljc/src/commands/mod.rs` - Registered extract module
- `tools/ljc/src/main.rs` - Added Extract CLI command
- `tools/ljc/Cargo.toml` - Added tempfile dependency
- `src/coordinator/models.py` - Added IsolationSpec, ResourceLimitsSpec models
- `src/coordinator/job_executor.py` - Updated build_job_request() to pass isolation
- `docs/linearjc-phase7-process-isolation.md` - Updated with implementation notes

**Next:** Task 9 - Testing (unit + integration + e2e) (4h)

### **2025-11-18: Security Hardening - Archive Extraction**

**Critical vulnerabilities fixed in tar extraction:**

1. **Path Traversal Prevention** (CVE-2007-4559)
   - All tar extraction points now validate paths before extraction
   - Blocks `../` sequences and absolute paths
   - Files: `archive_handler.py:49-74`, `main.py:439-443,879-889`

2. **Symlink Attack Prevention**
   - All symlinks blocked in tar archives
   - Prevents directory escape and data exfiltration attacks
   - Files: `archive_handler.py:49-54`

3. **TOCTOU Race Prevention**
   - Member-by-member extraction with inline validation
   - No gap between validation and extraction
   - Files: `archive_handler.py:159,186`

**Impact**: Prevents arbitrary file write, data exfiltration, and privilege escalation via malicious packages.

### **Clock Skew Fix - Complete**

4. **ljc Client Clock Skew Tolerance** - Bidirectional tolerance complete
   - Added 30s tolerance to ljc client message verification
   - Matches coordinator's existing tolerance
   - Prevents rejection when coordinator clock is ahead
   - Files: `tools/ljc/src/message_signing.rs:18-20,115-121`

### **Configuration Fixes**

5. **Coordinator ID Defaults** - Fixed mismatch
   - Changed default from `linearjc-coordinator-01` to `linearjc-coordinator`
   - Matches coordinator's hardcoded ID
   - Files: `tools/ljc/src/config.rs:83`, `.ljcconfig.example:33`

### **New Features**

6. **Version Bump Command** - Semantic versioning made easy
   - `ljc bump <major|minor|patch> <job-id> [--dry-run]`
   - Atomically updates job.yaml and manifest.yaml
   - Validates version format and detects mismatches
   - Defaults to 0.1.0 if version missing
   - Files: `tools/ljc/src/commands/bump.rs` (new, 280 lines)

7. **Shebang Validation** - Catch runtime errors at build time
   - Validates script.sh has proper shebang (`#!/...`)
   - Prevents executor rejection at runtime
   - Added to Level 1 validation (errors)
   - Files: `tools/ljc/src/commands/validate.rs:129-135,320-361`

---

## Production Deployment Session (2025-12-04)

**First real production job deployed: `build.musl-toolchain`**

Validates the complete deployment workflow from local development to production execution.

### New Commands Implemented

1. **`ljc registry add`** - Add registry entries locally
   - `--type fs|minio`, `--path`, `--kind file|dir`
   - Files: `tools/ljc/src/commands/registry.rs:88-159`

2. **`ljc registry push`** - Push local registry to coordinator
   - Sends via MQTT, coordinator merges and saves
   - Files: `tools/ljc/src/commands/registry.rs:161-319`

3. **Coordinator `registry_push` handler** - Receive and save pushed registries
   - Files: `src/coordinator/developer_api.py:387-480`
   - Files: `src/coordinator/main.py:197-222` (save_data_registry)

### Bug Fixes

1. **`ljc bump` manifest.yaml requirement removed**
   - Was requiring non-existent `manifest.yaml` file
   - SPEC only defines `job.yaml` for versioning
   - Files: `tools/ljc/src/commands/bump.rs:71-170`

2. **Auto-reload confirmed working**
   - Coordinator sets `_reload_requested = True` after install
   - Scheduler loop checks flag and calls `reload_jobs()`
   - No manual restart needed for job deployments

### Lessons Learned

- `min_daily: 0` causes validation error - use `min_daily: 1` and `ljc exec` for on-demand
- Coordinator auto-reloads within ~5 seconds after deploy
- Executor receives jobs via on-demand distribution (Airdrop) - no pre-deployment needed
- Long-running builds need appropriate timeout (36000s = 10h for GCC build)
- Use HTTPS for public git repos (SSH requires host key setup on executor)

### Known Issues

**⚠️ Stale Output Conflict After Failed Executions**

When a job fails, the tree validator may retain registered output paths, causing "Output conflict detected" errors on retry:

```
Error: Output conflict detected for tree 'my-job':
  • Path: /var/lib/linearjc/work/outputs/my_output
    Conflict with: tree 'my-job' (currently active)
```

**Current Workaround**: Restart coordinator to clear stale state
```bash
docker restart linearjc-coordinator
```

**Root Cause**: `tree_validation.py` registers outputs at execution start but may not properly unregister on all failure paths.

**TODO**: Fix coordinator to be failsafe - must clean up registered outputs on:
- Job script failure (exit code != 0)
- Timeout
- Executor disconnect
- Any exception during execution

The coordinator must NEVER require manual restart to recover from job failures.

---

## Previous Fixes (2025-11-17)

**Three critical bugs fixed in deployment workflow:**

1. **Clock Skew Tolerance (Coordinator)** - Added 30-second tolerance for timestamp validation
   - `message_signing.py`: Allow messages up to 30s in the future
   - Files: `submodules/linearjc/src/coordinator/message_signing.py:18,130-133`

2. **Double-Signing Bug** - Removed redundant message signing in developer API
   - `developer_api.py`: `publish_message()` already signs, was signing twice
   - Files: `submodules/linearjc/src/coordinator/developer_api.py:258-266`

3. **MQTT Race Condition** - Subscribe before sending request
   - `deploy.rs`: Coordinator responds in <2ms, faster than client could subscribe
   - Files: `submodules/linearjc/tools/ljc/src/commands/deploy.rs:122-124`

**Validation Status**: Deployment workflow tested end-to-end ✅

---

## Executor/Coordinator Roadmap

### Quick Status

| Phase | Feature | Status | Effort | Priority |
|-------|---------|--------|--------|----------|
| **Phase 1** | .ljc package format & install | ✅ Complete | 8h | P0 |
| **Phase 2** | SIGHUP hot reload | ✅ Complete | 4h | P0 |
| **Phase 3** | Airdrop (on-demand distribution) | ✅ Complete | 12h | P0 |
| **Phase 4** | Smart executor selection | 📋 Planned | 8h | P2 |
| **Phase 5** | Cache management (LRU) | 📋 Planned | 4h | P2 |
| **Phase 6** | ~~Landlock isolation~~ | ⚠️ Superseded by Phase 7 | - | - |
| **Phase 7** | **Process Isolation & Standardized Layout** | ✅ **Complete** | **33h** | **Done** |
| **Phase 8** | **linearjc-core Shared Crate** | ✅ **Complete** | **16h** | **Done** |
| **Phase 13** | **Job Output Visibility** | 🚧 **In Progress** | **9h** | **P0** |

**Total Completed**: 73h (Phases 1-3, 7-8) | **In Progress**: Phase 13 (~9h) | **Future**: Phases 4-5 (~12h)

---

### Phase 13: Job Output Visibility 🚧 **IN PROGRESS**

**Goal**: Make job stdout/stderr visible through developer tooling without SSH access to executor.

**Status**: 🚧 In Progress - SPEC updated, implementation pending

**Priority**: P0 - Critical for developer experience

**Effort**: 9 hours

#### Problem Statement

When a job fails, the current tooling shows:
```
$ ljc logs build.musl-toolchain

TIMESTAMP                          AGE
────────────────────────────────────────
2025-12-04 19:18:36               0.1h
```

This tells you WHEN it failed, not WHY. To debug, you must SSH to the executor and grep logs manually.

#### Design Principle

**Job output is part of the execution result, not a separate logging concern.**

Just like `exit_code` and `duration_ms`, the output belongs in the completion message.

#### Solution Overview

**Two capture modes:**

| Mode | When | Behavior |
|------|------|----------|
| **Buffered** | No active followers | Capture to buffer, send tail on completion |
| **Streaming** | `ljc tail` / `ljc exec --follow` active | Send chunks every 3s during execution |

The executor only streams when someone is listening (coordinator notifies on attach).

#### Progress Message Changes

**During execution (streaming mode):**
```json
{
  "execution_id": "build.musl-toolchain-20251204-191836",
  "state": "running",
  "elapsed_ms": 125000,
  "output": {
    "chunk": "  CC ldmain.o\n  CC ldwrite.o\n",
    "total_lines": 2341
  }
}
```

**On completion:**
```json
{
  "execution_id": "build.musl-toolchain-20251204-191836",
  "state": "failed",
  "exit_code": 1,
  "duration_ms": 212952,
  "output": {
    "tail": "...\n/bin/sh: rsync: not found\nmake: *** Error 127\n",
    "tail_lines": 100,
    "total_lines": 5432
  }
}
```

#### Expected Result

```
$ ljc logs build.musl-toolchain

═══════════════════════════════════════════════════════════════
  build.musl-toolchain - Execution History
═══════════════════════════════════════════════════════════════

[1] 2025-12-04 19:22:19 UTC - FAILED (exit 1) - 3m 32s

    ... (5332 lines before) ...
    /bin/sh: rsync: not found
    make[4]: *** [Makefile:1361: headers_install] Error 127
    make: *** [Makefile:182: all] Error 2
```

#### Implementation Tasks

| # | Task | Component | Effort | Status |
|---|------|-----------|--------|--------|
| 1 | Capture stdout/stderr via pipe (not file redirect) | Executor | 2h | 📋 Pending |
| 2 | Implement chunking (time/lines/size triggers) | Executor | 1h | 📋 Pending |
| 3 | Add streaming mode toggle (on follower attach) | Executor | 1h | 📋 Pending |
| 4 | Include output.tail in completion message | Executor | 0.5h | 📋 Pending |
| 5 | Forward output chunks to attached clients | Coordinator | 1h | 📋 Pending |
| 6 | Store output.tail in execution history | Coordinator | 1h | 📋 Pending |
| 7 | Display streaming chunks in ljc tail/exec | ljc | 1h | 📋 Pending |
| 8 | Display stored tails in ljc logs | ljc | 1h | 📋 Pending |
| 9 | E2E tests for output visibility | Tests | 0.5h | 📋 Pending |
| **TOTAL** | | | **9h** | |

#### Configuration

```yaml
executor:
  output:
    tail_lines: 100           # Lines in completion message
    chunk_interval_ms: 3000   # Max time between chunks
    chunk_max_lines: 50       # Max lines per chunk
    chunk_max_bytes: 32768    # Max bytes per chunk
```

#### Development & Testing

**IMPORTANT: All code changes MUST be tested locally before production deployment.**

```bash
cd submodules/linearjc

# 1. Run full test suite (spawns local MQTT, MinIO, Coordinator, Executor)
pytest tests/ -v

# 2. Run only E2E tests (faster iteration)
pytest tests/e2e/ -v -s

# 3. Run specific test file
pytest tests/e2e/test_logs.py -v -s
```

**Local test infrastructure** (auto-spawned by pytest fixtures):
- Mosquitto MQTT broker (random port)
- MinIO S3-compatible storage (test credentials)
- Coordinator (Python subprocess)
- Executor (Rust binary subprocess)

**Recommended development loop:**
1. Make code changes (executor/coordinator/ljc)
2. Run `pytest tests/e2e/ -v` - verify changes work
3. Run `pytest tests/ -v` - verify nothing broke
4. Only then deploy to production (see `docs/deployment-workflow.md`)

**For Phase 13 specifically:**
- Existing test jobs in `tests/e2e/jobs/` produce stdout
- Add test that verifies `output.tail` appears in completion message
- Add test that verifies `ljc logs` displays the tail

#### SPEC Reference

See `SPEC.md` → **Part 3: Executor → Output Capture** for full specification.

### Phase 7: Process Isolation & Standardized Layout ✅ **COMPLETE**

**Goal**: Replace Docker overhead with lightweight kernel-level process isolation

**Status**: ✅ Complete - E2E smoke tests passing (4/4), package handling fixed, SPEC.md v0.5.0 compliant

**Priority**: P1 - HIGHEST (breaking change, highest value)

**Effort**: 33 hours

#### Summary

Replace Docker-based job isolation with native Linux kernel features:
- **Landlock** (filesystem isolation)
- **cgroups v2** (resource limits)
- **Network namespaces** (network control)

Standardize working directory to `/in` and `/out` for predictable access patterns.

#### Key Changes

**1. Directory Layout** (Breaking Change):
```
Before (Phase 3):
  /var/lib/linearjc/work/{id}/inputs/{archive_name}/file
  /var/lib/linearjc/work/{id}/outputs/{archive_name}/file

After (Phase 7):
  /var/lib/linearjc/work/{id}/in/{dataset_label}     # Direct access by label!
  /var/lib/linearjc/work/{id}/out/{dataset_label}
  /var/lib/linearjc/work/{id}/tmp/
```

**2. Job Configuration** (in job.yaml):
```yaml
executor:
  user: nobody
  timeout: 3600

  # NEW: Isolation configuration (OPTIONAL - developer controls!)
  isolation:
    filesystem: strict       # strict | relaxed | none
    network: false           # true | false
    limits:                  # OPTIONAL resource limits
      cpu_percent: 50        # % of one core
      memory_mb: 512
      processes: 10
    extra_read_paths:        # For relaxed mode
      - /usr/local/bin/tool  # Legitimate tools
```

**3. Isolation Modes**:
- `filesystem: strict` - Landlock enforced, only /in (RO), /out (RW), /tmp (RW), system libs
- `filesystem: relaxed` - Same + extra_read_paths from job config
- `filesystem: none` - No landlock (for jobs needing filesystem flexibility)
- `network: true/false` - Network namespace isolation

**4. Script Access Patterns**:
```bash
# Before (complex, nested):
cat "$LINEARJC_INPUT_DIR/sensor_config/config.json"

# After (clean, direct):
cat /in/sensor_config  # Direct file access by dataset label!
echo "result" > /out/daily_report/report.csv

# Violations fail at kernel level:
cat /etc/passwd     # EPERM (if filesystem: strict)
curl evil.com       # Network unreachable (if network: false)
```

**5. Developer Testing** (NEW: `ljc extract` command):
```bash
ljc extract dist/backup.pool.ljc
cd .extract/work/test-execution-id
./run.sh  # Test exact executor directory structure locally!
```

#### Benefits vs Docker

| Feature | Docker | Landlock+cgroups |
|---------|--------|------------------|
| Startup overhead | ~1-2 seconds | <10ms |
| Memory overhead | ~50-100MB | ~0MB |
| Complexity | Container runtime | Native kernel |
| Security | Good | Excellent (minimal attack surface) |

#### Implementation Tasks

| # | Task | Effort | File | Priority | Status |
|---|------|--------|------|----------|--------|
| 1 | Standardized directory layout | 4h | executor/main.rs | P0 | ✅ Complete (4h) |
| 2 | Landlock filesystem isolation | 6h | executor/isolation.rs (NEW) | P0 | ✅ Complete (2h) |
| 3 | cgroups v2 resource limits | 4h | executor/isolation.rs | P0 | ✅ Complete (1h) |
| 4 | Network namespace isolation | 3h | executor/isolation.rs | P0 | ✅ Complete (0.5h) |
| 5 | Integrate isolation into execute_job | 4h | executor/main.rs | P0 | ✅ Complete (4.5h) |
| 6 | ljc extract command | 3h | ljc/commands/extract.rs (NEW) | P1 | ✅ Complete (3h) |
| 7 | Coordinator config parsing | 2h | coordinator/models.py, job_executor.py | P1 | ✅ Complete (2h) |
| 8 | Documentation & migration guide | 3h | docs/* | P1 | ✅ Complete (3h) |
| 9a | Unit tests (isolation, directory layout) | 2.5h | executor/src/*.rs | P0 | ✅ Complete (2.5h) |
| 9b | E2E test jobs (strict/relaxed/none) | 1h | tests/phase7-e2e/ | P0 | ✅ Complete (1h) |
| 9c | E2E deployment & verification | 1.5h | MQTT + executor | P0 | ✅ Complete (1.5h) |
| **TOTAL** | | **33h** | | | **✅ COMPLETE** |

#### Dependencies

**Rust Crates**:
```toml
[dependencies]
landlock = "0.4"       # Filesystem isolation (Linux 5.13+)
cgroups-rs = "0.3"     # Resource limits (cgroups v2)
nix = "0.28"           # Network namespaces (already exists)
```

**System Requirements**:
- Linux 5.13+ (landlock support)
- cgroups v2 enabled
- **NO privileged setup needed** (unprivileged mode)

#### Migration Plan

**Stage 1**: Opt-in (v0.4.0-alpha)
- Add isolation support, default `filesystem: none`
- Existing jobs work unchanged
- New jobs can opt-in

**Stage 2**: Opt-out (v0.4.0-beta)
- Change default to `filesystem: strict`
- Jobs must explicitly set `none` to disable

**Stage 3**: Mandatory (v0.5.0)
- Remove `filesystem: none` option
- All jobs use isolation (breaking release)

#### Design Decisions

**1. Static binaries only** - No dynamic linking concerns
   - Jobs bundle everything in bin/ directory
   - System libs allowed read-only (/lib, /usr/lib)

**2. Developer controls isolation** - Per-job config, not global
   - Job author decides trust level
   - Explicit `filesystem: none` shows intent

**3. ljc extract for local testing** - Same structure as executor
   - Extract package to .extract/work/test-execution-id/
   - Includes run.sh helper script
   - Developers test exact environment

**4. /in and /out are absolute paths** - Simple, predictable
   - No symlinks or mount complexity
   - Jobs just use /in/label and /out/label

#### Success Criteria

**Functional**:
- ✅ Jobs read inputs from /in/{label}
- ✅ Jobs write outputs to /out/{label}
- ✅ Landlock prevents unauthorized filesystem access
- ✅ cgroups enforce resource limits
- ✅ Network isolation works (when network: false)
- ✅ ljc extract creates correct structure

**Performance**:
- ✅ Isolation overhead < 10ms
- ✅ No memory overhead vs current

**Developer Experience**:
- ✅ ljc extract allows local testing
- ✅ Clear error messages for violations
- ✅ Migration guide complete

**Security**:
- ✅ Jobs cannot escape filesystem isolation
- ✅ Jobs cannot exceed resource limits
- ✅ Jobs cannot access network when disabled

#### Detailed Specification

See companion document: `docs/linearjc-phase7-process-isolation.md` (created 2025-11-18)
- Complete implementation guide
- All code snippets
- Testing plan
- Migration guide

#### Open Questions

**Q1**: Symlinks vs absolute paths for /in and /out?
**A**: Absolute paths in work dir (no mount complexity)

**Q2**: Default isolation mode?
**A**: Start with `none` (v0.4.0-alpha), move to `strict` (v0.5.0)

**Q3**: Backward compat for old scripts?
**A**: Keep `$LINEARJC_INPUT_DIR` env vars for 1-2 releases

#### Next Steps

1. ✅ Design complete (this document + detailed spec)
2. ✅ Review and approve breaking changes
3. ✅ Implement all tasks
4. ✅ Complete

---

### Phase 8: linearjc-core Shared Crate ✅ **COMPLETE**

**Goal**: Extract security-critical code into a shared Rust crate used by both executor and ljc

**Status**: ✅ Complete (2025-12-03)

**Priority**: P1 - Enables `ljc test` command for local isolated testing

**Effort**: 16 hours

#### Motivation

Currently, security-critical code is:
- **Duplicated**: Message signing in executor, ljc, and coordinator (3 implementations)
- **Scattered**: Isolation code only in executor, not available to ljc
- **Untestable locally**: `ljc extract` cannot apply real isolation

The SPEC.md v0.5.0 design calls for `linearjc-core` as a shared crate.

#### Design Principle

**linearjc-core is shared ONLY between Rust components:**
- ✅ **Executor** (Rust) - uses linearjc-core
- ✅ **ljc tool** (Rust) - uses linearjc-core
- ❌ **Coordinator** (Python) - remains independent (no language mixing)

The coordinator keeps its own Python implementations. This avoids FFI complexity and keeps each component self-contained.

#### Components to Extract

| Module | Current Location | Description |
|--------|-----------------|-------------|
| `isolation.rs` | `executor/src/isolation.rs` | Landlock, cgroups v2, network namespaces |
| `workdir.rs` | Embedded in `executor/src/main.rs` | Work directory setup (in/out/tmp) |
| `execution.rs` | Embedded in `executor/src/main.rs` | Fork, setuid, exec job process |
| `archive.rs` | Both executor and ljc | Tar/gzip with security checks |
| `signing.rs` | `ljc/src/message_signing.rs` | HMAC-SHA256 signing/verification |

#### Directory Structure

```
src/
├── linearjc-core/           # NEW: Shared crate
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs           # Public API
│       ├── isolation.rs     # Landlock, cgroups, netns
│       ├── workdir.rs       # Work directory setup
│       ├── execution.rs     # Process execution
│       ├── archive.rs       # Secure tar handling
│       └── signing.rs       # Message signing
│
├── executor/                # Thin wrapper
│   ├── Cargo.toml           # depends on linearjc-core
│   └── src/
│       └── main.rs          # MQTT client + I/O only
│
tools/
└── ljc/                     # Developer CLI
    ├── Cargo.toml           # depends on linearjc-core
    └── src/
        └── commands/
            └── test.rs      # NEW: Uses linearjc-core for isolated execution
```

#### Implementation Tasks

| # | Task | Effort | Status |
|---|------|--------|--------|
| 1 | Create linearjc-core crate skeleton | 1h | ✅ Complete |
| 2 | Move isolation.rs to core | 2h | ✅ Complete |
| 3 | Extract workdir setup to core | 2h | ✅ Complete |
| 4 | Extract execution logic to core | 3h | ✅ Complete |
| 5 | Unify archive handling in core | 2h | ✅ Complete |
| 6 | Move message signing to core | 1h | ✅ Complete |
| 7 | Update executor to use core | 2h | ✅ Complete |
| 8 | Update ljc to use core | 1h | ✅ Complete |
| 9 | Implement `ljc test` command | 2h | ✅ Complete |
| **TOTAL** | | **16h** | **✅ COMPLETE** |

#### `ljc test` Command ✅ IMPLEMENTED

```bash
# Run job locally with job.yaml defaults
ljc test backup.pool

# Skip isolation (faster, no root required)
ljc test backup.pool --no-isolation

# Override isolation mode (strict, relaxed, none)
ljc test backup.pool --isolation strict

# Show script stdout/stderr
ljc test backup.pool --verbose

# Keep workdir after execution (for debugging)
ljc test backup.pool --keep

# Override timeout (seconds)
ljc test backup.pool --timeout 60

# Override network access
ljc test backup.pool --network false
```

**How it works**:
1. Read job configuration from `job.yaml`
2. Create temporary work directory with `in/out/tmp` structure
3. Copy test inputs from `{job}/test/in/{registry_key}/` if available
4. Execute job using `linearjc-core::execution::execute_simple`
5. Report results with duration, exit code, and output files
6. Clean up (or keep with `--keep` flag)

**Test Data Convention**:
- Place test inputs in `jobs/{job-id}/test/in/{registry_key}/`
- Directory structure mirrors executor's input layout
- If no test data exists, placeholders are created

#### Benefits

| Before | After |
|--------|-------|
| `ljc extract` + manual `./run.sh` | `ljc test` with real isolation |
| Different code paths in executor vs ljc | Identical execution via shared crate |
| Message signing duplicated 3x | Single implementation in core |
| Can't test isolation locally | Full isolation testing on dev machine |

#### Success Criteria

- [x] linearjc-core crate compiles standalone (27 tests pass)
- [x] Executor uses linearjc-core (7 tests pass)
- [x] ljc uses linearjc-core for signing (13 tests pass)
- [x] All existing tests pass (47 total)
- [x] `ljc test` command implemented with full feature set

---

## ljc Tool Roadmap

### Quick Status

| Phase | Feature | Status | Time |
|-------|---------|--------|------|
| **Phase 1** | Core Infrastructure | ✅ Complete | 3h |
| **Phase 2** | Repository Management | ✅ Complete | 2h |
| **Phase 3** | Job Creation (Scaffolding) | ✅ Complete | 1h |
| **Phase 4** | Validation | ✅ Complete | 2h |
| **Phase 5** | Package Building | ✅ Complete | 1h |
| **Phase 6** | MQTT Authentication & Message Signing | ✅ Complete | 2h |
| **Phase 7** | MQTT Deployment (Client) | ✅ Complete | 2h |
| **Phase 8** | Registry Sync (MQTT) | ✅ Complete | 2h |
| **Phase 9** | Coordinator Developer API | ✅ Complete | 3h |
| **Phase 10** | Coordinator Version Upgrade | ✅ Complete | 1h |
| **Phase 11** | Developer Runtime Commands | ✅ Complete | 13h |

**Total Completed**: 32 hours

---

## Working Commands (v0.2.0-alpha)

```bash
ljc init <path>                        # ✅ Create repository
ljc new <job-id>                       # ✅ Scaffold new job
ljc list                               # ✅ List jobs
ljc info <job-id>                      # ✅ Show job details
ljc registry [--verbose]               # ✅ Show registry map
ljc validate [job|--all]               # ✅ Validate jobs (includes shebang check)
ljc bump <major|minor|patch> <job-id>  # ✅ Bump semantic version
ljc build <job-id>                     # ✅ Build .ljc package
ljc extract <package>                  # ✅ Extract package for manual testing
ljc test <job-id> [--no-isolation]     # ✅ Test job locally
ljc deploy <package> --to <host>       # ✅ Deploy to coordinator (MQTT + MinIO)
ljc sync --from <host>                 # ✅ Sync registry from coordinator (MQTT)
ljc exec <job-id> [--follow] [--wait]  # ✅ Execute job immediately (all modes working)
ljc tail <job-id|exec-id>              # ✅ Follow job execution progress
ljc status [job-id] [--all] [--json]   # ✅ Query job scheduling status
ljc ps [--all] [--executor <id>]       # ✅ List active job executions
ljc logs <job-id> [--last N]           # ✅ Show execution history
ljc kill <exec-id> [--force] [--wait]  # ✅ Cancel running job
```

---

## Overview

`ljc` is a command-line tool for developing, validating, and deploying LinearJC jobs. It manages job structure, registry allocation, package building, and deployment workflows.

**Design Principles:**
- Stateless tool - everything derived from filesystem + remote queries
- Simple validation - fail fast with clear errors
- Consistent format - compact one-line YAML everywhere
- VCS agnostic - users choose git/other
- Version-based upgrades - coordinator allows replacing jobs with higher versions
- MQTT-based deployment - secure, automated, consistent with airdrop pattern
- Signed messages - HMAC-SHA256 authentication like executor/coordinator

---

## Core Concepts

### Registry Map
Pure registry data in compact format (mirrors coordinator's data_registry.yaml):
```yaml
registry:
  hello_input: {type: filesystem, path_type: file, path: /var/lib/linearjc/work/hello_input.txt, readable: true, writable: true}
  hello_output: {type: filesystem, path_type: file, path: /var/lib/linearjc/work/hello_output.txt, readable: true, writable: true}
```

**No job associations** - validation logic scans jobs to determine:
- Output conflicts (multiple jobs writing to same key)
- Orphaned entries (registry keys not used by any job)

### Job Structure
```
jobs/backup.pool/
├── job.yaml                       # Job definition
├── script.sh                      # Executable script
├── manifest.yaml                  # Package metadata
├── JOB.md                         # Documentation
├── bin/                           # Optional bundled binaries
└── test/                          # Test inputs (excluded from package)
```

**Note**: Registry entries are managed centrally via `ljc registry add` and pushed
via `ljc registry push` (see SPEC.md). Packages do NOT contain registry definitions.

### Package Format
`.ljc` file = tar.gz containing job files for deployment to coordinator.

### MQTT Architecture

**Secure Communication Pattern:**
```
Developer (ljc)          MQTT Broker          Coordinator          MinIO
      |                       |                     |                 |
      |--[deploy-req]-------->|--[forward]--------->|                 |
      |    (SIGNED)           |                     | verify          |
      |                       |<--[upload-url]------|                 |
      |<--[upload-url]--------|    (SIGNED)         | (presigned)     |
      |                       |                     |                 |
      |--[HTTP PUT]-----------|---------------------|---------------->|
      |                       |                     |                 |
      |--[deployed]---------->|--[forward]--------->|                 |
      |    (SIGNED)           |                     | download+install|
      |                       |<--[result]----------|                 |
      |<--[result]------------|    (SIGNED)         |                 |
```

**Message Signing (HMAC-SHA256):**
```json
{
  "client_id": "developer-alice",
  "timestamp": "2025-11-17T21:00:00Z",
  "nonce": "abc123",
  "payload": { "action": "deploy", "data": {...} },
  "signature": "hmac_sha256(shared_secret, timestamp+nonce+payload)"
}
```

---

## Development Workflow

```bash
# 1. Initialize repository
ljc init ~/my-linearjc-jobs

# 2. Sync registry state from coordinator
cd ~/my-linearjc-jobs
ljc sync
# → Connects via MQTT (signed messages)
# → Downloads current registry from coordinator

# 3. Create new job (simple scaffolding)
ljc new backup.pool
# → Creates job directory structure
# → Generates template files with comments
# → Developer edits manually

# 4. Edit job files
vim jobs/backup.pool/job.yaml                   # Configure job
vim jobs/backup.pool/script.sh                   # Implement logic

# 4b. Add registry entries (if new inputs/outputs needed)
ljc registry add backup_input --type fs --path /var/share/backup/in --kind dir
ljc registry add backup_output --type fs --path /var/share/backup/out --kind dir
ljc registry push                               # Sync to coordinator

# 5. Validate
ljc validate backup.pool
# → Structure check
# → Registry consistency
# → Conflict detection
# → Script filename heuristics

# 6. Build package
ljc build backup.pool
# → Creates dist/backup.pool.ljc

# 7. Deploy to coordinator
export LINEARJC_SECRET="<set-from-environment>"
ljc deploy dist/backup.pool.ljc
# → Connects via MQTT (signed messages)
# → Requests presigned MinIO URL
# → Uploads package to MinIO
# → Notifies coordinator
# → Coordinator downloads and installs
# → Returns result
```

---

## Implementation Phases

### Phase 1: Core Infrastructure ✅ COMPLETE

**Goal**: Basic tool skeleton and file operations

**Completed**:
- [x] Create Rust project structure
- [x] Write roadmap document
- [x] Set up dependencies (clap, serde_yaml, tar, colored, etc.)
- [x] Implement basic CLI structure with clap
- [x] Add YAML parsing/writing utilities
- [x] Add compact YAML formatter (one-line flow style)
- [x] Repository abstraction (find, create, list jobs)
- [x] Version comparison utilities

**Deliverables**:
- ✅ Working `ljc --help`
- ✅ YAML utility functions
- ✅ Compact format round-trip tested

**Time Invested**: 3 hours

---

### Phase 2: Repository Management ✅ COMPLETE

**Goal**: Initialize and structure job repositories

**Completed Commands**:
- ✅ `ljc init <path>` - Create repository structure
- ✅ `ljc list` - List jobs in repository
- ✅ `ljc info <job-id>` - Show job information
- ✅ `ljc registry [--verbose]` - Show registry map

**Deliverables**:
- ✅ `ljc init` creates directory structure with README
- ✅ `ljc list` shows jobs with version and build status
- ✅ `ljc info` displays comprehensive job details
- ✅ `ljc registry` shows compact or verbose registry view

**Time Invested**: 2 hours

---

### Phase 3: Job Creation ✅ COMPLETE

**Goal**: Simple scaffolding for new jobs

**Command**: ✅ `ljc new <job-id> [--capability pool]`

**Approach**: Simple template generation (not interactive)
- Creates job directory structure
- Generates template files with helpful comments
- Developer edits manually (faster, more flexible)

**Files Generated**:
- `job.yaml` - Template with inline documentation
- `script.sh` - Boilerplate with environment variable examples
- `manifest.yaml` - Package metadata template
- `JOB.md` - Documentation template
- `test/in/` - Directory for test input data

**Deliverables**:
- ✅ `ljc new` creates complete job structure
- ✅ All files have helpful comments
- ✅ Script template shows correct environment variable usage
- ✅ Examples for both filesystem and MinIO registry types

**Time Invested**: 1 hour

**Design Decision**: Chose simple scaffolding over interactive prompts for:
- Faster implementation
- Developer flexibility (edit in IDE, copy from other jobs)
- Scriptability (CI/CD friendly)
- Self-documenting templates

---

### Phase 4: Validation ✅ COMPLETE

**Goal**: Comprehensive job validation before deployment

**Command**: ✅ `ljc validate [job-id|--all]`

**Validation Levels Implemented**:

**Level 1 - Structure** (Errors):
- ✅ Required files exist (job.yaml, script.sh, manifest.yaml)
- ✅ Valid YAML syntax
- ✅ Required fields present (job.id, job.version)

**Level 2 - Registry Consistency** (Errors):
- ✅ All job inputs/outputs have corresponding registry entries
- ✅ No duplicate output keys across jobs (critical!)
- ✅ Registry entries properly formatted

**Level 3 - Orphaned Entries** (Warnings):
- ✅ Detect registry keys not used by any job

**Level 4 - Script Heuristics** (Warnings):
- ✅ Check if expected filenames appear in script
- ✅ Detect hardcoded paths with regex patterns

**Deliverables**:
- ✅ Comprehensive validation with clear pass/fail
- ✅ Error vs warning distinction
- ✅ Output conflict detection (critical for write-by-one pattern)
- ✅ Orphaned entry detection
- ✅ Summary statistics
- ✅ **Shebang validation** (2025-11-18)

**Time Invested**: 2 hours

---

### Phase 4.5: Version Management ✅ COMPLETE (2025-11-18)

**Goal**: Developer-friendly semantic versioning workflow

**Deliverables**:
- ✅ `ljc bump major|minor|patch <job-id>` command
- ✅ Semantic version parsing and validation (major.minor.patch)
- ✅ Atomic updates to job.yaml and manifest.yaml
- ✅ Version mismatch detection between files
- ✅ Default to 0.1.0 if version missing
- ✅ `--dry-run` flag to preview changes
- ✅ Clear before/after display
- ✅ Unit tests for version logic (5 tests passing)

**Usage**:
```bash
ljc bump patch backup.pool   # 1.2.3 → 1.2.4
ljc bump minor backup.pool   # 1.2.3 → 1.3.0
ljc bump major backup.pool   # 1.2.3 → 2.0.0
ljc bump patch backup.pool --dry-run  # Preview only
```

**Validations**:
- ❌ ERROR if job.yaml and manifest.yaml versions don't match
- ❌ ERROR if version format is invalid (not X.Y.Z)
- ❌ ERROR if required files missing

**Implementation**: New `bump.rs` command (280 lines, well-tested)

**Time Invested**: 1.5 hours

---

### Phase 5: Package Building ✅ COMPLETE

**Goal**: Build deployable .ljc packages

**Command**: ✅ `ljc build <job-id> [-o output.ljc]`

**Process**:
1. ✅ Check required files exist
2. ✅ Create tar.gz archive
3. ✅ Include: job.yaml, script.sh, manifest.yaml, JOB.md, bin/, data/
4. ✅ Write to dist/<job-id>.ljc
5. ✅ Report package size

**Features**:
- ✅ Validates file existence before building
- ✅ Properly sets script.sh as executable in archive
- ✅ Optional custom output path
- ✅ Clear progress output
- ✅ Package verification (size, contents)

**Deliverables**:
- ✅ Creates valid .ljc tar.gz packages
- ✅ Coordinator-compatible format
- ✅ Human-readable build output

**Time Invested**: 1 hour

---

### Phase 6: MQTT Authentication & Message Signing ✅ COMPLETE

**Goal**: Secure MQTT communication with coordinator using signed messages

**Architecture**: Same pattern as coordinator ↔ executor (reused executor code)

**Message Format**:
```json
{
  "client_id": "developer-alice",
  "timestamp": "2025-11-17T21:00:00Z",
  "nonce": "abc123",
  "payload": {
    "action": "deploy",
    "data": { ... }
  },
  "signature": "hmac_sha256(shared_secret, timestamp+nonce+payload)"
}
```

**Security Features**:
- **HMAC-SHA256** signatures (like executor uses)
- **Replay protection** - timestamp + nonce validation
- **Authorization** - coordinator checks developer permissions
- **Audit trail** - all requests logged with client_id

**Configuration**:
```yaml
# .ljcconfig or environment variables
mqtt:
  broker: tcp://192.0.2.10:1883
  client_id: developer-alice

coordinator:
  id: linearjc-coordinator-01

auth:
  shared_secret: "${LINEARJC_SECRET}"  # From environment
```

**MQTT Topics**:
```
# Developer → Coordinator
linearjc/deploy/request/<coordinator-id>
linearjc/deploy/complete/<coordinator-id>
linearjc/sync/request/<coordinator-id>

# Coordinator → Developer
linearjc/deploy/response/<client-id>
linearjc/deploy/status/<client-id>
linearjc/sync/response/<client-id>
```

**Implementation**:
```rust
// Dependencies
rumqttc = "0.24"      // MQTT client
hmac = "0.12"         // HMAC signing
sha2 = "0.10"         // SHA-256
uuid = "1.0"          // Nonces
serde_json = "1.0"    // JSON messages

// Core module: src/mqtt_client.rs
struct MqttClient {
    client: rumqttc::Client,
    shared_secret: String,
    client_id: String,
}

impl MqttClient {
    fn connect(config: &Config) -> Result<Self>;
    fn sign_message(&self, payload: &Value) -> String;
    fn verify_signature(&self, msg: &Message) -> bool;
    fn publish_request(&self, topic: &str, msg: &Value) -> Result<String>;
    fn wait_for_response(&self, request_id: &str, timeout: Duration) -> Result<Value>;
}
```

**Deliverables**:
- [x] Message signing/verification utilities (src/message_signing.rs)
- [x] Configuration loading (.ljcconfig + env vars) (src/config.rs)
- [x] Unit tests for message signing (5 tests passing)
- [x] Unit tests for config loading (4 tests passing)
- [x] Example .ljcconfig file
- [x] Integration tests (2 tests passing)
- [ ] MQTT client module (deferred to Phase 7 - not needed standalone)
- [ ] Request/response correlation (deferred to Phase 7)

**Actual Time**: 2 hours (faster due to code reuse from executor)

---

### Phase 7: MQTT Deployment ✅ COMPLETE

**Goal**: Deploy packages via MQTT + MinIO (secure, automated)

**Command**: `ljc deploy <package>` (implemented)

**Workflow**:
```
1. ljc connects to MQTT broker
2. ljc → coordinator: deploy-request (signed)
   - Package metadata (name, version, size, checksum)
3. coordinator → ljc: upload-url (signed, presigned MinIO URL)
4. ljc uploads .ljc to MinIO via HTTP PUT
5. ljc → coordinator: deploy-complete (signed)
6. coordinator downloads from MinIO
7. coordinator installs package
8. coordinator → ljc: install-result (signed)
```

**Implementation**:
```rust
pub fn run(package: &Path) -> Result<()> {
    let config = load_config()?;
    let mut mqtt = MqttClient::connect(&config)?;

    // 1. Read package metadata
    let metadata = PackageMetadata {
        name: extract_job_id(package)?,
        version: extract_version(package)?,
        size: package.metadata()?.len(),
        checksum: compute_sha256(package)?,
    };

    // 2. Request upload URL
    println!("Requesting upload URL...");
    let response = mqtt.request_upload_url(&metadata)?;
    let upload_url = response.upload_url;

    // 3. Upload to MinIO
    println!("Uploading to MinIO...");
    upload_to_minio(package, &upload_url)?;

    // 4. Notify coordinator
    println!("Notifying coordinator...");
    let result = mqtt.notify_deployment_complete(&metadata)?;

    // 5. Display result
    println!("✓ Deployment complete");
    println!("  Job: {} v{}", result.job_id, result.version);
    println!("  Status: {}", result.status);

    Ok(())
}
```

**User Experience**:
```bash
$ export LINEARJC_SECRET="<set-from-environment>"
$ ljc deploy dist/backup.pool.ljc

Connecting to coordinator...
✓ Authenticated as developer-alice
Requesting upload URL...
✓ Received presigned URL (expires in 10 min)
Uploading to MinIO...
  [████████] 100% 12.3 KB
✓ Upload complete
Installing on coordinator...
✓ Installed successfully

Package: backup.pool v1.0.0
Time: 2.3 seconds
```

**Deliverables**:
- [x] MQTT client module (src/mqtt_client.rs)
- [x] MQTT-based deploy command (src/commands/deploy.rs)
- [x] MinIO HTTP upload with reqwest
- [x] Progress display with colored output
- [x] Package metadata extraction (job_id from filename)
- [x] SHA-256 checksum computation
- [x] Request/response correlation with UUID
- [x] Timeout handling (30s upload URL, 60s install)
- [x] Error handling and user-friendly messages
- [x] Comprehensive status reporting

**Actual Time**: 2 hours (estimate: 3h, saved 1h with clean design)

---

### Phase 8: Registry Sync (MQTT) ✅ COMPLETE

**Goal**: Sync registry from coordinator via MQTT

**Command**: `ljc sync --from <host>`

**Files Added/Modified**:
- `tools/ljc/src/commands/sync.rs` - Full implementation (~280 lines)
- `src/coordinator/developer_api.py` - `handle_registry_sync_request()` handler
- `src/coordinator/mqtt_client.py` - Topic subscription and routing
- `tests/e2e/test_sync.py` - 6 E2E tests

**MQTT Topics**:
- Request: `linearjc/registry/request/{coordinator_id}`
- Response: `linearjc/registry/response/{client_id}`

**Message Format**:
```json
// Request (ljc → coordinator)
{
  "request_id": "uuid",
  "action": "registry_sync",
  "client_id": "developer-alice"
}

// Response (coordinator → ljc)
{
  "request_id": "uuid",
  "status": "success",
  "registry": { "key": {"type": "fs", "path": "...", "kind": "dir"} },
  "entry_count": 4
}
```

**User Experience**:
```bash
$ ljc sync --from coordinator-host

Syncing registry from coordinator...

  → Loading configuration...
    ✓ Coordinator: linearjc-coordinator
    ✓ MQTT: coordinator-host:1883
    ✓ Client ID: builder

  → Loading local registry...
    ✓ Local entries: 0

  → Connecting to MQTT broker...
    ✓ Connected

  → Requesting registry...
    ✓ Request sent (ID: fc96d333)
    → Waiting for registry data...
    ✓ Received 4 entries

  → Updating local registry...
    ✓ Written to /path/to/registry.yaml

════════════════════════════════════════════════════════════
  ✓ Registry synced successfully!

  + 4 new entries:
    + deploy_input
    + deploy_output
    + echo_input
    + echo_output

  Total: 4 entries
════════════════════════════════════════════════════════════
```

**Deliverables**:
- [x] MQTT-based sync command
- [x] Registry diff computation (added/removed/changed)
- [x] Clear change reporting (colored output)
- [x] E2E tests (6 tests)

**Actual Time**: 2 hours (matched estimate)

---

### Phase 9: Coordinator Developer API ✅ COMPLETE

**Goal**: Coordinator-side MQTT handlers for developer requests

**Location**: `src/coordinator/developer_api.py`

**Components**:

**1. Message Verification**:
```python
class DeveloperAuth:
    def __init__(self, config):
        self.developers = config.developers  # From config file

    def verify_signature(self, message):
        client_id = message['client_id']
        developer = self.developers.get(client_id)
        if not developer:
            raise AuthError(f"Unknown client: {client_id}")

        # Check timestamp (reject if > 5 minutes old)
        # Check nonce (track seen nonces to prevent replay)
        # Verify HMAC signature

    def check_permission(self, client_id, action):
        developer = self.developers[client_id]
        if action not in developer['permissions']:
            raise PermissionError(f"{client_id} not authorized for {action}")
```

**2. Deploy Handler**:
```python
def handle_deploy_request(self, message):
    # 1. Verify signature and permissions
    self.auth.verify_signature(message)
    self.auth.check_permission(message['client_id'], 'deploy')

    # 2. Generate presigned MinIO URL
    package_key = f"deployments/{job_id}-v{version}.ljc"
    upload_url = self.minio.generate_presigned_put_url(
        bucket='linearjc',
        key=package_key,
        expires_seconds=600  # 10 minutes
    )

    # 3. Send response
    response = {
        'request_id': message['nonce'],
        'upload_url': upload_url,
        'expires_in': 600
    }
    self.publish_signed_response(message['client_id'], response)

def handle_deploy_complete(self, message):
    # 1. Verify signature
    self.auth.verify_signature(message)

    # 2. Download from MinIO
    package_key = message['payload']['package_key']
    package_data = self.minio.download(package_key)

    # 3. Verify checksum
    checksum = hashlib.sha256(package_data).hexdigest()
    if checksum != message['payload']['checksum']:
        raise ValueError("Checksum mismatch")

    # 4. Save to temp file and install
    with tempfile.NamedTemporaryFile(suffix='.ljc') as tmp:
        tmp.write(package_data)
        tmp.flush()
        result = self.install_package(tmp.name)

    # 5. Send result
    response = {
        'request_id': message['nonce'],
        'status': 'installed',
        'job_id': result.job_id,
        'version': result.version
    }
    self.publish_signed_response(message['client_id'], response)
```

**3. Sync Handler**:
```python
def handle_sync_request(self, message):
    # 1. Verify signature
    self.auth.verify_signature(message)
    self.auth.check_permission(message['client_id'], 'sync')

    # 2. Read current registry
    registry = self.load_data_registry()

    # 3. Send response
    response = {
        'request_id': message['nonce'],
        'registry': registry,
        'timestamp': datetime.utcnow().isoformat()
    }
    self.publish_signed_response(message['client_id'], response)
```

**Configuration**:
```yaml
# coordinator/config.yaml
developers:
  developer-alice:
    shared_secret: "<secret-from-manager>"
    permissions:
      - deploy
      - sync
    rate_limit:
      max_deploys_per_hour: 10

  developer-bob:
    shared_secret: "<alternate-test-secret>"
    permissions:
      - sync  # Read-only
```

**Deliverables**:
- [ ] DeveloperAuth class with signature verification
- [ ] Deploy request/complete handlers
- [ ] Sync request handler
- [ ] MQTT topic subscriptions
- [ ] Configuration loading
- [ ] Audit logging

**Time Estimate**: 4 hours

---

### Phase 10: Coordinator Version Upgrade ✅ COMPLETE

**Goal**: Allow coordinator to replace jobs with newer versions

**Location**: `src/coordinator/main.py` install command

**Implementation**:
```python
# In install command, after extracting job_id:
dest_job = jobs_dir / f"{job_id}.yaml"

if dest_job.exists():
    # Load existing version
    with open(dest_job) as f:
        existing = yaml.safe_load(f)
    existing_version = existing['job']['version']
    new_version = job_data['job']['version']

    # Compare versions (semantic versioning)
    if compare_versions(new_version, existing_version) > 0:
        click.echo(f"  Upgrading {job_id}: {existing_version} → {new_version}")
        # Continue with install (replace files)

        # For registry: allow duplicate keys if same job_id (upgrade path)
        # Check that all registry keys belong to this job
        for key in fragment_data.keys():
            if key in existing_registry:
                # Verify it was originally created by this job
                # (check comments or track in metadata)
                pass  # Allow replacement

    elif compare_versions(new_version, existing_version) == 0:
        click.echo(f"  ERROR: Version {new_version} already installed")
        sys.exit(1)
    else:
        click.echo(f"  ERROR: Cannot downgrade: {new_version} < {existing_version}")
        sys.exit(1)
```

**Version Comparison**:
```python
def compare_versions(v1: str, v2: str) -> int:
    """Compare semantic versions. Returns 1 if v1>v2, 0 if equal, -1 if v1<v2"""
    parts1 = [int(x) for x in v1.split('.')]
    parts2 = [int(x) for x in v2.split('.')]

    for i in range(3):
        p1 = parts1[i] if i < len(parts1) else 0
        p2 = parts2[i] if i < len(parts2) else 0
        if p1 > p2:
            return 1
        elif p1 < p2:
            return -1

    return 0
```

**Registry Handling for Upgrades**:
- Allow duplicate registry keys if job_id matches (upgrade path)
- Prevent conflicts with other jobs (different job_id)
- Update registry entries atomically

**Deliverables**:
- [x] Coordinator: Version comparison logic (`compare_versions()` function)
- [x] Coordinator: Upgrade path in install command
- [x] Coordinator: Clear upgrade/error messages
- [x] Coordinator: Prevent downgrades and same-version redeploy
- [x] Executor: Fixed Airdrop version comparison (was using broken string comparison)
- [x] E2E tests (3 tests)

**Actual Time**: 1 hour (estimate was 2h)

---

## Testing Plan

### Unit Tests
- [x] YAML compact format round-trip
- [x] Version comparison logic
- [ ] Message signing/verification
- [ ] Request/response correlation

### Integration Tests

**1. Full Development Workflow**:
```bash
ljc init /tmp/test-repo
cd /tmp/test-repo
ljc new test.pool
# Edit files
ljc validate test.pool
ljc build test.pool
# Verify dist/test.pool.ljc exists and is valid
```

**2. Conflict Detection**:
```bash
ljc new job1.pool  # Uses output: output1
ljc new job2.pool  # Try to use same output: output1
ljc validate --all
# Should fail with output conflict error
```

**3. MQTT Deployment** (requires coordinator):
```bash
export LINEARJC_SECRET="<test-placeholder-secret>"
ljc deploy dist/test.pool.ljc
# Should authenticate, upload, and install
```

**4. Registry Sync**:
```bash
ljc sync
# Should fetch registry from coordinator
# Verify registry-map.yaml updated
```

---

## Dependencies

**Rust Crates**:
```toml
[dependencies]
# CLI & output
clap = { version = "4.5", features = ["derive", "cargo"] }
colored = "2.1"

# Data formats
serde = { version = "1.0", features = ["derive"] }
serde_yaml = "0.9"
serde_json = "1.0"

# Packaging
tar = "0.4"
flate2 = "1.0"

# Error handling
anyhow = "1.0"
thiserror = "1.0"

# Utilities
regex = "1.10"
uuid = { version = "1.0", features = ["v4"] }

# MQTT (Phase 6+)
rumqttc = "0.24"
hmac = "0.12"
sha2 = "0.10"

# HTTP for MinIO upload
reqwest = { version = "0.12", features = ["blocking"] }
```

---

## File Locations

**Tool**: `submodules/linearjc/tools/ljc/`
**Coordinator**: `submodules/linearjc/src/coordinator/`
- `main.py` - Install command with version upgrade
- `developer_api.py` - MQTT developer request handlers (new)

---

## Future Enhancements

### Phase 11: Developer Runtime Commands ✅ COMPLETE

**Goal**: Interactive developer tools for testing and debugging jobs in real-time

**Motivation**: Dramatically improve development velocity by enabling immediate job testing and live monitoring without waiting for scheduler or manually checking logs.

**Status**: Complete 2025-12-04 - All 7 commands implemented

**Core Commands** (all complete):

#### `ljc exec <job-id> [--follow] [--wait]` ✅
**Execute job immediately (bypass scheduler)**

#### `ljc tail <job-id|execution-id> [--timeout <secs>]` ✅
**Follow job execution progress in real-time**

#### `ljc status [job-id] [--all] [--json]` ✅
**Show job scheduling state and execution status**
- Query coordinator's JobTree state via MQTT
- Display: last run, next run, executions in last 24h, scheduling bounds
- Show active execution if running (state, executor, duration, timeout)

#### `ljc ps [--all] [--executor <id>] [--json]` ✅
**List all active/running jobs**
- Query coordinator's JobTracker
- Show: execution_id, state, executor, duration
- Filter by executor

#### `ljc logs <job-id> [--last N] [--json]` ✅
**Show execution history from coordinator memory**
- Query JobTree.execution_history
- Display: timestamp, age

#### `ljc kill <execution-id> [--force] [--wait]` ✅
**Cancel running job execution**
- Send cancel request to coordinator
- Coordinator publishes cancel to executor
- Executor terminates job (SIGTERM or SIGKILL)
- `--wait`: Follow progress until termination

**Implementation**:
- Coordinator: 4 new handlers in `developer_api.py`
- MQTT: 4 new topics following existing pattern
- Executor: PID tracking + cancel subscription for kill support

**Future Commands** (not yet implemented):

#### `ljc watch [--jobs j1,j2] [--executors e1,e2]`
**Real-time TUI dashboard of all activity**

#### `ljc tree [job-id]`
**Visualize job trees and registry dependencies**

**Development Workflow Improvement**:
```bash
# Before: Wait hours for scheduler
vim jobs/backup.pool/script.sh
ljc build && ljc deploy
# ... wait ... check coordinator logs manually ...

# After: Instant feedback
vim jobs/backup.pool/script.sh
ljc exec backup.pool --follow
→ ✓ COMPLETED (13s)
ljc bump patch && ljc build && ljc deploy
```

**Security**:
- Same HMAC-SHA256 signing as deploy
- Audit trail with client_id logging
- Dev executions clearly marked in logs

---

### Phase 12: Multi-Job Tree Execution ✅ COMPLETE

**Goal**: Enable execution of job dependency chains (`a → b → c`)

**Completed 2025-12-03**

**Implementation**:

1. **Job Index Tracking** ✅
   - Added `job_index: int` field to `JobExecution` dataclass
   - Root job: `job_index=0`, chain jobs: `job_index=1, 2, ...`
   - `JobTracker.find_by_tree_execution_id()` returns jobs sorted by index

2. **Chain Continuation Logic** ✅
   - `_handle_progress_update()` checks if more jobs in chain on completion
   - If `job_index < len(tree.jobs) - 1`: execute next job via `_execute_chain_job()`
   - If last job: collect outputs from leaf job, unregister tree

3. **Input/Output Passing** ✅
   - `JobExecutor.prepare_chain_job_inputs()` handles intermediate data
   - Previous job's outputs read from MinIO (same `tree_execution_id`)
   - External inputs still fetched from filesystem/registry
   - Only leaf job outputs written to filesystem

4. **Progress Forwarding** ✅
   - `dev_client_id` passed to chain jobs for progress forwarding
   - All jobs in chain forward to `ljc exec --follow` / `ljc tail`

**Files Modified**:
- `src/coordinator/job_tracker.py` - Added `job_index` field, `find_by_tree_execution_id()`
- `src/coordinator/main.py` - `_execute_chain_job()`, chain continuation in progress handler
- `src/coordinator/job_executor.py` - `prepare_chain_job_inputs()`, `_prepare_intermediate_input()`
- `tests/unit/test_chain_execution.py` - 14 unit tests for chain logic
- `tests/e2e/test_chain.py` - E2E test structure for chain execution
- `tests/e2e/jobs/chain-step1/` - Test chain job 1
- `tests/e2e/jobs/chain-step2/` - Test chain job 2

**Test Results**: 87 passed, 8 skipped (49 unit tests including 14 new chain tests)

---

## Current Status Summary

**✅ Completed (9 hours)**:
- Full repository management
- Job scaffolding with templates
- Comprehensive validation
- Package building
- All core local operations working

**⏳ In Progress**:
- MQTT authentication module

**📋 Remaining**:
- MQTT deployment command
- MQTT sync command
- Coordinator developer API
- Coordinator version upgrade logic

**Total Time**: 9h complete | ~14h remaining | **~23h total**

---

## Next Session Checklist

**If Continuing Phase 6 (MQTT Auth):**
- [ ] Add rumqttc, hmac, sha2 dependencies
- [ ] Implement MqttClient struct
- [ ] Add message signing/verification
- [ ] Add configuration loading
- [ ] Test connection to broker
- [ ] Test request/response correlation

**If Continuing Phase 7 (Deployment):**
- [ ] Implement deploy command
- [ ] Add MinIO upload logic
- [ ] Test with coordinator stub
- [ ] Add progress display
- [ ] Handle errors gracefully

**General Maintenance:**
- [ ] Fix hello.pool script filenames
- [ ] Write unit tests
- [ ] Update documentation
- [ ] Consider release build optimizations

---

## Success Criteria

**v0.1.0 (Current)**: Local development workflow complete ✅
- [x] Create, validate, and build jobs locally
- [x] All commands working except sync/deploy

**v0.2.0 (Next)**: MQTT deployment complete
- [ ] Deploy jobs via MQTT + MinIO
- [ ] Sync registry via MQTT
- [ ] Coordinator handles developer requests
- [ ] Version upgrades working

**v1.0.0 (Production Ready)**:
- [ ] Full end-to-end workflow tested
- [ ] Error handling complete
- [ ] Documentation complete
- [ ] Multi-developer tested
- [ ] CI/CD integration examples
