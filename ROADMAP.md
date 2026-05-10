# LinearJC Roadmap

**Last updated**: 2026-05-10

## Implemented

| Feature | Status | Notes |
|---------|--------|-------|
| Coordinator v2 (async) | Done | Single-threaded async with aiomqtt, heartbeat discovery |
| Executor (Rust) | Done | Landlock, cgroups v2, network namespaces, privilege separation |
| linearjc-core (shared crate) | Done | Isolation, execution, archive - shared between executor and ljc |
| Register model | Done | fs/temp/minio types, protect flag, kind field |
| ENQ/DEQ locking | Done | Readers-writer locks, all-or-nothing acquisition, FIFO queue |
| Write-through caching | Done | MinIO as L1 cache, filesystem as registers |
| Linear chains | Done | A -> B -> C with explicit `depends:` |
| Fan-out (tree duplication) | Done | A -> B and A -> C creates two trees, A runs twice |
| Automatic cleanup | Done | MinIO temp objects cleaned on completion/failure/startup |
| ljc CLI | Done | new, validate, build, deploy, exec, ps, logs, tail, status, kill |
| ljc self-update | Done | Version check and download from coordinator |
| Executor auto-update | Done | Coordinator pushes updates via MQTT on heartbeat |
| Hot reload (SIGHUP) | Done | Safety validation against active locks |
| MQTT signing | Done | HMAC-SHA256 envelope signing with max-age |

## Current Limitations

### Fan-out duplicates root execution

The current fan-out model creates separate trees per consumer. Each tree runs its own copy of the shared root job. For a 6-hour rainwater simulation with 3 consumers, this means 18 hours of compute instead of 6.

This is the primary driver for moving to batch groups with single root execution.

### No generation scoping

There is no concept of a "batch generation" at runtime. If job A writes register X and job B reads register X, there is no guarantee that B reads the output from *this run* of A rather than a stale result from a previous run. A partial failure followed by a retry can silently mix fresh and stale data.

Generation tracking would pin all jobs in a batch execution to the same generation, making data provenance deterministic.

### No atomic job deployment

`ljc deploy` pushes one package at a time. When two jobs are updated together (e.g., a schema change in a producer requires a matching consumer update), there is a window where the old consumer runs against the new producer's output. There is no way to deploy a set of job updates as one atomic unit.

### Observability gaps

From production usage (see `docs/phase16-notes.md`):
- Job stdout/stderr not visible after execution completes
- `ljc exec --follow` only streams the first job in a chain
- No workdir preservation on failure for debugging
- `ljc logs` shows timestamps but not actual script output
- No timing breakdown per job (input prep, execution, upload)

## Priorities

### 1. Batch groups with single root execution

**Motivation**: Eliminate N*compute cost for fan-out workloads.

Replace tree-per-consumer model with a batch graph where each job runs once per batch generation. The coordinator derives the execution graph from register ownership (`reads:`/`writes:`), not from explicit `depends:` links. Fan-out and multi-input barriers emerge from the data contracts.

This replaces `JobTree`/`TreeExecution` with `BatchGraph`/`BatchExecution` in the coordinator. The `depends:` field becomes a validation assertion, not the primary dependency declaration.

### 2. Generation tracking

**Motivation**: Correctness guarantee that downstream jobs consume same-generation data.

Each batch execution gets a generation ID. Temp registers are scoped to that generation. Barrier jobs wait for all input registers to be ready within the same generation. This prevents stale data mixing after partial failures.

Connects to future GDG (Generation Data Group) support for versioned register retention and rollback.

### 3. Step-level restart

**Motivation**: Avoid re-running expensive completed steps after a late-chain failure.

When step 3 of a 5-step chain fails, the current model reruns the entire chain from the root. For a 6-hour simulation followed by post-processing, a failure in the last step wastes 6 hours of recomputation.

Step-level restart skips steps whose outputs are still valid for the current generation. The coordinator checks whether each step's output registers already contain data for this generation ID, and resumes from the first step that needs to run.

Depends on generation tracking (priority 2): without generation scoping, the coordinator cannot distinguish "this output is from the current run" from "this is stale data from yesterday." Generation IDs make the validity check deterministic.

### 4. Observability

**Motivation**: Production debugging is currently blind.

- Capture job stdout/stderr to MinIO before workdir cleanup
- Stream progress for all jobs in chain during `--follow`
- Preserve workdir on failure (opt-in flag)
- Include actual script output in `ljc logs`
- Timing breakdown per job step

See `docs/phase16-notes.md` for the full list of gaps found during production testing.

### 5. Atomic deployment sets

**Motivation**: Safe coordinated job updates.

A `ljc deploy-set` command that pushes multiple packages and a registry update as one batch. The coordinator applies the new configuration atomically on the next reload. In-flight batch generations finish with old versions; new generations use the new set.

### 6. Execution archives

**Motivation**: Audit trail and failure analysis.

Store execution metadata, event timelines, and job outputs in a structured archive. SQLite index for fast queries. Powers future CLI commands: `ljc timeline`, `ljc diff`, `ljc analyze`.

Lower priority than the above because the observability improvements (priority 3) address the immediate pain of blind debugging.

## Future ideas

Not prioritized, may never be implemented:

- **GDG (Generation Data Groups)**: Automatic versioning of registers with rolling retention
- **Conditional execution**: Run/skip based on upstream exit code ranges
- **DISP semantics**: Declarative cleanup behavior on success/failure
- **Checkpoint/restart**: Resume long-running jobs from checkpoint
- **Procedure templates (PROC)**: Reusable job templates with parameters
- **Single-machine mode**: `ljc run` for direct execution without coordinator/MQTT
- **ljc watch**: Real-time TUI dashboard
