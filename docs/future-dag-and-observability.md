# LinearJC Future: Register-Driven Barrier Execution & Observability

**Created**: 2026-01-12
**Status**: Proposal for future sessions
**Prerequisites**: Phase 15 (Fan-out) complete

---

## Overview

This document outlines planned enhancements to LinearJC's execution model and observability features. It covers:

1. **Execution Artifacts** - Persistent storage of all execution data
2. **Register-Driven Barrier Execution** - Multi-input jobs that wait for all required registers
3. **ljc CLI Improvements** - New views powered by execution artifacts
4. **Design Decisions** - Rationale for key architectural choices

The user-facing model should remain mainframe-style: modules produce and consume named registers, and the controller derives ordering from data readiness. The implementation may use graph algorithms internally, but LinearJC should not become a general DAG authoring system.

---

## Part 1: Execution Artifacts

### Current State

- Intermediate data stored in MinIO temp bucket
- Cleaned up immediately after chain completion
- No historical record of what data was processed
- Debugging requires reproducing the failure

### Proposed: Execution Archive

Store complete execution artifacts to `/data/linearjc/exec_jobs/`:

```
/data/linearjc/exec_jobs/
├── index.db                        # SQLite index for fast queries
└── {batch_exec_id}/
    ├── metadata.json               # Execution metadata
    ├── chain.json                  # Event timeline
    └── jobs/
        └── {job_id}/
            ├── job.yaml            # Job definition snapshot
            ├── inputs/             # All inputs (copied)
            ├── outputs/            # All outputs (before cleanup)
            ├── stdout.log          # Job stdout
            ├── stderr.log          # Job stderr
            └── execution.json      # Timing, exit code, metrics
```

### metadata.json Schema

```json
{
  "batch_exec_id": "daily.close-20260112-143052-a1b2c3",
  "batch_id": "daily.close",
  "status": "completed|failed|timeout",
  "trigger": "scheduled|manual|replay",
  "started_at": "2026-01-12T14:30:52Z",
  "completed_at": "2026-01-12T14:35:18Z",
  "duration_ms": 266000,
  "jobs": [
    {
      "job_id": "extract.data",
      "version": "1.2.0",
      "executor": "executor-host-executor-01",
      "started_at": "2026-01-12T14:30:53Z",
      "completed_at": "2026-01-12T14:32:15Z",
      "duration_ms": 82000,
      "exit_code": 0,
      "input_bytes": 12582912,
      "output_bytes": 2097152
    }
  ]
}
```

### chain.json Schema (Event Stream)

```json
{
  "events": [
    {"ts": "2026-01-12T14:30:52.001Z", "type": "batch_started", "batch_exec_id": "..."},
    {"ts": "2026-01-12T14:30:52.050Z", "type": "job_queued", "job_id": "extract.data"},
    {"ts": "2026-01-12T14:30:52.100Z", "type": "job_dispatched", "job_id": "extract.data", "executor": "..."},
    {"ts": "2026-01-12T14:30:53.200Z", "type": "job_started", "job_id": "extract.data"},
    {"ts": "2026-01-12T14:32:15.500Z", "type": "job_completed", "job_id": "extract.data", "exit_code": 0},
    {"ts": "2026-01-12T14:32:15.600Z", "type": "output_uploaded", "job_id": "extract.data", "key": "parsed.json", "bytes": 15234}
  ]
}
```

### index.db Schema (SQLite)

```sql
CREATE TABLE executions (
    batch_exec_id TEXT PRIMARY KEY,
    batch_id TEXT NOT NULL,
    status TEXT NOT NULL,
    trigger TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    duration_ms INTEGER,
    job_count INTEGER,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_executions (
    job_exec_id TEXT PRIMARY KEY,
    batch_exec_id TEXT NOT NULL REFERENCES executions(batch_exec_id),
    job_id TEXT NOT NULL,
    version TEXT NOT NULL,
    executor TEXT,
    status TEXT NOT NULL,
    started_at TEXT,
    completed_at TEXT,
    duration_ms INTEGER,
    exit_code INTEGER,
    input_bytes INTEGER,
    output_bytes INTEGER
);

CREATE INDEX idx_exec_batch ON executions(batch_id);
CREATE INDEX idx_exec_status ON executions(status);
CREATE INDEX idx_exec_started ON executions(started_at);
CREATE INDEX idx_job_batch ON job_executions(batch_exec_id);
CREATE INDEX idx_job_id ON job_executions(job_id);
```

### Configuration

```yaml
# coordinator config
coordinator:
  exec_archive:
    enabled: true
    path: /data/linearjc/exec_jobs
    retention_days: 30              # Auto-prune after 30 days
    auto_on_failure: true           # Always archive failed executions
    auto_on_success: false          # Only archive if --debug flag
    max_artifact_size_mb: 100       # Skip archiving huge outputs
```

### Coordinator Implementation

```python
async def _archive_execution(self, batch_exec_id: str, force: bool = False):
    """Archive execution artifacts before cleanup."""

    config = self.config.exec_archive
    if not config.enabled:
        return

    # Check if we should archive
    execution_failed = self._execution_failed(batch_exec_id)
    if not force and not execution_failed and not config.auto_on_success:
        return

    archive_dir = Path(config.path) / batch_exec_id
    archive_dir.mkdir(parents=True, exist_ok=True)

    # 1. Build and write metadata
    metadata = self._build_execution_metadata(batch_exec_id)
    (archive_dir / "metadata.json").write_text(
        json.dumps(metadata, indent=2)
    )

    # 2. Write event chain
    events = self._get_execution_events(batch_exec_id)
    (archive_dir / "chain.json").write_text(
        json.dumps({"events": events}, indent=2)
    )

    # 3. Download MinIO artifacts
    await self._archive_minio_artifacts(batch_exec_id, archive_dir)

    # 4. Copy work directory contents
    await self._archive_work_dir(batch_exec_id, archive_dir)

    # 5. Update index.db
    self._update_archive_index(batch_exec_id, metadata)

    logger.info(f"Archived execution: {archive_dir}")
```

### Retention & Cleanup

```python
async def _prune_old_archives(self):
    """Remove archives older than retention_days."""

    config = self.config.exec_archive
    cutoff = datetime.now() - timedelta(days=config.retention_days)

    conn = sqlite3.connect(config.path + "/index.db")
    old_execs = conn.execute("""
        SELECT batch_exec_id FROM executions
        WHERE created_at < ?
    """, (cutoff.isoformat(),)).fetchall()

    for (batch_exec_id,) in old_execs:
        archive_dir = Path(config.path) / batch_exec_id
        if archive_dir.exists():
            shutil.rmtree(archive_dir)
        conn.execute(
            "DELETE FROM executions WHERE batch_exec_id = ?",
            (batch_exec_id,)
        )
        conn.execute(
            "DELETE FROM job_executions WHERE batch_exec_id = ?",
            (batch_exec_id,)
        )

    conn.commit()
    conn.close()

    if old_execs:
        logger.info(f"Pruned {len(old_execs)} old execution archives")
```

---

## Part 2: Register-Driven Barrier Execution

### Current State (Phase 15)

- Linear chains: `A → B → C`
- Fan-out: `A → B` and `A → C` (A runs twice, separate trees)
- No barrier jobs yet: `A → C` and `B → C` rejected when C needs both outputs

### Proposed: Multi-Input Barrier Jobs

```
     A ──┐
         ├──► C ──► D
     B ──┘

C waits for BOTH input registers from A and B to be ready for the same batch generation.
```

### Job Definition

```yaml
registry:
  north_data: {type: temp, kind: file}
  south_data: {type: temp, kind: file}
  all_data:   {type: fs, path: /data/all.csv, kind: file}

- id: consolidate.all
  reads: [north_data, south_data]
  writes: [all_data]
```

`consolidate.all` is a barrier job. The coordinator infers the wait from register ownership:

- `north_data` is written by `extract.north`
- `south_data` is written by `extract.south`
- `consolidate.all` runs when both are ready for the same batch generation

If a future schema keeps multi-value `depends:`, it should be treated as a validation assertion, not as the primary dependency declaration.

### Data Structures

```python
class BatchGraph(BaseModel):
    """Internal graph derived from register ownership."""
    batch_id: str                         # e.g., "daily.close"
    roots: list[str]                      # Job IDs with no dependencies
    jobs: dict[str, Job]                  # All jobs by ID
    edges: dict[str, list[str]]           # job_id -> successor job_ids
    reverse_edges: dict[str, list[str]]   # job_id -> predecessor job_ids

    min_daily: int                        # Computed from jobs
    max_daily: int


class BatchExecution(BaseModel):
    """Tracks a single batch generation."""
    batch_exec_id: str
    graph: BatchGraph

    # Job states
    pending: set[str]                     # Waiting for predecessors
    ready: set[str]                       # Ready to dispatch
    running: dict[str, str]               # job_id -> job_execution_id
    completed: set[str]                   # Successfully finished
    failed: set[str]                      # Failed

    # Barrier tracking: job_id -> set of predecessors not yet complete
    barriers: dict[str, set[str]]

    started_at: float
    finished_at: float | None = None
    status: str = "running"               # running, completed, failed
```

### Barrier Logic

```python
async def _on_job_completed(self, batch_exec: BatchExecution, job_id: str):
    """Handle job completion - update register readiness and barriers."""

    batch_exec.completed.add(job_id)
    del batch_exec.running[job_id]

    # Check all successors
    for successor_id in batch_exec.graph.edges.get(job_id, []):
        # Remove this job from successor's barrier
        batch_exec.barriers[successor_id].discard(job_id)

        # If barrier is empty, successor is ready
        if not batch_exec.barriers[successor_id]:
            batch_exec.pending.discard(successor_id)
            batch_exec.ready.add(successor_id)

            # Dispatch immediately
            await self._dispatch_batch_job(batch_exec, successor_id)


async def _dispatch_batch_job(self, batch_exec: BatchExecution, job_id: str):
    """Dispatch a job with inputs from all predecessors."""

    job = batch_exec.graph.jobs[job_id]
    batch_exec.ready.discard(job_id)

    # Prepare inputs from ALL predecessors
    inputs = {}
    for pred_id in batch_exec.graph.reverse_edges.get(job_id, []):
        pred_job = batch_exec.graph.jobs[pred_id]
        for output_key in pred_job.writes:
            if output_key in job.reads:
                # Download from MinIO
                inputs[output_key] = await self._get_predecessor_output(
                    batch_exec.batch_exec_id, pred_id, output_key
                )

    # Create job execution and dispatch
    job_exec_id = f"{batch_exec.batch_exec_id}-{job_id}"
    batch_exec.running[job_id] = job_exec_id

    await self._dispatch_to_executor(job, job_exec_id, inputs)
```

### Parallel Execution

With barrier support, independent root modules can run in parallel:

```python
async def _start_batch_execution(self, graph: BatchGraph) -> BatchExecution:
    """Start a new batch generation."""

    batch_exec_id = f"{graph.batch_id}-{timestamp()}-{short_uuid()}"

    # Initialize barriers for all non-root jobs
    barriers = {}
    for job_id, predecessors in graph.reverse_edges.items():
        if predecessors:
            barriers[job_id] = set(predecessors)

    batch_exec = BatchExecution(
        batch_exec_id=batch_exec_id,
        graph=graph,
        pending=set(barriers.keys()),
        ready=set(graph.roots),        # Roots are immediately ready
        running={},
        completed=set(),
        failed=set(),
        barriers=barriers,
        started_at=time.time(),
    )

    # Dispatch ALL roots in parallel
    for root_id in graph.roots:
        await self._dispatch_batch_job(batch_exec, root_id)

    return batch_exec
```

### Failure Handling

```python
async def _on_job_failed(self, batch_exec: BatchExecution, job_id: str, error: str):
    """Handle job failure in a batch generation."""

    batch_exec.failed.add(job_id)
    del batch_exec.running[job_id]

    # Strategy: Fail entire batch generation (simplest)
    # Future: Could implement partial failure (fail only dependent branch)

    batch_exec.status = "failed"
    batch_exec.finished_at = time.time()

    # Cancel any running jobs
    for running_job_id, job_exec_id in list(batch_exec.running.items()):
        await self._cancel_job(job_exec_id)

    # Archive for debugging (always archive failures)
    await self._archive_execution(batch_exec.batch_exec_id, force=True)

    # Cleanup MinIO intermediates
    await self._cleanup_execution(batch_exec.batch_exec_id)
```

### Scheduling

Batch-level scheduling (recommended approach):

```python
def _get_ready_batches(self) -> list[BatchGraph]:
    """Get batch groups ready to execute based on schedule."""

    ready = []
    for graph in self.batch_graphs.values():
        # Check min/max daily constraints
        executions_today = self._count_executions_last_24h(graph.batch_id)

        if executions_today >= graph.max_daily:
            continue  # At max

        if executions_today < graph.min_daily:
            # Must run - check if enough time left in window
            ready.append(graph)
        else:
            # May run - use ideal interval
            if self._ideal_interval_elapsed(graph):
                ready.append(graph)

    return ready
```

---

## Part 3: ljc CLI Improvements

### New Commands Enabled by Execution Archives

#### `ljc timeline` - Execution Timeline Visualization

```bash
$ ljc timeline {batch_exec_id}

  daily.close | 2026-01-12 14:30:52 | 4m 26s | completed
  ════════════════════════════════════════════════════════════

  14:30:52 ┬─────────────────────────────────────────────────
           │ extract.north    [██████████░░░░░░░░]  42s  ✓
           │ extract.south    [████████████████░░]  58s  ✓
  14:31:50 ┼──────────── barrier ─────────────────
           │ validate.all     [██████████████░░░░]  51s  ✓
  14:32:41 ┼──────────────────────────────────────
           │ consolidate      [████████████████████]  1m 12s  ✓
  14:33:53 ┴─────────────────────────────────────────────────

  Critical path: extract.south → validate.all → consolidate
```

#### `ljc diff` - Compare Executions

```bash
$ ljc diff {batch_exec_id_a} {batch_exec_id_b}

  Comparing:
    A: daily.close-20260111-140000 (succeeded)
    B: daily.close-20260112-143052 (succeeded)

  TIMING
  │ Job             │ A        │ B        │ Delta    │
  ├─────────────────┼──────────┼──────────┼──────────┤
  │ consolidate     │ 45s      │ 1m 12s   │ +60%     │

  DATA SIZE
  │ extracted.north │ 1.8 MB   │ 2.1 MB   │ +17%     │
```

#### `ljc analyze` - Failure Analysis

```bash
$ ljc analyze {failed_batch_exec_id}

  Failure Analysis
  ════════════════════════════════════════════════════════════

  FAILED: validate.all after 12s (exit code 1)

  LAST SUCCESS: daily.close-20260112-143052 (yesterday)

  STDERR (last 10 lines):
  > ValidationError: NULL timestamp at row 12847
  > FATAL: 23 validation errors, threshold is 10

  INPUT DIFF from last success:
  + 23 new records with NULL 'timestamp' in extracted.north

  ARTIFACTS: /data/linearjc/exec_jobs/daily.close-20260113-090000/
```

#### `ljc dashboard` - Performance Overview

```bash
$ ljc dashboard {batch_id} --period 7d

  daily.close (last 7 days)
  ════════════════════════════════════════════════════════════

  SUMMARY: 6 succeeded, 1 failed | Avg: 4m 12s | Trend: +8%/day

  Jan 07 │████████████████████░░░░│ 3m 42s  ✓
  Jan 08 │█████████████████████░░░│ 3m 51s  ✓
  Jan 09 │██████████████████████░░│ 4m 02s  ✓
  Jan 10 │███████████████████████░│ 4m 15s  ✓
  Jan 11 │████████████████████████│ 4m 26s  ✓
  Jan 12 │███████░░░░░░░░░░░░░░░░░│ 1m 12s  ✗
  Jan 13 │█████████████████████████│ 4m 38s  ✓

  BOTTLENECK: consolidate (27% of total time)
```

#### `ljc lineage` - Data Lineage

```bash
$ ljc lineage {register_key} --execution {batch_exec_id}

  Data Lineage: published.report
  ════════════════════════════════════════════════════════════

  SOURCES
  ├── sensor.raw_north    /data/sensors/north/    (12.4 MB)
  └── sensor.raw_south    /data/sensors/south/    (8.7 MB)

  TRANSFORMATIONS
  ├─► extract.north → extracted.north (2.1 MB)
  ├─► extract.south → extracted.south (1.8 MB)
  ├─► validate.all  → validated.data (3.6 MB)    [barrier]
  ├─► consolidate   → consolidated (1.2 MB)
  └─► publish       → published.report (847 KB)   [FINAL]
```

#### `ljc replay` - Replay Execution

```bash
$ ljc replay {batch_exec_id} --from {job_id}

  Replaying from validate.all using archived inputs...

  ✓ Restored: extracted.north (2.1 MB)
  ✓ Restored: extracted.south (1.8 MB)

  validate.all    [████████████████████████] 51s  ✓
  consolidate     [████████████████████████] 1m 10s  ✓
  publish.report  [████████████████████████] 32s  ✓

  Replay completed. Outputs identical to original.
```

#### `ljc export` - Export Data

```bash
# Export metrics for monitoring systems
$ ljc export metrics --format prometheus > metrics.prom

# Export history for analysis
$ ljc export history --format parquet > executions.parquet

# Export derived register graph for visualization
$ ljc export graph {batch_id} --format dot > pipeline.dot
```

### Implementation Priority

| Command | Value | Complexity | Dependencies |
|---------|-------|------------|--------------|
| `ljc timeline` | High | Low | Exec archives |
| `ljc analyze` | High | Medium | Exec archives |
| `ljc diff` | High | Medium | Exec archives + index.db |
| `ljc dashboard` | High | Medium | index.db |
| `ljc lineage` | Medium | Medium | Exec archives |
| `ljc replay` | Very High | High | Exec archives + coordinator |
| `ljc export` | Medium | Low | index.db |

---

## Part 4: Design Decisions

### Decision 1: Persistent Outputs via Publish Jobs

**Problem**: Some intermediate results need to be accessible outside the chain.

**Options Considered**:

1. **Mark register as persistent** - `retention: persistent` in data-registry
2. **Explicit publish job** - Separate job that writes to filesystem

**Decision**: Use explicit publish jobs.

**Rationale**:
- Keeps chain semantics clean (intermediates are always temporary)
- Clear ownership and versioning
- No consistency hazards (A can't overwrite while B is reading)
- Explicit is better than implicit

**Example**:

```yaml
# DON'T: Mark intermediate as persistent (rejected)
data.processed:
  type: fs
  retention: persistent  # Mixing concerns!

# DO: Use explicit publish job
- id: process.data
  writes: [intermediate.processed]  # MinIO temp

- id: publish.snapshot
  depends: [process.data]
  reads: [intermediate.processed]
  writes: [published.snapshot]      # Filesystem, persistent
  # Script: cp in/intermediate.processed out/published.snapshot

- id: analyze.data
  depends: [process.data]
  reads: [intermediate.processed]   # Continues chain
```

### Decision 2: Execution Archives vs Debug Folder

**Problem**: Where to store execution artifacts?

**Options Considered**:

1. `/data/linearjc/debug/` - Implies temporary/debugging
2. `/data/linearjc/exec_jobs/` - Implies execution history

**Decision**: Use `/data/linearjc/exec_jobs/`.

**Rationale**:
- Not just for debugging - powers analytics, compliance, auditing
- Better naming for what it actually is
- Leaves room for growth (replay, lineage tracking)

### Decision 3: Barrier Execution Model

**Problem**: How to handle A→C, B→C where C depends on both?

**Options Considered**:

1. **Duplicate upstream** - Run A twice, B twice for each downstream
2. **Barrier/wait** - Run A and B once, C waits for both

**Decision**: Barrier/wait model with parallel execution.

**Rationale**:
- Avoids redundant work
- Natural fit for data pipelines
- Matches mainframe scheduler semantics
- Enables true parallelism (A and B run concurrently)

### Decision 4: Scheduling Unit

**Problem**: With barrier execution, what is the scheduling unit?

**Options Considered**:

1. **Per-job schedules** - Each job has its own schedule, batch graph triggered when any root is due
2. **Per-batch schedule** - Batch group has a single schedule, all jobs inherit

**Decision**: Per-batch schedule.

**Rationale**:
- Simpler mental model
- Avoids conflicts (what if A wants 4x/day but B wants 1x/day?)
- The batch group is the logical unit of work

```yaml
# Batch-level schedule
batch:
  id: daily.close
  schedule:
    min_daily: 1
    max_daily: 4
  jobs:
    - extract.north
    - extract.south
    - consolidate
```

---

## Part 5: Comparison with Mainframe Systems

| Feature | Mainframe (CA-7/TWS) | LinearJC Current | LinearJC Proposed |
|---------|---------------------|------------------|-------------------|
| Linear chains | ✓ | ✓ | ✓ |
| Fan-out | ✓ | ✓ (Phase 15) | ✓ |
| Multi-input barriers | ✓ | ✗ | ✓ (register-driven) |
| Parallel execution | ✓ | ✗ | ✓ (independent root modules) |
| Execution history | ✓ (JES spool) | ✗ | ✓ (exec_jobs/) |
| Data lineage | ✓ (GDG) | ✗ | ✓ (ljc lineage) |
| Replay/restart | ✓ | ✗ | ✓ (ljc replay) |
| Performance tracking | ✓ (SMF) | ✗ | ✓ (index.db) |
| Conditional execution | ✓ | ✗ | Future |
| Time windows | ✓ | ✗ | Future |
| Calendars | ✓ | ✗ | Future |

---

## Implementation Roadmap

### Phase 16: Execution Archives

1. Add `exec_archive` config section
2. Implement `_archive_execution()` in coordinator
3. Create index.db schema and update logic
4. Add retention/pruning in maintenance loop
5. Add `--debug` flag to `ljc exec`

**Estimate**: 1-2 sessions

### Phase 17: Register-Driven Barrier Execution

1. Extend tree_builder to derive `BatchGraph` from register ownership
2. Add `BatchExecution` and barrier tracking
3. Implement parallel dispatch for independent root modules
4. Update coordinator message handlers
5. Update `ljc` status displays

**Estimate**: 2-3 sessions

### Phase 18: ljc Observability Commands

1. `ljc timeline` - Timeline visualization
2. `ljc analyze` - Failure analysis
3. `ljc diff` - Execution comparison
4. `ljc dashboard` - Performance overview

**Estimate**: 2 sessions

### Phase 19: Advanced Features

1. `ljc replay` - Replay from archived inputs
2. `ljc lineage` - Data lineage tracking
3. `ljc export` - Export for external tools

**Estimate**: 2 sessions

---

## Open Questions

1. **Archive storage limits**: Should we cap total archive size? Or just rely on retention_days?

2. **Partial barrier failure**: Should we fail the entire batch generation or allow independent branches to complete?

3. **Cross-batch dependencies**: Should a job in batch A be able to depend on a register produced by batch B?

4. **Real-time streaming**: Should `ljc tail` show live stdout/stderr during execution?

---

## References

- Phase 15 implementation: Fan-out support (this session)
- Mainframe comparison: JES2/JES3, CA-7, TWS/Tivoli
- Current chain execution: `src/coordinator_v2/coordinator.py`
- Tree builder: `src/coordinator/tree_builder.py`
