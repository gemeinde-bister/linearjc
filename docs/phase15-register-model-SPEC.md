# LinearJC Register Model Specification

**Version**: 0.8.0-draft
**Date**: 2026-01-13
**Status**: Phase 15 Proposal

## Overview

This specification defines a mainframe-inspired register model for LinearJC, addressing:
- Data persistence semantics (permanent vs temporary)
- Resource locking (readers-writer locks)
- Write-through caching (MinIO as L1, filesystem as registers)
- Fan-out serialization

Both coordinator and executor are single-threaded async; no mutexes or thread synchronization required.

The design is inspired by IBM mainframe concepts (JCL, DASD, ENQ/DEQ) adapted for modern distributed execution.

### Non-Goals

This specification explicitly does not address:

- **Multi-coordinator setups**: LinearJC is designed for exactly one coordinator with N executors. Distributed consensus, lock synchronization across coordinators, and split-brain scenarios are out of scope.
- **External monitoring dependencies**: The system must be self-healing without requiring Prometheus, Telegraf, or similar infrastructure. Timeouts and cleanup are handled internally.
- **Versioned registers (GDG-style)**: Generation data groups with automatic versioning are a potential future enhancement (see Appendix B) but not part of this specification.

---

## 1. Mental Model

### 1.1 CPU Analogy

```
┌─────────────────────────────────────────────────────────────┐
│                         Executor                             │
│                    (CPU executing operands)                  │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                    MinIO Temp Bucket                         │
│                      (L1 Cache)                              │
│           Fast, ephemeral, per-execution paths               │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│                      Filesystem                              │
│                     (Registers)                              │
│            Persistent, authoritative, locked                 │
└─────────────────────────────────────────────────────────────┘
```

### 1.2 Mainframe Analogy

| Mainframe Concept | LinearJC Equivalent |
|-------------------|---------------------|
| Dataset (DSN) | Registry entry |
| DASD | Filesystem (registers) |
| VIO / &&TEMP | MinIO temp (`type: temp`) |
| DISP=SHR | `reads:` (shared lock) |
| DISP=OLD | `writes:` (exclusive lock) |
| ENQ/DEQ | RegisterLock |
| JES Queue | Tree scheduling with lock waiting |
| Job Step | Job in chain |
| Catalog | Data registry |

### 1.3 JCL Comparison

```jcl
//STEP1    EXEC PGM=EXTRACT
//INPUT    DD DSN=PROD.AP.DATABASE,DISP=SHR           ← type: fs, reads
//WORK     DD DSN=&&SPENDING,DISP=(NEW,PASS)          ← type: temp, writes
//
//STEP2    EXEC PGM=PERSIST
//INPUT    DD DSN=&&SPENDING,DISP=(OLD,DELETE)        ← type: temp, reads
//OUTPUT   DD DSN=PROD.SPENDING.EXPORT,DISP=(NEW,CATLG) ← type: fs, writes
```

Equivalent LinearJC:
```yaml
# registry.yaml
registry:
  ap_database:         {type: fs, path: /data/.../ap.db, kind: file}
  spending_temp:       {type: temp, kind: file}
  spending_export:     {type: fs, path: /data/.../spending.json, kind: file}

# export.spending/job.yaml
job:
  id: export.spending
  reads: [ap_database]
  writes: [spending_temp]
  depends: []

# persist.spending/job.yaml
job:
  id: persist.spending
  reads: [spending_temp]
  writes: [spending_export]
  depends: [export.spending]
```

---

## 2. Registry Types

### 2.1 Type: fs (Filesystem Register)

Permanent storage on filesystem. Write-through semantics.

```yaml
my_register: {type: fs, path: /absolute/path, kind: file|dir}
my_input:    {type: fs, path: /absolute/path, kind: file|dir, protect: true}
```

| Field | Required | Description |
|-------|----------|-------------|
| `type` | Yes | `"fs"` |
| `path` | Yes | Absolute filesystem path |
| `kind` | Yes | `"file"` (single file) or `"dir"` (directory) |
| `protect` | No | `true` = external input, write-protected (default: `false`) |

**Behavior (protect: false, default):**
- **Write**: Job output → MinIO temp → collected to `path` (write-through)
- **Read**: Read from `path`, upload to MinIO for executor
- **Locking**: ENQ/DEQ (exclusive for writes, shared for reads)
- **Lifecycle**: Permanent (survives tree completion)
- **Initial state**: READY if file exists, EMPTY otherwise

**Behavior (protect: true):**
- **Write**: Not allowed - jobs cannot declare this in `writes:`
- **Read**: Read from `path`, upload to MinIO for executor
- **Locking**: Shared locks only (no exclusive locks possible)
- **Lifecycle**: Permanent, managed externally
- **Initial state**: Always READY (existence verified at startup)
- **Use case**: External inputs (databases, configs, data from other systems)

### 2.2 Type: temp (Temporary Register)

Temporary storage in MinIO only. Equivalent to JCL `&&TEMP`.

```yaml
my_temp: {type: temp, kind: file|dir}
```

| Field | Required | Description |
|-------|----------|-------------|
| `type` | Yes | `"temp"` |
| `kind` | Yes | `"file"` (single file) or `"dir"` (directory) |
| `path` | No | Not allowed (auto-generated in MinIO) |

**Behavior:**
- **Write**: Job output → MinIO temp (no filesystem write)
- **Read**: Read from MinIO temp (within same tree execution)
- **Locking**: None (unique MinIO path per tree_exec_id)
- **Lifecycle**: Deleted when tree completes or fails

**MinIO Path Convention:**
```
jobs/{tree_exec_id}/output_{registry_key}.tar.gz
```

### 2.3 Type: minio (Permanent MinIO Register)

Permanent storage in MinIO bucket. For large objects or cross-system sharing.

```yaml
my_object: {type: minio, bucket: my-bucket, prefix: data/, kind: file|dir}
```

| Field | Required | Description |
|-------|----------|-------------|
| `type` | Yes | `"minio"` |
| `bucket` | Yes | MinIO bucket name |
| `prefix` | No | Object prefix (default: empty) |
| `kind` | Yes | `"file"` (single object) or `"dir"` (multiple objects) |

**Behavior:**
- **Write**: Job output → MinIO temp → copied to `bucket/prefix` (uses MinIO CopyObject for same-cluster efficiency; no re-download/re-upload)
- **Read**: Read from `bucket/prefix`
- **Locking**: ENQ/DEQ (exclusive for writes, shared for reads)
- **Lifecycle**: Permanent (survives tree completion)

### 2.4 Type Comparison

| Aspect | `fs` | `fs` + `protect` | `temp` | `minio` |
|--------|------|------------------|--------|---------|
| Storage | Filesystem | Filesystem | MinIO temp | MinIO permanent |
| Persistence | Permanent | Permanent | Tree duration | Permanent |
| Locking | ENQ/DEQ | Shared only | None | ENQ/DEQ |
| Writable | Yes | **No** | Yes | Yes |
| Initial state | READY/EMPTY | **Always READY** | EMPTY | READY/EMPTY |
| Write-through | Yes | N/A | No | Yes |
| Use case | Managed outputs | **External inputs** | Intermediates | Large data |

---

## 3. Locking Model (ENQ/DEQ)

### 3.1 Lock Types

Inspired by mainframe ENQ (enqueue) mechanism:

| Lock Mode | JCL Equivalent | Compatibility |
|-----------|----------------|---------------|
| **Shared (S)** | DISP=SHR | Compatible with other Shared |
| **Exclusive (E)** | DISP=OLD | Incompatible with all |

### 3.2 Compatibility Matrix

| Held \ Requested | Shared | Exclusive |
|------------------|--------|-----------|
| **None** | Grant | Grant |
| **Shared** | Grant | Wait |
| **Exclusive** | Wait | Wait |

### 3.3 Hazard Detection

| Hazard | Scenario | Resolution |
|--------|----------|------------|
| **RAW** (Read-After-Write) | Tree B reads register X while Tree A writes X | B waits for A |
| **WAW** (Write-After-Write) | Tree B writes register X while Tree A writes X | B waits for A |
| **WAR** (Write-After-Read) | Tree B writes register X while Tree A reads X | B waits for A |

### 3.4 Lock Path Resolution

Different register types use different lock paths:

```python
def _lock_path(entry: DataRegistryEntry, registry_key: str) -> str | None:
    """
    Resolve register entry to canonical lock path.

    Returns:
        Lock path string, or None if type doesn't require locking.
    """
    if entry.type == "fs":
        return str(Path(entry.path).resolve())
    elif entry.type == "minio":
        return f"minio:{entry.bucket}/{entry.prefix or ''}"
    elif entry.type == "temp":
        return None  # Temp registers don't need locks
    else:
        raise ValueError(f"Unknown register type: {entry.type}")
```

### 3.5 Lock Ordering (Deadlock Prevention)

**Critical**: Locks must be acquired in canonical order to prevent deadlock.

**Problem scenario:**
```
Tree1: ENQ(A), ENQ(B)  ← holds A, waiting for B
Tree2: ENQ(B), ENQ(A)  ← holds B, waiting for A
→ Deadlock!
```

**Solution**: Always acquire locks in sorted order by lock path.

```python
def _sorted_lock_paths(tree: JobTree, registry: dict) -> list[tuple[str, str]]:
    """
    Extract and sort all lock paths for a tree.

    Returns:
        List of (lock_path, mode) tuples sorted by lock_path.
        Mode is "exclusive" for writes, "shared" for reads.
    """
    locks = []

    for job in tree.jobs:
        for key in job.writes:
            entry = registry.get(key)
            if entry and (path := _lock_path(entry, key)):
                locks.append((path, "exclusive"))

        for key in job.reads:
            entry = registry.get(key)
            if entry and entry.type != "temp" and (path := _lock_path(entry, key)):
                # Skip if already have exclusive (write takes precedence)
                if not any(p == path and m == "exclusive" for p, m in locks):
                    locks.append((path, "shared"))

    # Sort by path for consistent ordering
    return sorted(set(locks), key=lambda x: x[0])
```

### 3.6 Implementation

```python
class RegisterLock:
    """
    ENQ/DEQ style locking for permanent registers.

    Single-threaded (async), no mutex needed.
    """

    def __init__(self):
        # path -> LockState
        self._locks: dict[str, LockState] = {}
        # tree_exec_id -> set of held paths
        self._held_by_tree: dict[str, set[str]] = {}
        # path -> list of waiting (tree_exec_id, mode)
        self._wait_queue: dict[str, list[tuple[str, str]]] = {}

    def try_acquire_all(
        self,
        locks: list[tuple[str, str]],  # [(path, mode), ...] - must be sorted
        tree_exec_id: str
    ) -> bool:
        """
        Atomically acquire all locks or none.

        Returns:
            True if all granted (tree can dispatch)
            False if any denied (tree added to wait queue, holds NO locks)

        Critical: This is all-or-nothing. A tree waiting in queue holds
        zero locks, preventing unnecessary blocking of other trees.
        """
        # Phase 1: Check all locks (no acquisition yet)
        first_blocker: tuple[str, str] | None = None
        for path, mode in locks:
            if not self._can_grant(path, mode):
                first_blocker = (path, mode)
                break

        if first_blocker:
            # At least one lock unavailable - add to wait queue for first blocker
            path, mode = first_blocker
            self._wait_queue.setdefault(path, []).append((tree_exec_id, mode))
            return False

        # Phase 2: All available - acquire atomically
        for path, mode in locks:
            self._grant(path, mode, tree_exec_id)

        return True

    def _can_grant(self, path: str, mode: str) -> bool:
        """
        Check if lock CAN be granted (without actually granting).

        FIFO enforcement: If anyone is waiting in queue, new requests
        must wait behind them - even if the lock mode would be compatible.
        This prevents writer starvation.
        """
        # FIFO: Must wait behind anyone in queue
        if self._wait_queue.get(path):
            return False

        current = self._locks.get(path)

        if current is None:
            return True

        if mode == "shared" and current.mode == "shared":
            return True  # Compatible, no queue, can grant

        return False

    def dequeue(self, tree_exec_id: str) -> list[str]:
        """
        Release all locks held by tree.

        Returns:
            List of paths where waiters were woken
        """
        woken_paths = []
        held_paths = self._held_by_tree.pop(tree_exec_id, set())

        for path in held_paths:
            lock = self._locks.get(path)
            if lock:
                lock.holders.discard(tree_exec_id)
                if not lock.holders:
                    del self._locks[path]
                    # Process wait queue for this path
                    if self._process_wait_queue(path):
                        woken_paths.append(path)

        return woken_paths

    def _grant(self, path: str, mode: str, tree_exec_id: str):
        """Grant lock to tree."""
        current = self._locks.get(path)
        if current and mode == "shared" and current.mode == "shared":
            # Add to existing shared lock
            current.holders.add(tree_exec_id)
        else:
            # New lock
            self._locks[path] = LockState(mode=mode, holders={tree_exec_id})
        self._held_by_tree.setdefault(tree_exec_id, set()).add(path)

    def _process_wait_queue(self, path: str) -> bool:
        """
        Process waiting trees after lock release.

        FIFO with batching: Grant to consecutive compatible waiters.
        Once an exclusive waiter is encountered, stop (they get next turn).

        Returns:
            True if any waiter was woken
        """
        queue = self._wait_queue.get(path, [])
        if not queue:
            return False

        woken = []
        current = self._locks.get(path)  # None after release

        for i, (tree_exec_id, mode) in enumerate(queue):
            if current is None:
                # Lock is free - grant to first waiter
                self._grant(path, mode, tree_exec_id)
                current = self._locks[path]
                woken.append(i)
            elif mode == "shared" and current.mode == "shared":
                # Batch consecutive shared waiters
                current.holders.add(tree_exec_id)
                self._held_by_tree.setdefault(tree_exec_id, set()).add(path)
                woken.append(i)
            else:
                # Incompatible waiter - stop here (FIFO)
                break

        # Remove woken from queue (reverse to preserve indices)
        for i in reversed(woken):
            tree_exec_id, _ = queue.pop(i)
            # Notify coordinator to retry full lock acquisition for this tree
            self._notify_tree_ready(tree_exec_id)

        return len(woken) > 0


@dataclass
class LockState:
    mode: str  # "shared" or "exclusive"
    holders: set[str]  # tree_exec_ids
```

### 3.7 Lock Acquisition Flow

```
Tree wants to execute:
  1. Compute sorted lock paths (Section 3.5) from ALL jobs in tree
  2. Call try_acquire_all(sorted_locks, tree_exec_id)
     - If True: all locks granted, dispatch tree
     - If False: tree waits in queue (holds ZERO locks)
  3. When woken from queue: retry try_acquire_all() with same locks
  4. On tree completion/failure: DEQ all locks
```

**Critical invariant**: A tree waiting in queue holds zero locks. This is
enforced by `try_acquire_all()` which checks all locks before granting any.
Partial lock states are impossible.

### 3.8 Locking Scope

| Register Type | Locking Required |
|---------------|------------------|
| `type: fs` | Yes - exclusive for writes, shared for reads |
| `type: fs` + `protect` | Shared only - exclusive not allowed |
| `type: temp` | No - unique MinIO path per tree_exec_id |
| `type: minio` | Yes - exclusive for writes, shared for reads |

### 3.9 Lock Lifetime

**Lock lifetime equals tree lifetime.** Locks are acquired when a tree is dispatched and released when the tree completes (success or failure). There is no separate lease renewal mechanism.

**Lock release triggers:**
1. **Tree completion** (success): Normal path, job finished, outputs collected
2. **Tree failure** (job error): Job exited non-zero, tree marked failed
3. **Job timeout**: Job exceeded configured timeout, tree marked failed
4. **Executor death**: Executor missed heartbeats, all trees on that executor marked failed

**Executor crash detection** (separate from job timeout):
- Executor sends heartbeat every 30 seconds
- Coordinator tracks last heartbeat per executor
- After 3 missed heartbeats (90 seconds), executor presumed dead
- All trees running on dead executor: immediate lock release + cleanup
- This prevents hour-long lock holds when executor crashes early in a long job

**Coordinator restart** (clean slate):
- Lock state is purely in-memory
- Restart clears all MQTT state and MinIO temp data
- Running jobs on executors become orphaned (no coordinator to report to)
- Executors detect coordinator absence and stop gracefully
- This is simpler than lock reconstruction and acceptable for rare restart events

**SIGHUP reload** (preserves state):
- Lock state preserved, running trees continue
- Registry changes applied with Phase 5 safety checks
- See Section 11.7 for hot reload safety rules

**Design rationale:** A single-coordinator architecture with heartbeat-based crash detection and clean-slate restart is simpler and sufficient. Lease-based locking and lock persistence add complexity without proportional benefit.

---

## 4. Write-Through Caching

### 4.1 Data Flow

```
Job Output Flow (type: fs):
┌──────────┐    ┌─────────────┐    ┌────────────┐    ┌──────────┐
│ Executor │───▶│ MinIO Temp  │───▶│ Filesystem │    │ Next Job │
│          │    │ (L1 Cache)  │    │ (Register) │    │          │
└──────────┘    └─────────────┘    └────────────┘    └──────────┘
     │                │                   │                │
     │   1. Upload    │   2. Collect      │   3. Read      │
     │   (presigned)  │   (write-through) │   (cache or fs)│
     └────────────────┴───────────────────┴────────────────┘

Job Output Flow (type: temp):
┌──────────┐    ┌─────────────┐    ┌──────────┐
│ Executor │───▶│ MinIO Temp  │───▶│ Next Job │
│          │    │ (Cache only)│    │          │
└──────────┘    └─────────────┘    └──────────┘
     │                │                  │
     │   1. Upload    │   2. Read        │
     │   (presigned)  │   (direct)       │
     └────────────────┴──────────────────┘
```

### 4.2 Collection Timing

**Current behavior**: Collect only leaf job outputs
**New behavior**: Collect all `type: fs` and `type: minio` outputs after each job

```python
async def _on_job_completed(self, sm, tex):
    job = sm.job
    tree_exec_id = tex.tree_execution_id

    # Write-through: collect permanent outputs to storage
    for registry_key in job.writes:
        entry = self.data_registry.get(registry_key)

        if entry.type == "fs":
            await self._collect_to_filesystem(registry_key, entry, tree_exec_id)
            # Keep in MinIO as cache (don't delete yet)

        elif entry.type == "minio":
            await self._collect_to_minio_permanent(registry_key, entry, tree_exec_id)
            # Keep in temp as cache

        elif entry.type == "temp":
            # No collection - stays in MinIO temp only
            pass

    # Continue chain or finish
    if not self.tree_tracker.is_last_job(tree_exec_id):
        await self._continue_chain(tex, sm)
    else:
        await self._finish_tree(tex, sm)
```

### 4.3 Cache Behavior

For chain job inputs, try MinIO cache first, fallback to storage:

```python
async def _prepare_chain_inputs(self, job, prev_job, tree_exec_id):
    prepared = {}

    for registry_key in job.reads:
        entry = self.data_registry.get(registry_key)

        if registry_key in prev_job.writes:
            # Data from previous job in chain
            if entry.type == "temp":
                # Temp: always in MinIO (no fallback)
                prepared[registry_key] = self._prepare_temp_input(
                    registry_key, tree_exec_id
                )
            else:
                # Permanent: try cache, fallback to storage
                if await self._minio_temp_exists(tree_exec_id, registry_key):
                    # Cache hit
                    prepared[registry_key] = self._prepare_cached_input(
                        registry_key, tree_exec_id
                    )
                else:
                    # Cache miss - read from storage
                    prepared[registry_key] = await self._prepare_storage_input(
                        registry_key, entry, tree_exec_id
                    )
        else:
            # External input (not from chain)
            prepared[registry_key] = await self._prepare_storage_input(
                registry_key, entry, tree_exec_id
            )

    return prepared
```

### 4.4 Multi-Output Atomicity

**Problem**: Jobs with multiple `writes:` entries may partially fail during collection.

```yaml
job:
  id: multi_writer
  writes: [output_a, output_b, output_c]  # Job produces 3 outputs
```

If collection of `output_b` fails after `output_a` succeeds:
- `output_a`: new data (collected)
- `output_b`: stale or missing (failed)
- `output_c`: stale or missing (not attempted)

**Consistency guarantee**: Per-output atomic, not per-job atomic.

**Implementation**: Each output is collected atomically using temp directory + rename:

```python
async def _collect_filesystem_output_atomic(
    self, registry_key: str, entry: DataRegistryEntry, tree_exec_id: str
) -> None:
    """Atomic collection: extract to temp, rename on success."""
    dest_path = Path(entry.path)
    temp_path = dest_path.parent / f".{dest_path.name}.{tree_exec_id}.tmp"

    try:
        # Extract to temp location
        extract_archive(archive_path, str(temp_path), path_type=entry.kind)

        # Atomic replace (same filesystem required)
        # os.replace() is atomic on POSIX - overwrites dest if exists
        os.replace(temp_path, dest_path)

    except Exception:
        # Cleanup temp on failure
        if temp_path.exists():
            temp_path.unlink() if temp_path.is_file() else shutil.rmtree(temp_path)
        raise
```

**Design decision**: Per-output atomicity is sufficient because:
1. Each register is independently meaningful
2. Downstream jobs declare explicit `reads:` dependencies
3. Full job-level atomicity would require transaction logs (complexity not justified)

**Consumer responsibility**: Jobs reading multiple outputs from a failed producer
must handle the case where some outputs are stale. Use `depends:` to enforce
ordering if outputs must be consumed together.

---

## 5. Fan-Out Behavior

### 5.1 Tree Construction

Fan-out creates separate trees for each path:

```
Jobs:
  A: depends=[], writes=[X]
  B: depends=[A], writes=[Y]
  C: depends=[A], writes=[Z]

Trees created:
  Tree1: [A → B]
  Tree2: [A → C]
```

### 5.2 Locking with Fan-Out

**Case 1: Temp intermediate (parallel OK)**
```yaml
X: {type: temp, kind: file}  # A's output
Y: {type: fs, path: /y, kind: file}
Z: {type: fs, path: /z, kind: file}
```
- Tree1 and Tree2 can run in parallel
- Each gets unique MinIO path for X
- No lock conflicts (Y and Z are different)

**Case 2: Permanent intermediate (serialized)**
```yaml
X: {type: fs, path: /x, kind: file}  # A's output
Y: {type: fs, path: /y, kind: file}
Z: {type: fs, path: /z, kind: file}
```
- Tree1 starts: ENQ(/x, exclusive) → granted
- Tree2 starts: ENQ(/x, exclusive) → **wait** (conflict with Tree1)
- Tree1 completes: DEQ(/x)
- Tree2 wakes: ENQ(/x, exclusive) → granted

### 5.3 Fan-Out Decision Guide

| Intermediate Type | Parallelism | Use When |
|-------------------|-------------|----------|
| `temp` | Full parallel | Data only needed within chain |
| `fs` | Serialized | Data must persist for debugging/recovery |
| `minio` | Serialized | Large data, cross-system access |

---

## 6. Cleanup

### 6.1 Tree Completion

```python
async def _finish_tree(self, tex, sm):
    tree_exec_id = tex.tree_execution_id

    # 1. Release all locks
    released = self.register_lock.dequeue(tree_exec_id)
    logger.info(f"Released {len(released)} locks for {tree_exec_id}")

    # 2. Clean up MinIO temp objects (including cache)
    await self._cleanup_minio_temp(tree_exec_id)

    # 3. Clean up work directory
    await self._cleanup_work_dir(tree_exec_id)

    # 4. Mark tree completed
    self.tree_tracker.mark_completed(tree_exec_id)
```

### 6.2 Tree Failure

Same as completion, but also:
- Log failure reason
- Notify dev client (if attached)
- Permanent outputs may be partially written (application must handle)

---

## 7. Schema Changes

### 7.1 DataRegistryEntry Model

```python
class DataRegistryEntry(BaseModel):
    """Entry in the data registry."""

    type: Literal["fs", "temp", "minio"]

    # For type: fs
    path: Optional[str] = None  # Required for fs
    protect: bool = False  # If True, register is write-protected (external input)

    # For type: minio
    bucket: Optional[str] = None  # Required for minio
    prefix: Optional[str] = None  # Optional for minio

    # For all types
    kind: Literal["file", "dir"]  # Required for all

    @model_validator(mode="after")
    def validate_type_fields(self) -> "DataRegistryEntry":
        if self.type == "fs":
            if not self.path:
                raise ValueError("path required for type: fs")
        elif self.type == "minio":
            if not self.bucket:
                raise ValueError("bucket required for type: minio")
            if self.protect:
                raise ValueError("protect not supported for type: minio")
            # Validate bucket name (S3/MinIO naming rules)
            if not re.match(r'^[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]$', self.bucket):
                raise ValueError(
                    f"Invalid bucket name: {self.bucket}. "
                    "Must be 3-63 chars, lowercase alphanumeric, dots, hyphens."
                )
            # Validate prefix for path traversal
            if self.prefix:
                if '..' in self.prefix:
                    raise ValueError(f"Path traversal in prefix: {self.prefix}")
                if self.prefix.startswith('/'):
                    raise ValueError(f"Prefix must not start with /: {self.prefix}")
        elif self.type == "temp":
            if self.path:
                raise ValueError("path not allowed for type: temp")
            if self.bucket:
                raise ValueError("bucket not allowed for type: temp")
            if self.protect:
                raise ValueError("protect not allowed for type: temp (always writable)")
        return self
```

### 7.2 Job Validation

```python
def validate_job_writes(job: Job, registry: Dict[str, DataRegistryEntry]):
    """Validate that job doesn't write to protected registers."""
    for key in job.writes:
        entry = registry.get(key)
        if not entry:
            raise ValidationError(f"Unknown register: {key}")
        if entry.protect:
            raise ValidationError(
                f"Job '{job.id}' cannot write to protected register '{key}'"
            )
```

### 7.3 Startup Validation

```python
def validate_protected_registers(registry: Dict[str, DataRegistryEntry]):
    """Verify all protected registers exist at startup."""
    for key, entry in registry.items():
        if entry.protect:
            if not Path(entry.path).exists():
                raise StartupError(
                    f"Protected register '{key}' not found: {entry.path}"
                )
```

### 7.4 Registry YAML Format

```yaml
registry:
  # Protected filesystem input (external, always READY)
  source_db: {type: fs, path: /data/app/data.db, kind: file, protect: true}
  config_dir: {type: fs, path: /data/config/, kind: dir, protect: true}

  # Managed filesystem output (written by jobs)
  my_data: {type: fs, path: /data/data/output.json, kind: file}
  my_dir:  {type: fs, path: /data/data/outputs/, kind: dir}

  # Temporary register (chain-only, MinIO)
  my_temp: {type: temp, kind: file}

  # MinIO register (permanent, large data)
  my_object: {type: minio, bucket: data-bucket, prefix: outputs/, kind: dir}
```

---

## 8. Migration Guide

### 8.1 Existing Jobs

Existing jobs with `type: fs` continue to work:
- Outputs now collected after each job (not just leaf)
- Locking now applies to all permanent registers
- No job.yaml changes required

### 8.2 Converting Intermediates

For better performance, convert intermediate-only outputs to `type: temp`:

**Before:**
```yaml
registry:
  intermediate: {type: fs, path: /data/intermediate.json, kind: file}
```

**After:**
```yaml
registry:
  intermediate: {type: temp, kind: file}
```

### 8.3 Fan-Out Optimization

If fan-out trees are serializing unnecessarily:

1. Check if shared output is truly needed on disk
2. If not, convert to `type: temp`
3. Add explicit persist job if persistence is needed later

---

## 9. Implementation Checklist

### Phase 1: Schema
- [ ] Add `type: temp` to DataRegistryEntry validator
- [ ] Add `type: minio` to DataRegistryEntry validator
- [ ] Add `protect: bool` field to DataRegistryEntry
- [ ] Add `bucket` and `prefix` fields for minio type
- [ ] Update registry YAML parser
- [ ] Add validation: jobs cannot write to protected registers
- [ ] Add startup validation: protected registers must exist
- [ ] Add validation tests

### Phase 2: Locking (RegisterLock)
- [ ] Implement RegisterLock class with `try_acquire_all()` (all-or-nothing)
- [ ] Implement `_can_grant()` with FIFO queue check
- [ ] Implement `_process_wait_queue()` with shared batching
- [ ] Implement `dequeue()` for lock release
- [ ] Extract locks from ALL jobs in tree (not just leaf)
- [ ] Skip locking for `type: temp` registers
- [ ] Skip write lock for `protect: true` registers (shared only)
- [ ] Add locking unit tests
- [ ] Add integration tests (multi-tree scenarios)

### Phase 3: Write-Through Collection
- [ ] Modify _on_job_completed to collect after EACH job (not just leaf)
- [ ] Only collect `type: fs` and `type: minio` outputs
- [ ] Skip collection for `type: temp` (stays in MinIO)
- [ ] Implement atomic collection (temp dir + rename)
- [ ] Implement cache-aware _prepare_chain_inputs (try MinIO first)
- [ ] Keep MinIO data as cache until tree ends
- [ ] Add write-through tests

### Phase 4: Executor Crash Detection
- [ ] Track last heartbeat per executor
- [ ] Implement heartbeat monitor loop (30s interval)
- [ ] Detect dead executor after 3 missed heartbeats (90s)
- [ ] On executor death: mark all trees failed, release locks, cleanup MinIO temp
- [ ] On collection failure: mark tree failed, release locks, cleanup partial
- [ ] Add failure handling tests

Note: Coordinator restart is clean slate (see Section 3.9, 11.1). No lock reconstruction needed.

### Phase 5: Hot Reload Safety
- [ ] Warn on register path changes while trees active
- [ ] Block removal of registers with active trees
- [ ] Block adding `protect: true` to registers with active writes
- [ ] Block removing `protect: true` from registers with active shared locks
- [ ] Update TreeValidator.update_registry() for safety checks

### Phase 6: Cleanup
- [ ] Update cleanup to handle new lock release
- [ ] Ensure temp data cleaned on tree completion
- [ ] Orphan cleanup for failed collections
- [ ] Add cleanup tests

### Phase 7: Documentation
- [ ] Update SPEC.md with new types and flags
- [ ] Update job-design-guide.md
- [ ] Add migration guide
- [ ] Update examples

### Phase 8: Execution Archives (Optional)
See Appendix C for full specification. This phase enables debugging and replay.
- [ ] Create archive directory structure
- [ ] Implement metadata.json generation
- [ ] Implement timeline.json event collection
- [ ] Create index.db schema and update logic
- [ ] Add archive configuration to coordinator config
- [ ] Implement artifact collection (inputs/outputs)
- [ ] Implement stdout/stderr capture
- [ ] Add retention/pruning to maintenance loop
- [ ] Implement `ljc history` command
- [ ] Implement `ljc timeline` command
- [ ] Implement `ljc analyze` command
- [ ] Implement `ljc replay` command

---

## 10. Example: Full Chain with Mixed Types

```yaml
# registry.yaml
registry:
  # External input (protected - always READY, cannot be written)
  source_db: {type: fs, path: /data/app/data.db, kind: file, protect: true}

  # Intermediate (temp - fast, no persistence, MinIO only)
  extracted_data: {type: temp, kind: file}

  # Final output (managed - write-through to disk)
  report: {type: fs, path: /data/reports/daily.json, kind: file}

# jobs/extract/job.yaml
job:
  id: extract
  reads: [source_db]      # Protected input - always available
  writes: [extracted_data] # Temp output - MinIO only
  depends: []

# jobs/transform/job.yaml
job:
  id: transform
  reads: [extracted_data]  # Temp input - from MinIO
  writes: [report]         # Managed output - write-through to fs
  depends: [extract]
```

**Execution flow:**

```
1. Startup:
   - source_db: protect=true → verify exists → state=READY
   - extracted_data: type=temp → state=EMPTY
   - report: type=fs → check exists → state=EMPTY (or READY if exists)

2. Tree [extract → transform] scheduled

3. Lock acquisition:
   - source_db: protect=true → shared lock only, always granted
   - report: ENQ(/data/reports/daily.json, exclusive) → granted
   - extracted_data: type=temp → no lock needed

4. extract job:
   - Input: source_db read from fs (READY), uploaded to MinIO
   - Execute: extract data
   - Output: uploaded to MinIO temp (no fs write - temp type)
   - State: extracted_data → READY (in MinIO)

5. transform job:
   - Input: extracted_data from MinIO temp (cache hit)
   - Execute: transform data
   - Output: uploaded to MinIO temp, then collected to fs (write-through)
   - State: report → READY (on disk)

6. Tree completion:
   - DEQ all locks
   - Delete MinIO temp objects (including extracted_data)
   - report.json now on disk at /data/reports/daily.json
```

**Validation at job load time:**
```
✓ extract reads [source_db] - exists, protect=true OK
✓ extract writes [extracted_data] - type=temp, not protected OK
✓ transform reads [extracted_data] - exists OK
✓ transform writes [report] - type=fs, not protected OK

✗ If any job declared writes: [source_db] → ValidationError
  "Job 'xxx' cannot write to protected register 'source_db'"
```

---

## 11. Edge Cases and Failure Modes

### 11.1 Coordinator Restart

**Behavior**: Clean slate. Restart clears all transient state.

**What gets cleared**:
- All in-memory lock state
- All MQTT subscriptions and retained messages
- All MinIO temp bucket contents (`linearjc-temp/jobs/*`)

**Executor behavior on coordinator restart**:
- Executors detect coordinator absence (MQTT disconnect or missing heartbeat response)
- Running jobs are abandoned (no one to report completion to)
- Executors reconnect when coordinator returns
- Executors re-register capabilities on reconnect

**Why this is acceptable**:
- Coordinator restarts are rare (upgrades, crashes)
- Clean slate is simpler than lock reconstruction
- No risk of stale lock state causing deadlocks
- Jobs that were running will be rescheduled on next trigger

**Contrast with SIGHUP reload**: Reload preserves all state, only updates registry.
See Section 3.9 for details.

### 11.2 Partial Output Collection

**Problem**: Job writes outputs [A, B, C]. Collection of B fails after A succeeds.

**Current state**:
- A: on disk (collected)
- B: in MinIO only (collection failed)
- C: in MinIO only (not attempted)
- Locks: released (tree marked failed)

**Risk**: Register A has new data, B and C have stale data. Inconsistent state.

**Mitigation options**:
1. **Atomic collection**: Collect to temp dir, rename on success
2. **All-or-nothing**: On any failure, delete collected outputs
3. **Versioned registers**: Each write creates new version, old version preserved

**Recommended**: Option 1 (atomic via temp dir + rename)

### 11.3 Executor Crash

**Problem**: Executor dies mid-job. No "failed" message sent.

**Detection**: Heartbeat-based (see Section 3.9).
- Executor heartbeat: every 30 seconds
- Death threshold: 3 missed heartbeats (90 seconds)
- Detection is independent of job timeout

**Response**:
1. All trees running on dead executor marked failed
2. All locks held by those trees immediately released
3. MinIO temp data for those trees cleaned up
4. Waiting trees can now proceed

**Result**: Lock hold time on executor crash is ~90 seconds, not hours.

### 11.4 Protected Register Modification

**Problem**: External process modifies `protect: true` register while jobs reading.

**Spec position**: Protected registers are externally managed. LinearJC cannot prevent external modification.

**User responsibility**:
- Don't modify protected registers while LinearJC trees are running
- Or accept that jobs may see inconsistent data

**Optional enhancement**: Checksum validation (record checksum at tree start, verify at job dispatch)

### 11.5 Lock Starvation

**Problem**: Continuous readers prevent writer from ever acquiring lock.

**Scenario**:
```
Time 0: Tree1 holds shared lock (reading)
Time 1: Tree2 wants exclusive (writing) - waits
Time 2: Tree3 wants shared (reading) - granted? or wait behind Tree2?
```

**Policy options**:
1. **Readers-preference**: New readers granted even with waiting writer (starvation risk)
2. **Writers-preference**: No new readers while writer waiting (readers blocked)
3. **FIFO**: Strict queue order, no preference

**Recommended**: Option 3 (FIFO) - fair, predictable, matches mainframe ENQ default

### 11.6 temp Register Uniqueness

**Guarantee**: Each tree execution gets unique MinIO paths.

**Path format**: `jobs/{tree_exec_id}/output_{registry_key}.tar.gz`

**Fan-out safety**:
- Tree1 (exec-001): `jobs/exec-001/output_temp_x.tar.gz`
- Tree2 (exec-002): `jobs/exec-002/output_temp_x.tar.gz`
- Different paths, no conflict

### 11.7 Registry Hot Reload

**Supported changes**:
- Add new register: OK
- Modify register path: Dangerous if trees running (locks on old path)
- Remove register: Fails jobs referencing it
- Add `protect: true`: Fails any running writes
- Remove `protect: true`: Dangerous if shared locks held (readers assume immutable)

**Recommendation**:
- Warn on path changes while trees active
- Block removal of registers with running trees
- Block adding `protect: true` for registers with active writes
- Block removing `protect: true` for registers with active shared locks

### 11.8 Write-Through Failure Recovery

**Scenario**: Job completes, MinIO upload succeeds, fs collection fails.

**State after failure**:
- MinIO: has data (not cleaned yet)
- Filesystem: stale or missing
- Register state: should be EMPTY (write failed)
- Locks: released

**Recovery**: Next tree execution will re-run job, overwrite MinIO, retry collection.

**Data in MinIO**: Cleaned by periodic maintenance (orphan detection).

---

## Appendix A: Future Evolution - Register-Driven Barrier Scheduling

### A.1 Current Model (Explicit Dependencies)

Jobs declare explicit dependencies via `depends:` field:
```yaml
job_b:
  depends: [job_a]  # Explicit: "run after job_a"
```

Trees are built from the explicit dependency graph. Multi-input barrier jobs are not yet supported.

### A.2 Future Model (Register-Driven Scheduling)

Dependencies become **implicit from register ownership**:
```yaml
job_a: {reads: [], writes: [X]}
job_b: {reads: [X], writes: [Y]}  # Implicitly depends on job_a
```

A job is ready when ALL input registers for the current batch generation are READY.

### A.3 Register States

```python
class RegisterState(Enum):
    EMPTY = "empty"      # No data, jobs reading this must wait
    WRITING = "writing"  # Write in progress, all access blocked
    READY = "ready"      # Data available, reads allowed
```

### A.4 Scheduling Algorithm

```python
def is_job_ready(job: Job) -> bool:
    """Job can run when ALL input registers are READY."""
    for key in job.reads:
        entry = registry[key]
        if entry.protect:
            continue  # Protected registers always READY
        if register_states[key] != READY:
            return False
    return True

async def schedule():
    for job in all_jobs:
        if is_job_ready(job) and not already_running(job):
            if can_acquire_write_locks(job):
                for key in job.writes:
                    register_states[key] = WRITING
                dispatch(job)

async def on_job_completed(job):
    for key in job.writes:
        register_states[key] = READY
    # Newly ready jobs discovered on next schedule() call
```

### A.5 Multi-Input Barrier Support

With register-driven scheduling, multi-input barriers work naturally:

```yaml
registry:
  source:   {type: fs, path: /data/source.db, kind: file, protect: true}
  data_a:   {type: temp, kind: file}
  data_b:   {type: temp, kind: file}
  merged:   {type: fs, path: /data/merged.json, kind: file}

jobs:
  extract_a: {reads: [source], writes: [data_a]}
  extract_b: {reads: [source], writes: [data_b]}
  build:     {reads: [data_a, data_b], writes: [merged]}  # Barrier job
```

**Execution flow:**
```
1. Initial state:
   source = READY (protect: true)
   data_a = EMPTY
   data_b = EMPTY
   merged = EMPTY

2. Schedule:
   extract_a ready? source=READY ✓ → dispatch
   extract_b ready? source=READY ✓ → dispatch (parallel!)
   build ready? data_a=EMPTY ✗ → wait

3. extract_a completes → data_a = READY
   build ready? data_a=READY, data_b=EMPTY ✗ → wait

4. extract_b completes → data_b = READY
   build ready? data_a=READY, data_b=READY ✓ → dispatch!
```

### A.6 Benefits of Register-Driven Barrier Model

| Aspect | Explicit task graph | Register-driven barrier |
|--------|--------------|----------|
| Multi-input barriers | Not supported | Natural |
| Dependencies | Declared in `depends:` | Implicit from `reads:`/`writes:` |
| Scheduling | Topological sort | Simple readiness check |
| Parallelism | Graph analysis | Automatic from ready registers |
| Mental model | "Job A before Job B" | "Job needs registers X and Y" |

### A.7 Migration Path

The register-driven barrier model is **backward compatible**:
- `depends:` can still be used for explicit ordering
- Existing jobs work unchanged
- Protected registers (`protect: true`) are always READY
- Managed registers start EMPTY or READY (if file exists)

### A.8 Implementation Considerations

1. **Scheduling frequency**: Event-driven (on job completion) vs polling
2. **Concurrent writes**: Same register can only have one writer at a time
3. **Re-execution**: When a register is re-written, should dependents re-run?
4. **Versioning**: Optional generation numbers for registers (like GDG)

---

## Appendix B: Mainframe Reference

### JCL DISP Parameter

```
DISP=(status,normal-disp,abnormal-disp)

Status:
  NEW - Create new dataset
  OLD - Exclusive access to existing
  SHR - Shared access to existing
  MOD - Append to existing

Normal/Abnormal Disposition:
  DELETE - Delete dataset
  KEEP   - Keep dataset
  PASS   - Pass to next step
  CATLG  - Catalog dataset
  UNCATLG - Uncatalog dataset
```

### ENQ/DEQ Macro

```asm
ENQ (QNAME,RNAME,E,length,SYSTEM)   Request exclusive
ENQ (QNAME,RNAME,S,length,SYSTEM)   Request shared
DEQ (QNAME,RNAME,length,SYSTEM)     Release
```

### GDG (Generation Data Group)

```jcl
//OUTPUT DD DSN=MY.DATA(+1),DISP=(NEW,CATLG)  Create new generation
//INPUT  DD DSN=MY.DATA(0),DISP=SHR           Read current generation
//OLD    DD DSN=MY.DATA(-1),DISP=SHR          Read previous generation
```

Future LinearJC enhancement: versioned registers with generation support.

---

## Appendix C: Execution Archives

Complementary to the register model - provides execution history, debugging, and observability.

### C.1 Overview

Execution archives store complete artifacts for each tree execution:
- Job inputs/outputs at time of execution
- stdout/stderr logs
- Timing and metrics
- Event timeline

**Relationship to Register Model:**
- Registers are **authoritative** (current state of data)
- Archives are **historical** (snapshot at execution time)
- Archives created AFTER write-through collection completes

### C.2 Archive Structure

```
/data/linearjc/exec_archive/
├── index.db                           # SQLite index for fast queries
└── {tree_exec_id}/
    ├── metadata.json                  # Execution summary
    ├── timeline.json                  # Event stream
    └── jobs/
        └── {job_id}/
            ├── job.yaml               # Job definition snapshot
            ├── inputs/                # Archived inputs
            │   └── {register_key}.tar.gz
            ├── outputs/               # Archived outputs
            │   └── {register_key}.tar.gz
            ├── stdout.log             # Job stdout
            ├── stderr.log             # Job stderr
            └── metrics.json           # Timing, sizes, exit code
```

### C.3 metadata.json Schema

```json
{
  "tree_exec_id": "daily.report-20260112-143052-a1b2c3",
  "chain_id": "daily.report",
  "status": "completed",
  "trigger": "scheduled",
  "started_at": "2026-01-12T14:30:52Z",
  "completed_at": "2026-01-12T14:35:18Z",
  "duration_ms": 266000,
  "jobs": [
    {
      "job_id": "extract.data",
      "version": "1.2.0",
      "executor": "executor-host-executor-01",
      "status": "completed",
      "started_at": "2026-01-12T14:30:53Z",
      "completed_at": "2026-01-12T14:32:15Z",
      "duration_ms": 82000,
      "exit_code": 0,
      "input_bytes": 12582912,
      "output_bytes": 2097152
    }
  ],
  "registers_written": ["spending_export", "daily_report"],
  "registers_read": ["ap_database", "config"]
}
```

### C.4 timeline.json Schema

```json
{
  "events": [
    {"ts": "2026-01-12T14:30:52.001Z", "type": "tree_started"},
    {"ts": "2026-01-12T14:30:52.010Z", "type": "lock_acquired", "register": "spending_export", "mode": "exclusive"},
    {"ts": "2026-01-12T14:30:52.015Z", "type": "lock_acquired", "register": "ap_database", "mode": "shared"},
    {"ts": "2026-01-12T14:30:52.050Z", "type": "job_queued", "job_id": "extract.data"},
    {"ts": "2026-01-12T14:30:52.100Z", "type": "job_dispatched", "job_id": "extract.data", "executor": "executor-host"},
    {"ts": "2026-01-12T14:30:53.200Z", "type": "job_started", "job_id": "extract.data"},
    {"ts": "2026-01-12T14:32:15.500Z", "type": "job_completed", "job_id": "extract.data", "exit_code": 0},
    {"ts": "2026-01-12T14:32:15.600Z", "type": "output_collected", "job_id": "extract.data", "register": "spending_temp", "type": "temp"},
    {"ts": "2026-01-12T14:35:18.000Z", "type": "output_collected", "job_id": "persist.data", "register": "spending_export", "type": "fs"},
    {"ts": "2026-01-12T14:35:18.100Z", "type": "lock_released", "register": "spending_export"},
    {"ts": "2026-01-12T14:35:18.150Z", "type": "tree_completed"}
  ]
}
```

### C.5 index.db Schema (SQLite)

```sql
CREATE TABLE executions (
    tree_exec_id TEXT PRIMARY KEY,
    chain_id TEXT NOT NULL,
    status TEXT NOT NULL,           -- completed, failed, timeout
    trigger TEXT NOT NULL,          -- scheduled, manual, replay
    started_at TEXT NOT NULL,
    completed_at TEXT,
    duration_ms INTEGER,
    job_count INTEGER,
    archive_path TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE job_executions (
    job_exec_id TEXT PRIMARY KEY,
    tree_exec_id TEXT NOT NULL REFERENCES executions(tree_exec_id),
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

CREATE TABLE register_access (
    tree_exec_id TEXT NOT NULL REFERENCES executions(tree_exec_id),
    register_key TEXT NOT NULL,
    access_mode TEXT NOT NULL,      -- read, write
    register_type TEXT NOT NULL,    -- fs, temp, minio
    job_id TEXT,
    bytes INTEGER
);

-- Indexes for common queries
CREATE INDEX idx_exec_chain ON executions(chain_id);
CREATE INDEX idx_exec_status ON executions(status);
CREATE INDEX idx_exec_started ON executions(started_at DESC);
CREATE INDEX idx_job_tree ON job_executions(tree_exec_id);
CREATE INDEX idx_job_id ON job_executions(job_id);
CREATE INDEX idx_register_key ON register_access(register_key);
```

### C.6 Configuration

```yaml
coordinator:
  exec_archive:
    enabled: true
    path: /data/linearjc/exec_archive
    retention_days: 30                 # Auto-prune after 30 days
    archive_on_failure: true           # Always archive failed executions
    archive_on_success: false          # Only archive if --archive flag
    max_artifact_size_mb: 100          # Skip archiving huge outputs
    include_inputs: true               # Archive input data
    include_outputs: true              # Archive output data
    include_temp: false                # Skip temp registers (MinIO only)
```

### C.7 Archive Timing

Archives are created **after** successful write-through collection:

```python
async def _finish_tree(self, tree_exec):
    # 1. Collect all outputs (write-through)
    for job in tree_exec.completed_jobs:
        await self._collect_outputs(job)

    # 2. Release locks
    self.register_lock.dequeue(tree_exec.id)

    # 3. Archive (before MinIO cleanup)
    if self._should_archive(tree_exec):
        await self._archive_execution(tree_exec)

    # 4. Cleanup MinIO temp data
    await self._cleanup_minio_temp(tree_exec.id)
```

### C.8 ljc CLI Commands

#### `ljc history` - List Executions

```bash
$ ljc history daily.report --last 7

  TREE EXEC ID                          STATUS     DURATION  STARTED
  ──────────────────────────────────────────────────────────────────
  daily.report-20260112-143052-a1b2  completed  4m 26s    Jan 12 14:30
  daily.report-20260111-140000-x9y8  completed  4m 12s    Jan 11 14:00
  daily.report-20260110-140000-m3n4  failed     1m 02s    Jan 10 14:00
```

#### `ljc timeline` - Execution Timeline

```bash
$ ljc timeline daily.report-20260112-143052-a1b2

  daily.report | 2026-01-12 14:30:52 | 4m 26s | completed
  ════════════════════════════════════════════════════════

  14:30:52 ┬─────────────────────────────────────────────
           │ extract.data     [████████████████░░░░]  1m 22s  ✓
  14:32:15 ┼─────────────────────────────────────────────
           │ transform.data   [██████████████████░░]  1m 45s  ✓
  14:34:00 ┼─────────────────────────────────────────────
           │ persist.data     [████████████████████]  1m 18s  ✓
  14:35:18 ┴─────────────────────────────────────────────

  Registers written: spending_export (2.1 MB)
  Registers read: ap_database (12.4 MB)
```

#### `ljc analyze` - Failure Analysis

```bash
$ ljc analyze daily.report-20260110-140000-m3n4

  Failure Analysis
  ════════════════════════════════════════════════════════

  FAILED: transform.data after 1m 02s (exit code 1)

  LAST SUCCESS: daily.report-20260109-140000 (yesterday)

  STDERR (last 10 lines):
  > ValidationError: NULL timestamp at row 12847
  > FATAL: 23 validation errors, threshold is 10

  ARCHIVED INPUTS:
  > extract.data output: /data/linearjc/exec_archive/.../jobs/extract.data/outputs/

  DIFF from last success:
  > Input size: 12.4 MB → 14.1 MB (+14%)
  > New records with NULL 'timestamp': 23
```

#### `ljc replay` - Replay from Archive

```bash
$ ljc replay daily.report-20260110-140000-m3n4 --from transform.data

  Replaying from transform.data using archived inputs...

  ✓ Restored: extracted_data (2.1 MB) from archive

  transform.data    [████████████████████████] 1m 45s  ✓
  persist.data      [████████████████████████] 1m 18s  ✓

  Replay completed successfully.
  New execution: daily.report-20260112-160000-replay
```

### C.9 Retention & Cleanup

```python
async def _prune_old_archives(self):
    """Remove archives older than retention_days."""
    config = self.config.exec_archive
    cutoff = datetime.now() - timedelta(days=config.retention_days)

    conn = sqlite3.connect(f"{config.path}/index.db")

    # Find old executions
    old_execs = conn.execute("""
        SELECT tree_exec_id, archive_path FROM executions
        WHERE created_at < ? AND status = 'completed'
    """, (cutoff.isoformat(),)).fetchall()

    # Keep failed executions longer (2x retention)
    failed_cutoff = datetime.now() - timedelta(days=config.retention_days * 2)
    old_failed = conn.execute("""
        SELECT tree_exec_id, archive_path FROM executions
        WHERE created_at < ? AND status = 'failed'
    """, (failed_cutoff.isoformat(),)).fetchall()

    for exec_id, archive_path in old_execs + old_failed:
        if Path(archive_path).exists():
            shutil.rmtree(archive_path)
        conn.execute("DELETE FROM executions WHERE tree_exec_id = ?", (exec_id,))
        conn.execute("DELETE FROM job_executions WHERE tree_exec_id = ?", (exec_id,))
        conn.execute("DELETE FROM register_access WHERE tree_exec_id = ?", (exec_id,))

    conn.commit()
    conn.close()

    if old_execs or old_failed:
        logger.info(f"Pruned {len(old_execs) + len(old_failed)} old archives")
```

### C.10 Integration with Register Model

| Register Type | Archived? | Notes |
|---------------|-----------|-------|
| `fs` | Yes (if include_outputs) | Authoritative copy in register, archive is snapshot |
| `fs` + `protect` | Yes (if include_inputs) | External input, archive preserves what was read |
| `temp` | Optional (include_temp) | Usually skip - only useful for debugging |
| `minio` | Yes (if include_outputs) | Download from MinIO before cleanup |

**Key principle**: Archives are snapshots for debugging/auditing. Registers are authoritative current state.

### C.11 Implementation Checklist

See **Section 9: Phase 8** for the integrated implementation checklist.

This appendix provides the detailed specification; the checklist is maintained
in the main Implementation Checklist section for single-source tracking.

---

## Glossary

| Term | Definition |
|------|------------|
| **Chain** | A linear sequence of jobs where each job depends on the previous one. Example: `[extract → transform → persist]`. |
| **chain_id** | Identifier for a chain, typically the root job's ID. Used to group related tree executions in archives. |
| **Coordinator** | The single central process that schedules jobs, manages locks, and orchestrates executors. LinearJC has exactly one coordinator. |
| **DEQ (Dequeue)** | Release a lock on a register. Mainframe-derived term. Called when a tree completes or fails. |
| **ENQ (Enqueue)** | Request a lock on a register. Mainframe-derived term. Blocks if lock unavailable. |
| **Executor** | A worker process that runs jobs. Multiple executors connect to the single coordinator. |
| **Barrier job** | Future multi-input job that waits until all input registers for the same batch generation are ready. |
| **Fan-out** | A single job feeding into multiple downstream jobs. Creates separate trees: `[A→B]` and `[A→C]`. |
| **Job** | A unit of work with defined inputs (`reads:`), outputs (`writes:`), and dependencies (`depends:`). |
| **L1 Cache** | MinIO temp bucket, used as fast intermediate storage before write-through to filesystem. |
| **Lock path** | Canonical identifier for a register used in locking. Filesystem path for `fs`, `minio:bucket/prefix` for `minio`. |
| **Protected register** | A register with `protect: true`. External input that jobs can read but not write. |
| **Register** | A named data location in the registry. Can be filesystem path, MinIO object, or temporary storage. |
| **Registry** | The `registry.yaml` file defining all registers (data locations) available to jobs. |
| **Tree** | An execution instance of a chain. Has a unique `tree_exec_id`. Holds locks for its duration. |
| **tree_exec_id** | Unique identifier for a tree execution. Format: `{chain_id}-{YYYYMMDD}-{HHMMSS}-{random}`. Example: `daily.report-20260112-143052-a1b2`. |
| **Write-through** | Data written to L1 cache (MinIO) is immediately persisted to authoritative storage (filesystem). |
