# Register Model Implementation Tracker

**Spec**: [phase15-register-model-SPEC.md](./phase15-register-model-SPEC.md)
**Started**: 2026-01-13
**Status**: Phase 7 Complete

---

## Implementation Phases

### Phase 1: Schema ✅ COMPLETE (2026-01-13)
- [x] Add `type: temp` to DataRegistryEntry validator
- [x] Add `type: minio` to DataRegistryEntry validator (already existed, enhanced validation)
- [x] Add `protect: bool` field to DataRegistryEntry
- [x] Add `bucket` and `prefix` fields for minio type (already existed, added prefix validation)
- [x] Update registry YAML parser (load_data_registry handles temp type)
- [x] Add validation: jobs cannot write to protected registers
- [x] Add startup validation: protected registers must exist
- [x] Add validation tests (35 model tests + 15 register model tests)

**Files Changed**:
- `src/coordinator/models.py` - DataRegistryEntry with Literal types, protect field, model_validator
- `src/coordinator/security_utils.py` - validate_job_writes_against_registry(), validate_protected_registers_exist()
- `src/coordinator_v2/coordinator.py` - load_data_registry() temp support, validation calls in _initialize() and _hot_reload()
- `tests/unit/test_models.py` - Updated for new model (35 tests)
- `tests/unit/test_register_model.py` - New file for Phase 15 validation (15 tests)
- `tests/unit/test_scheduler.py` - Fixed minio test to include required `kind`

**Design Decisions**:
- Used `Literal["fs", "temp", "minio"]` for type field (cleaner than field_validator)
- Used `Literal["file", "dir"]` for kind field (now required for all types, not just fs)
- Used `model_validator(mode="after")` for cross-field validation
- Added S3 bucket naming validation (3-63 chars, lowercase alphanumeric, dots, hyphens)
- Added prefix validation (no `..` path traversal, no leading `/`)

**Compatibility Notes**:
- Breaking: `kind` is now required for all types (was only required for fs)
- Non-breaking: `protect` defaults to `False`
- Registry YAML must be updated to include `kind` for minio entries

**Documentation Updated**:
- `docs/job-design-guide.md` - Updated registry format, job definition, examples for Phase 15


### Phase 2: Locking (RegisterLock) ✅ COMPLETE (2026-01-13)
- [x] Implement RegisterLock class with `try_acquire_all()` (all-or-nothing)
- [x] Implement `_can_grant()` with FIFO queue check
- [x] Implement `_process_wait_queue()` with shared batching
- [x] Implement `dequeue()` for lock release
- [x] Extract locks from ALL jobs in tree (not just leaf) - `sorted_lock_paths()`
- [x] Skip locking for `type: temp` registers - returns None from `lock_path()`
- [x] Protected registers cannot be written to - raises ValueError in `sorted_lock_paths()`
- [x] Add locking unit tests (45 tests)
- [x] Integrate with coordinator execution flow

**Files Changed**:
- `src/coordinator_v2/register_lock.py` - New file with RegisterLock, LockState, LockRequest, lock_path(), sorted_lock_paths()
- `src/coordinator_v2/coordinator.py` - Import RegisterLock, init in __init__ and _initialize, acquire in _execute_tree, release in _collect_outputs, _handle_chain_failure, _maintenance_loop
- `tests/unit/test_register_lock.py` - New file with 45 unit tests

**Design Decisions**:
- Lock acquisition happens AFTER finding executor (consistent with TreeValidator pattern)
- TreeValidator kept as defense-in-depth (RegisterLock is primary mechanism)
- No wake callback used - scheduler loop naturally retries blocked trees
- Sorted lock ordering for deadlock prevention (canonical path ordering)
- Shared mode for reads, exclusive mode for writes

**Key Functions**:
- `lock_path(entry, key)` - Resolves DataRegistryEntry to canonical lock path
  - fs: absolute filesystem path
  - minio: `minio:{bucket}/{prefix}`
  - temp: None (no locking needed)
- `sorted_lock_paths(tree, registry)` - Extracts all locks from all jobs in tree, sorted
- `RegisterLock.try_acquire_all(locks, tree_exec_id)` - All-or-nothing acquisition
- `RegisterLock.dequeue(tree_exec_id)` - Releases all locks, wakes waiters

**Notes**:


### Phase 3: Write-Through Collection ✅ COMPLETE (2026-01-13)
- [x] Modify `_on_progress` to collect after EACH job (not just leaf)
- [x] Only collect `type: fs` and `type: minio` outputs
- [x] Skip collection for `type: temp` (stays in MinIO)
- [x] Implement atomic collection (temp dir + os.replace)
- [x] Implement cache-aware `_prepare_chain_inputs` (try MinIO first, fallback to storage)
- [x] Keep MinIO data as cache until tree ends
- [x] Add write-through tests (16 tests)

**Files Changed**:
- `src/coordinator/minio_manager.py` - Added `copy_object()` and `object_exists()` methods
- `src/coordinator_v2/coordinator.py` - Added `_collect_job_outputs()`, `_collect_minio_output()`, `_minio_temp_exists()`, `_find_job_by_id()`, updated `_collect_filesystem_output()` for atomic collection, updated `_on_progress()` for per-job collection, updated `_prepare_chain_inputs()` for cache-aware logic, simplified `_collect_outputs()` to finalization only
- `tests/unit/test_write_through.py` - New file with 16 unit tests

**Key Functions**:
- `_collect_job_outputs(job, tree_exec_id)` - Write-through for single job: collects fs and minio, skips temp
- `_collect_filesystem_output(key, entry, exec_id)` - Atomic collection with temp dir + os.replace
- `_collect_minio_output(key, entry, exec_id)` - Server-side copy from temp to permanent bucket
- `_minio_temp_exists(tree_exec_id, key)` - Check MinIO cache for chain inputs
- `_prepare_chain_inputs()` - Cache-aware: try MinIO cache, fallback to storage for fs/minio types
- `MinioManager.copy_object()` - Server-side copy (no download/re-upload)
- `MinioManager.object_exists()` - Check object existence using stat_object

**Design Decisions**:
- Atomic collection uses temp file in same directory as dest for same-filesystem atomic replace
- Cache check not called for temp type (no fallback needed, no persistent storage)
- Server-side MinIO copy for efficiency (no coordinator download/upload)
- MinIO data kept as cache until tree ends (cleanup unchanged)
- External temp reads logged as warning (temp only valid within chain)

**Notes**:


### Phase 4: Executor Crash Detection ✅ COMPLETE (2026-01-13)
- [x] Track last heartbeat per executor (already in ExecutorRegistry via on_heartbeat)
- [x] Implement heartbeat monitor loop (30s interval via _maintenance_loop)
- [x] Detect dead executor after 3 missed heartbeats (90s TTL in ExecutorRegistry)
- [x] On executor death: mark all trees failed
- [x] On executor death: release all locks immediately
- [x] On executor death: cleanup MinIO temp
- [x] Add crash detection tests (15 tests)

**Files Changed**:
- `src/coordinator_v2/executor_registry.py` - Modified `prune_stale()` to return list of dead executor IDs (not count)
- `src/coordinator_v2/coordinator.py` - Added `_handle_dead_executors()` method, integrated in `_maintenance_loop()`
- `src/coordinator_v2/register_lock.py` - Added `is_held()` helper method
- `tests/unit/test_executor_crash.py` - New file with 15 unit tests
- `tests/unit/test_coordinator_v2.py` - Updated test to expect list from `prune_stale()`

**Key Functions**:
- `ExecutorRegistry.prune_stale()` - Now returns list of dead executor IDs for cleanup
- `Coordinator._handle_dead_executors(dead_executor_ids)` - Handles all cleanup:
  - Finds all active jobs on dead executors
  - Marks their trees as failed
  - Releases all locks via `register_lock.dequeue()`
  - Unregisters from `TreeValidator`
  - Transitions all jobs to FAILED state
  - Schedules MinIO temp cleanup via `_cleanup_execution()`
  - Forwards failure to dev client if attached

**Design Decisions**:
- Reuse existing `_maintenance_loop()` which runs every 30s (no new loop needed)
- Batch processing: one call handles multiple dead executors
- Non-blocking cleanup: `_cleanup_execution()` runs as background task
- Graceful handling: orphan trees (already removed) don't cause crashes

**Notes**:


### Phase 5: Hot Reload Safety (SIGHUP) ✅ COMPLETE (2026-01-13)
- [x] Warn on register path changes while trees active
- [x] Block removal of registers with active trees
- [x] Block adding `protect: true` to registers with active writes
- [x] Block removing `protect: true` from registers with active shared locks
- [x] Block type changes with active locks
- [x] Warn on bucket/prefix changes for minio with active locks
- [x] Warn on kind changes (file/dir) with active locks
- [x] Add hot reload safety tests (33 tests)

**Files Changed**:
- `src/coordinator_v2/registry_reload.py` - New file with compare_registries(), validate_registry_reload()
- `src/coordinator_v2/coordinator.py` - Updated _hot_reload() to validate before applying changes

**Key Functions**:
- `compare_registries(old, new)` - Detects all changes between registries (add/remove/modify)
- `validate_registry_reload(old, new, lock_mgr)` - Returns blocked/warnings/errors based on lock state
- `ChangeType` enum - ADDED, REMOVED, PATH_CHANGED, BUCKET_CHANGED, TYPE_CHANGED, PROTECT_ADDED, PROTECT_REMOVED, KIND_CHANGED

**Design Decisions**:
- Registry loaded to temp variable before validation (avoids partial apply on error)
- Blocking changes: REMOVED, TYPE_CHANGED (with locks), PROTECT_ADDED (with exclusive), PROTECT_REMOVED (with shared)
- Warning changes: PATH_CHANGED, BUCKET_CHANGED, KIND_CHANGED (locks become orphaned but not corrupted)
- TreeValidator.update_registry() kept simple (just updates reference) - safety checks in coordinator

**Notes**:


### Phase 6: Cleanup ✅ COMPLETE (2026-01-13)
- [x] Update cleanup to handle new lock release flow
- [x] Ensure temp data cleaned on tree completion
- [x] Orphan cleanup for failed collections
- [x] Add cleanup tests (19 tests)

**Files Changed**:
- `tests/unit/test_cleanup.py` - New file with 19 cleanup tests

**Key Findings (from code review)**:
- Phases 2-5 already integrated cleanup functionality:
  - Lock release (`register_lock.dequeue`) always called before cleanup scheduling
  - `_cleanup_execution()` scheduled via `asyncio.create_task()` (non-blocking)
  - Called in all paths: success (`_collect_outputs`), failure (`_handle_chain_failure`), executor death (`_handle_dead_executors`), timeout (maintenance loop)
- Atomic collection temp files cleaned in exception handler of `_collect_filesystem_output()`
- Orphan cleanup runs at startup and hourly via maintenance loop

**Test Coverage**:
- Lock release before cleanup scheduling (3 tests)
- MinIO temp cleanup (2 tests)
- Work directory cleanup (2 tests)
- Atomic collection temp file cleanup on failure (2 tests)
- Orphan cleanup (4 tests)
- Failed collection triggers cleanup (1 test)
- Cleanup best-effort pattern (2 tests)
- Complete cleanup flow documentation (3 tests)

**Notes**:


### Phase 7: Documentation ✅ COMPLETE (2026-01-13)
- [x] Update SPEC.md with any implementation deviations
- [x] Update job-design-guide.md with new types
- [x] Add migration guide for existing jobs
- [x] Update examples

**Files Changed**:
- `SPEC.md` - Updated to v0.8.0: register model section with three types, ENQ/DEQ locking, write-through flow, hot reload safety, migration guide (Appendix B), fan-out with temp registers
- `docs/job-design-guide.md` - Already updated in Phase 1 (verified complete)
- `examples/jobs/README.md` - Updated registry format, job examples using Phase 15 format (reads/writes), temp register examples

**Documentation Updates**:
- SPEC.md Register Map section: Added `temp` type, `protect` flag, ENQ/DEQ table
- SPEC.md Registry section: Complete rewrite with three types, field tables
- SPEC.md Register Locking: Full ENQ/DEQ spec with compatibility matrix, lock scope, executor crash detection
- SPEC.md Input/Output Flow: Write-through collection, cache-aware chain inputs
- SPEC.md Hot Reload: Safety validation table (blocked vs warned changes)
- SPEC.md Fan-Out: Example showing `temp` enables parallel execution
- SPEC.md Appendix B: Migration guide from v0.5.x to v0.8.x

**Notes**:


### Phase 8: Execution Archives (Optional)
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

**Notes**:


---

## Decisions Log

| Date | Decision | Rationale |
|------|----------|-----------|
| 2026-01-13 | Coordinator restart = clean slate | Simpler than lock reconstruction, acceptable for rare event |
| 2026-01-13 | Heartbeat-based crash detection (90s) | Decouples crash detection from job timeout (which can be hours) |
| 2026-01-13 | All-or-nothing lock acquisition | Prevents partial lock holds that block other trees unnecessarily |
| 2026-01-13 | FIFO with queue check before grant | Prevents writer starvation from continuous readers |
| 2026-01-13 | `kind` required for all types | Consistency - all register types need to know if file or directory |
| 2026-01-13 | Use Literal types over field_validator | Cleaner error messages, better IDE support, less code |
| 2026-01-13 | Validation in security_utils.py | Keep security-related validation together, reusable |
| 2026-01-13 | Validate at startup AND hot reload | Prevent invalid state from entering the system at any point |
| 2026-01-13 | Hot reload: load-validate-apply pattern | Load new registry to temp, validate against locks, then apply if safe |
| 2026-01-13 | Path/bucket changes warn but don't block | Orphaned locks are benign (tree completes, lock released); corruption is worse |
| 2026-01-13 | protect:true+exclusive blocks, protect:true+shared OK | Writers can't be interrupted; readers can have stricter access later |
| 2026-01-13 | protect:false+shared blocks, protect:false+exclusive OK | Readers expect immutability; writers already have full access |

---

## Test Scenarios

### Lock Manager ✅ COMPLETE
- [x] Single tree, single lock - granted immediately (`test_single_exclusive_lock_granted`)
- [x] Two trees, same exclusive lock - second waits (`test_exclusive_blocks_exclusive`)
- [x] Two trees, same shared lock - both granted (`test_shared_plus_shared_compatible`)
- [x] Exclusive waiter behind shared holders - waits until all release (`test_shared_blocks_exclusive`)
- [x] Shared waiter behind exclusive waiter - waits (FIFO) (`test_fifo_prevents_starvation`)
- [x] Tree needs [A, B], A available, B held - tree waits, holds nothing (`test_partial_block_acquires_nothing`)
- [x] Deadlock prevention: Tree1 [A,B], Tree2 [B,A] - sorted order prevents (`test_sorted_ordering_prevents_deadlock`)

### Executor Crash ✅ COMPLETE
- [x] Executor crashes at job start - detected in 90s, locks released (`test_marks_tree_failed_on_executor_death`)
- [x] Executor crashes mid-job - detected in 90s, locks released (`test_releases_locks_on_executor_death`)
- [x] Executor network blip (1 missed heartbeat) - no action (`test_prune_returns_empty_list_when_no_stale`)
- [x] Executor slow (2 missed heartbeats) - no action yet (covered by TTL test)
- [x] Executor dead (3 missed heartbeats) - marked dead, cleanup (`test_prune_returns_stale_ids`)
- [x] Multiple trees on same executor - all marked failed (`test_multiple_trees_on_same_executor`)
- [x] Unaffected trees on healthy executors continue (`test_unaffected_trees_continue`)
- [x] No active jobs on dead executor - no action (`test_no_active_jobs_on_dead_executor`)
- [x] Executor comes back online - re-registered (`test_executor_back_online_after_prune`)

### Write-Through ✅ COMPLETE
- [x] Chain job: output collected after each job (`test_collects_fs_output`)
- [x] Temp register: stays in MinIO, no fs write (`test_skips_temp_output`)
- [x] Cache hit: chain reads from MinIO (fast) (`test_fs_cache_hit`)
- [x] Cache miss: falls back to fs read (`test_fs_cache_miss_fallback_to_storage`)
- [x] Atomic collection: partial write doesn't corrupt (`test_atomic_collection_cleans_temp_on_failure`)

### Hot Reload ✅ COMPLETE
- [x] Add new register while idle - OK (`test_added_register_allowed`)
- [x] Remove register with running tree - blocked (`test_removed_register_blocked_with_lock`)
- [x] Change path with running tree - warned (`test_path_changed_warns_with_lock`)
- [x] Add protect to register being written - blocked (`test_protect_added_blocked_with_exclusive_lock`)
- [x] Add protect with only readers - OK (`test_protect_added_allowed_with_shared_lock`)
- [x] Remove protect with readers - blocked (`test_protect_removed_blocked_with_shared_lock`)
- [x] Remove protect with writers - OK (`test_protect_removed_allowed_with_exclusive_lock`)
- [x] Type change with lock - blocked (`test_type_changed_blocked_with_lock`)
- [x] Bucket change with lock - warned (`test_bucket_changed_warns_with_lock`)
- [x] Kind change with lock - warned (`test_kind_changed_warns_with_lock`)

---

## Issues Found

| ID | Description | Status | Resolution |
|----|-------------|--------|------------|
| - | - | - | - |

---

## Performance Notes

_Record any performance observations during implementation._

---

## Migration Checklist

When deploying to production:

- [ ] Backup existing registry.yaml
- [ ] Update coordinator binary
- [ ] Update executor binary (if changed)
- [ ] Restart coordinator (clean slate)
- [ ] Verify executor reconnection
- [ ] Test with simple job before full workload
- [ ] Monitor first scheduled tree execution
