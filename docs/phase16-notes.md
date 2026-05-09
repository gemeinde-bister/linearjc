# Phase 16 Notes - CLI Improvements and Observability

Collected during budget-dash pipeline testing (2026-01-13).

## CLI Enhancements Needed

### 1. Registry Management Commands

**Priority: High**

`ljc register get/push` for manual data operations:
```bash
ljc register get budgetdash.budgets ./local-copy/
ljc register push ./budget-2026.json budgetdash.budgets/
ljc register push ./external.db myapp.source --force  # Override protected
```

This is what we did manually with scp. Would streamline bootstrapping and manual data updates.

**Priority: Medium**

`ljc register orphans` / `ljc register delete` for cleanup:
```bash
ljc register orphans              # List unused registry entries
ljc register delete myapp.old     # Remove from coordinator + optionally filesystem
```

We saw orphaned entries in `ljc validate` output.

### 2. Tree Inspection Commands

**Priority: High**

Need a way to display execution trees and find root jobs:
```bash
ljc tree                          # Show all trees with their jobs
ljc tree publish.dashboard        # Show tree containing this job
ljc tree --roots                  # List only root job IDs
ljc exec publish.dashboard --get-root  # Show which root to execute
```

Currently if you try `ljc exec <dependent-job>`, you get "Job not found" with no guidance on which root to execute instead.

### 3. Search Command

**Priority: Low**

Useful for debugging, but `ssh + find` works:
```bash
ljc search "*.json" --in budgetdash.budgets
ljc search --workdir <exec-id>    # If workdirs were preserved
```

## Bugs Found

### Bug 1: `ljc exec <dependent-job>` fails with "Job not found"

- **Issue**: When trying to exec a job that has dependencies (not a tree root), you get "Job not found"
- **Expected**: Should either execute from the root, or give a helpful error like "publish.dashboard is part of tree 'export.spending', use `ljc exec export.spending`"
- **Workaround**: Must exec the tree root, not intermediate/leaf jobs

### Bug 2: `ljc exec --follow` only shows first job in chain

- **Issue**: When executing a 3-job chain, `--follow` output stopped after job 1 completed
- **Expected**: Should stream progress for all jobs in the tree (1/3, 2/3, 3/3)
- **Impact**: User thinks chain is done when only first job finished; must check logs to see full progress

## Observability Gaps

### Log Files - Biggest Gap

**Priority: High**

Current problems:
- Coordinator: "Unknown error" with no details
- Executor: "exit code 1" but no stdout/stderr visible
- Work directory cleaned up immediately after execution
- `ljc logs <job>` only shows timestamps, not actual output

**Suggestions:**

1. **Capture job stdout/stderr to MinIO before cleanup** - coordinator can fetch on failure
2. **`ljc logs <job> --last` should show actual script output**, not just timestamps
3. **Preserve workdir on failure** (optional flag) for debugging
4. **Stream stdout in real-time during `--follow`** (not just state changes)

The log visibility issue is painful - we had to test locally to find bugs because remote debugging is blind.

### Profiling

**Priority: Low**

Jobs needed ~25 seconds to execute a 3-job chain, which seems longer than necessary. Need profiling tools:
- Time breakdown per job
- Time spent in: input prep, download, execution, upload, write-through
- Identify bottlenecks (network? MinIO? archive compression?)

## Isolation with uvx

**Priority: Medium**

Need to test `isolation: relaxed` with uvx/uv. Likely needs:
```yaml
extra_read_paths:
  - /usr/bin/uv
  - /usr/bin/python3
  - ~/.cache/uv    # Or wherever uv caches packages
```

Currently using `isolation: none` for all Python jobs as workaround.

## Design Observations

### Tree structure depends on `depends:` not data flow

- Initially `render.dashboard` and `export.spending` were separate trees even though render reads spending's output
- This is by design (locking handles conflicts), but can be confusing
- After adding `depends: [export.spending]`, they became one tree
- Documentation updated to clarify this

### What works correctly

- Chain continuation (airdrop, waiting for executor install)
- Write-through to multiple destinations (registry + web server)
- Registry locking (5 locks acquired/released correctly)
- File permissions (world-readable for web serving)
- Full 3-job chain execution: export.spending -> render.dashboard -> publish.dashboard
