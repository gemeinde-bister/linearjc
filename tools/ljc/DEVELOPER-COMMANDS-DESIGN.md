# LinearJC Developer Commands - Deep Analysis & Design

## Internal State Available in Coordinator

### 1. **JobTree State** (`self.trees`)
```python
class JobTree:
    root: Job                          # First job in chain
    jobs: List[Job]                    # Ordered: root → leaf
    min_daily: int                     # Scheduling bounds
    max_daily: int
    last_execution: Optional[float]    # Unix timestamp
    next_execution: Optional[float]    # Scheduled next run
    execution_history: List[float]     # Last 25h of runs
```

**What we can query:**
- When job last ran
- When it will run next
- Execution frequency (N runs in last 24h)
- Whether within min/max bounds
- Full job chain for tree

### 2. **JobTracker State** (`self.job_tracker`)
```python
class JobExecution:
    job_execution_id: str              # Unique execution ID
    tree_execution_id: str             # Links to tree run
    job_id: str                        # Job being executed
    job_version: str
    state: JobState                    # QUEUED → RUNNING → COMPLETED
    executor_id: Optional[str]         # Which executor has it
    assigned_at: Optional[float]
    timeout_at: Optional[float]
    last_progress: Optional[float]     # Heartbeat timestamp
    error_message: Optional[str]       # If FAILED

# States: QUEUED, ASSIGNED, DOWNLOADING, READY, RUNNING,
#         UPLOADING, COMPLETED, FAILED, TIMEOUT
```

**What we can query:**
- All active/running jobs
- Which executor is running what
- Job progress state
- Time since last heartbeat
- Failure reasons

### 3. **MQTT Topics Already in Use**

**Subscribe (Coordinator listens):**
- `linearjc/capabilities/{executor_id}` - Executor announcements
- `linearjc/jobs/progress/+` - All job progress updates
- `linearjc/deploy/request/{coordinator_id}` - Deploy requests ✅ (used by ljc)
- `linearjc/deploy/complete/{coordinator_id}` - Deploy completions

**Publish (Coordinator sends):**
- `linearjc/jobs/requests/{job_execution_id}` - Job assignments
- `linearjc/jobs/available` - Job announcements
- `linearjc/deploy/response/{client_id}` - Deploy responses ✅ (used by ljc)

---

## Proposed Developer Commands

### **Priority 1: Essential Development Tools**

#### 1. `ljc exec <job-id> [--wait] [--follow]`
**Purpose**: Execute job immediately for testing (bypass scheduler)

**Use cases:**
- Test job after code changes without waiting for scheduler
- Debug failing jobs in controlled environment
- Verify registry inputs/outputs work correctly

**Implementation:**
```
1. ljc sends signed MQTT message to new topic:
   linearjc/dev/exec/request/{coordinator_id}

2. Coordinator receives, validates:
   - Job exists
   - Executors available with capability
   - Registry entries valid

3. Coordinator creates JobExecution with special flag:
   is_dev_execution: true (bypasses scheduling)

4. Assigns to executor immediately

5. Options:
   --wait: Block until completion, exit with job exit code
   --follow: Stream progress updates (implies --wait)
```

**Response format:**
```json
{
  "status": "accepted",
  "job_execution_id": "hello.world-20251118-143022-abc123def",
  "tree_execution_id": "hello.world-20251118-143022-abc12345",
  "message": "Job queued for execution"
}
```

---

#### 2. `ljc tail <job-id|execution-id> [--since <time>]`
**Purpose**: Follow job execution progress in real-time

**Use cases:**
- Watch job progress during development
- Debug stuck jobs
- See exactly when/where failures occur

**Implementation:**
```
1. If job-id provided:
   - Query coordinator for latest execution of that job
   - Get job_execution_id

2. Subscribe to MQTT: linearjc/jobs/progress/{job_execution_id}

3. Display progress updates as they arrive:
   - State transitions
   - Timestamps
   - Error messages (if FAILED)

4. Exit when terminal state reached (COMPLETED, FAILED, TIMEOUT)

5. --since: Show progress since timestamp/relative time
   (requires coordinator to cache recent progress - future enhancement)
```

**Output:**
```
→ Tailing: hello.world (execution: hello.world-20251118-143022-abc123def)
→ Executor: executor-01

14:30:22 QUEUED      Job queued for execution
14:30:23 ASSIGNED    Assigned to executor-01
14:30:24 DOWNLOADING Downloading inputs...
14:30:26 READY       Inputs ready, starting execution
14:30:27 RUNNING     Executing script.sh
14:30:35 UPLOADING   Uploading outputs...
14:30:38 COMPLETED   Job completed successfully (duration: 16s)

✓ Job completed successfully
```

---

#### 3. `ljc status [job-id] [--all] [--json]`
**Purpose**: Show job/tree scheduling status and execution history

**Use cases:**
- Check when job will run next
- Verify scheduling bounds
- See execution frequency

**Implementation:**
```
1. ljc sends signed MQTT request:
   linearjc/dev/status/request/{coordinator_id}
   Payload: { "job_id": "backup.pool" } (or null for all)

2. Coordinator queries self.trees and self.job_tracker:
   - Find tree(s) matching job_id
   - Get scheduling state
   - Get active executions
   - Calculate metrics

3. Respond on: linearjc/dev/status/response/{client_id}
```

**Response format:**
```json
{
  "job_id": "backup.pool",
  "tree": {
    "jobs": ["backup.pool", "backup.verify"],
    "scheduling": {
      "min_daily": 2,
      "max_daily": 4,
      "executions_last_24h": 3,
      "within_bounds": true
    },
    "last_execution": "2025-11-18T12:30:00Z",
    "next_execution": "2025-11-18T18:45:00Z"
  },
  "active_execution": {
    "execution_id": "backup.pool-20251118-143022-abc123",
    "state": "RUNNING",
    "executor": "executor-02",
    "duration": 45.2,
    "timeout_in": 314.8
  }
}
```

**Terminal output:**
```
backup.pool
├─ Tree: backup.pool → backup.verify
├─ Scheduling: 2-4 runs/day (currently: 3)
│  └─ Within bounds: ✓
├─ Last run: 2025-11-18 12:30:00 (6h ago)
├─ Next run: 2025-11-18 18:45:00 (in 4h 15m)
└─ Status: ✓ RUNNING on executor-02 (45s, timeout in 5m)
```

---

#### 4. `ljc ps [--all] [--executor <id>]`
**Purpose**: List all running/active jobs (like Unix `ps`)

**Use cases:**
- See what's currently running
- Identify stuck jobs
- Monitor executor load

**Implementation:**
```
1. Query coordinator via MQTT for job_tracker state
2. Filter by state (default: show ASSIGNED → UPLOADING)
3. Group by executor
4. Show duration, timeout remaining
```

**Output:**
```
Active Jobs:

EXECUTOR: executor-01
  backup.pool-20251118-143022-abc123    RUNNING    2m 15s  (timeout in 8m)
  analyze.data-20251118-143045-def456   UPLOADING  45s     (timeout in 2m)

EXECUTOR: executor-02
  export.csv-20251118-143100-ghi789     DOWNLOADING  12s   (timeout in 5m)

Total: 3 jobs active across 2 executors
```

---

### **Priority 2: Debugging & Troubleshooting**

#### 5. `ljc logs <job-id> [--last N] [--failed] [--json]`
**Purpose**: Show execution history from coordinator's memory

**Use cases:**
- Review recent execution history
- Find patterns in failures
- Calculate success rate

**Implementation:**
```
1. Query coordinator for JobTree.execution_history
2. Query job_tracker.get_all() for completed jobs
3. Filter by job_id
4. Show recent executions with outcomes

Note: Coordinator only keeps 25h of history in memory
For longer history, need persistent logging (future: database/log aggregation)
```

**Output:**
```
Recent executions of backup.pool (last 5):

2025-11-18 12:30:00  ✓ COMPLETED  (18s, executor-01)
2025-11-18 08:15:00  ✓ COMPLETED  (21s, executor-02)
2025-11-18 04:00:00  ✗ FAILED     (5s, executor-01)
  └─ Error: Input file not found: /data/sensor.csv
2025-11-17 20:45:00  ✓ COMPLETED  (19s, executor-01)
2025-11-17 16:30:00  ✓ COMPLETED  (22s, executor-02)

Success rate: 80% (4/5)
Avg duration: 17s (successful jobs)
```

---

#### 6. `ljc kill <execution-id> [--force]`
**Purpose**: Cancel running job execution

**Use cases:**
- Stop stuck jobs
- Cancel long-running jobs during debugging
- Emergency stop before bad data propagates

**Implementation:**
```
1. Send cancel request to coordinator
2. Coordinator publishes cancel message to executor
3. Executor terminates job (SIGTERM, then SIGKILL after grace period)
4. Coordinator marks as CANCELLED state (new state needed)

Security: Requires signed message (same auth as deploy)
```

---

### **Priority 3: Advanced Features**

#### 7. `ljc watch [--jobs job1,job2] [--executors ex1,ex2]`
**Purpose**: Real-time dashboard of all activity

**Use cases:**
- Monitor production system
- Debug complex multi-job scenarios
- Observe scheduler behavior

**Implementation:**
```
1. Subscribe to all progress topics: linearjc/jobs/progress/+
2. Periodically poll status endpoint for scheduling info
3. TUI (text UI) showing:
   - Active jobs (updating live)
   - Recent completions
   - Upcoming scheduled runs
   - Executor status

Uses: ratatui or similar TUI library in Rust
```

---

#### 8. `ljc tree [job-id]`
**Purpose**: Visualize job trees and dependencies

**Output:**
```
backup.pool → backup.verify → backup.archive
├─ Scheduling: 2-4 runs/day
├─ Last run: 6h ago
├─ Next run: in 4h 15m
└─ Registry flow:
   ├─ IN:  raw_data
   ├─ OUT: backup_data → [backup.verify]
   ├─ OUT: verified_data → [backup.archive]
   └─ OUT: archive_data
```

---

## Implementation Architecture

### **Option A: Extend Developer API (Recommended)**

**Add new coordinator endpoints:**
```python
# In developer_api.py

def handle_exec_request(self, envelope: Dict, client_id: str):
    """Handle immediate execution request."""
    payload = verify_message(envelope)
    job_id = payload['job_id']

    # Validate job exists, has executors
    # Create JobExecution with is_dev_execution=true
    # Bypass scheduler, assign immediately
    # Return execution_id to client

def handle_status_request(self, envelope: Dict, client_id: str):
    """Handle status query."""
    payload = verify_message(envelope)
    job_id = payload.get('job_id')

    # Query self.coordinator.trees
    # Query self.coordinator.job_tracker
    # Return scheduling + execution state

def handle_logs_request(self, envelope: Dict, client_id: str):
    """Handle execution history query."""
    # Return JobTree.execution_history
    # Include outcomes from completed jobs
```

**New MQTT topics:**
```
linearjc/dev/exec/request/{coordinator_id}
linearjc/dev/exec/response/{client_id}

linearjc/dev/status/request/{coordinator_id}
linearjc/dev/status/response/{client_id}

linearjc/dev/logs/request/{coordinator_id}
linearjc/dev/logs/response/{client_id}

linearjc/dev/kill/request/{coordinator_id}
linearjc/dev/kill/response/{client_id}
```

**Authentication:** Same shared secret HMAC signing as deploy

---

### **Option B: HTTP REST API**

**Alternative:** Add FastAPI/Flask REST endpoints to coordinator

**Pros:**
- Standard HTTP tools (curl, Postman)
- Easier debugging
- Better for CI/CD integration

**Cons:**
- Another protocol to secure (need TLS + auth)
- More attack surface
- Doesn't fit "MQTT-first" design

**Verdict:** MQTT approach is more consistent, reuses existing auth

---

## Security Considerations

### **1. Authorization**
- Use same shared secret HMAC signing as deploy
- Coordinator validates message signature
- Rate limiting on dev commands (prevent DoS)

### **2. Isolation**
- Dev executions can't interfere with scheduled jobs
- Use separate execution pool? (future: resource limits)
- Mark dev executions clearly in logs

### **3. Audit Trail**
- Log all dev commands: who executed, when, what
- Include client_id in logs
- Correlation IDs for tracing

---

## Phased Implementation

### **Phase 1: Core Infrastructure** (2-3h)
- Extend DeveloperAPI class with request handlers
- Add MQTT topic subscriptions
- Implement exec request/response

### **Phase 2: Status & Monitoring** (2-3h)
- Implement status command
- Add job_tracker query methods
- Implement ps command

### **Phase 3: Live Monitoring** (3-4h)
- Implement tail command
- Subscribe to progress topics
- Real-time output formatting

### **Phase 4: History & Debugging** (2-3h)
- Implement logs command
- Query execution history
- Format success/failure stats

### **Phase 5: Advanced** (4-5h)
- Implement kill command
- Add watch TUI
- Tree visualization

**Total: ~15-20 hours for full implementation**

---

## Use Case Examples

### **Scenario 1: Developing a new job**
```bash
# Edit job script
vim jobs/backup.pool/script.sh

# Validate locally
ljc validate backup.pool

# Test execution immediately
ljc exec backup.pool --follow

→ Tailing: backup.pool
14:30:22 QUEUED
14:30:23 ASSIGNED to executor-01
14:30:27 RUNNING
14:30:35 UPLOADING
✓ COMPLETED (13s)

# Build and deploy if successful
ljc bump patch backup.pool
ljc build backup.pool
ljc deploy dist/backup.pool.ljc --to coordinator-host
```

### **Scenario 2: Debugging production issue**
```bash
# Check status
ljc status backup.pool

backup.pool: ✗ Last execution FAILED 2h ago

# Review recent failures
ljc logs backup.pool --failed

2025-11-18 12:30:00  ✗ FAILED (5s)
  └─ Error: Input file not found: /data/sensor.csv

# Check if job is scheduled
ljc status backup.pool

Next run: in 45m

# Execute now to test fix
ljc exec backup.pool --follow
✓ COMPLETED
```

### **Scenario 3: Monitoring production**
```bash
# See what's running
ljc ps

3 jobs active:
  backup.pool (executor-01) - 2m 15s
  analyze.data (executor-01) - 45s
  export.csv (executor-02) - 12s

# Watch specific job
ljc tail backup.pool

→ Following backup.pool...
[live updates stream here]
```

---

## Alternative: Minimal Implementation (Quick Win)

If 15-20h is too much, start with **just `ljc exec`**:

1. Add one MQTT endpoint: `linearjc/dev/exec/request`
2. Coordinator creates immediate execution (bypasses scheduler)
3. ljc returns execution_id
4. User manually tails: `mosquitto_sub -t "linearjc/jobs/progress/#"`

**Time: 2-3 hours**
**Value: Immediate job testing without waiting for scheduler**

Then add other commands incrementally based on user feedback.

---

## Recommendation

**Start with Priority 1 commands:**
1. `ljc exec` - Most valuable for development
2. `ljc status` - Essential for debugging
3. `ljc tail` - Critical for seeing what's happening

**Skip for now:**
- `ljc watch` (complex TUI, low ROI initially)
- Persistent logging (needs database/log aggregation)

**Implementation order:**
1. Extend DeveloperAPI with exec + status handlers (4h)
2. Add ljc commands: exec, status, ps (3h)
3. Add tail command with progress streaming (3h)

**Total MVP: ~10 hours**

This gives 80% of the value with minimal complexity.
