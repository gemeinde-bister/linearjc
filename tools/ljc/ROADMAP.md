---
# LinearJC Development Tool (ljc) - Roadmap

**Last Updated**: 2025-11-18
**Current Version**: v0.2.0-alpha
**Status**: ✅ DEPLOY WORKFLOW COMPLETE | Security Hardened | Production Ready

---

## Recent Updates (2025-11-18)

### **Security Hardening - Archive Extraction**

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

## Quick Status

| Phase | Feature | Status | Time |
|-------|---------|--------|------|
| **Phase 1** | Core Infrastructure | ✅ Complete | 3h |
| **Phase 2** | Repository Management | ✅ Complete | 2h |
| **Phase 3** | Job Creation (Scaffolding) | ✅ Complete | 1h |
| **Phase 4** | Validation | ✅ Complete | 2h |
| **Phase 5** | Package Building | ✅ Complete | 1h |
| **Phase 6** | MQTT Authentication & Message Signing | ✅ Complete | 2h |
| **Phase 7** | MQTT Deployment (Client) | ✅ Complete | 2h |
| **Phase 9** | Coordinator Developer API | ✅ Complete | 3h |
| **Phase 8** | Registry Sync (MQTT) | ⏸️  Deferred | 2h est |
| **Phase 10** | Coordinator Version Upgrade | ⏸️  Deferred | 2h est |
| **Phase 11** | Developer Runtime Commands | 📋 Planned | 20h est |

**Total Completed**: 16 hours | **Planned**: ~20 hours | **Deferred**: ~4 hours

---

## Working Commands (v0.2.0-alpha)

```bash
ljc init <path>                        # ✅ Create repository
ljc new <job-id>                       # ✅ Scaffold new job
ljc list                               # ✅ List jobs
ljc info <job-id>                      # ✅ Show job details
ljc registry [--verbose]               # ✅ Show registry map
ljc validate [job|--all]               # ✅ Validate jobs (includes shebang check)
ljc bump <major|minor|patch> <job-id>  # ✅ Bump semantic version (NEW)
ljc build <job-id>                     # ✅ Build .ljc package
ljc deploy <package> --to <host>       # ✅ Deploy to coordinator (MQTT + MinIO)
ljc sync --from <host>                 # ⏳ Sync registry (stub only)
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

### Phase 8: Registry Sync (MQTT) 📋 PLANNED

**Goal**: Sync registry from coordinator via MQTT

**Command**: `ljc sync`

**Workflow**:
```
1. ljc → coordinator: sync-request (signed)
2. coordinator → ljc: registry-data (signed)
3. ljc updates local registry-map.yaml
4. ljc shows diff (new/changed/removed entries)
```

**Implementation**:
```rust
pub fn run() -> Result<()> {
    let config = load_config()?;
    let mut mqtt = MqttClient::connect(&config)?;

    println!("Requesting registry...");
    let response = mqtt.request_registry()?;

    let old_registry = YamlUtils::parse_registry(&repo.registry_map)?;
    let new_registry = response.registry;

    // Compute diff
    let added = find_new_keys(&old_registry, &new_registry);
    let removed = find_removed_keys(&old_registry, &new_registry);
    let changed = find_changed_entries(&old_registry, &new_registry);

    // Update local file
    YamlUtils::write_compact(&repo.registry_map, &new_registry)?;

    println!("✓ Registry synced");
    println!("  + {} new entries", added.len());
    println!("  ~ {} changed", changed.len());
    println!("  - {} removed", removed.len());

    Ok(())
}
```

**User Experience**:
```bash
$ ljc sync

Connecting to coordinator...
✓ Authenticated as developer-alice
Requesting registry...
✓ Received registry (42 entries)
Updating registry-map.yaml...

Changes:
  + backup_input (new)
  + backup_output (new)
  ~ hello_output (path changed)

✓ Registry synced
Last sync: 2025-11-17 21:00:00
```

**Deliverables**:
- [ ] MQTT-based sync command
- [ ] Registry diff computation
- [ ] Clear change reporting
- [ ] Timestamp tracking

**Time Estimate**: 2 hours

---

### Phase 9: Coordinator Developer API 📋 PLANNED

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

### Phase 10: Coordinator Version Upgrade 📋 PLANNED

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
- [ ] Version comparison logic
- [ ] Upgrade path in install command
- [ ] Registry key ownership tracking
- [ ] Clear upgrade messages
- [ ] Prevent downgrades

**Time Estimate**: 2 hours

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

### Phase 11: Developer Runtime Commands 📋 PLANNED

**Goal**: Interactive developer tools for testing and debugging jobs in real-time

**Motivation**: Dramatically improve development velocity by enabling immediate job testing and live monitoring without waiting for scheduler or manually checking logs.

**Priority 1 - Core Commands** (~10 hours):

#### `ljc exec <job-id> [--follow] [--wait]`
**Execute job immediately (bypass scheduler)**
- Sends signed MQTT request to coordinator
- Coordinator creates JobExecution with `is_dev_execution: true`
- Assigns to executor immediately (no scheduling delay)
- `--follow`: Stream real-time progress updates
- `--wait`: Block until completion, exit with job exit code

**Impact**: 10-100x faster development iteration vs waiting for scheduler

#### `ljc status [job-id] [--all] [--json]`
**Show job scheduling state and execution status**
- Query coordinator's JobTree state via MQTT
- Display: last run, next run, executions in last 24h, scheduling bounds
- Show active execution if running (state, executor, duration, timeout)
- `--all`: Show all trees

#### `ljc tail <job-id|execution-id> [--since <time>]`
**Follow job execution progress in real-time**
- Subscribe to MQTT: `linearjc/jobs/progress/{job_execution_id}`
- Display state transitions with timestamps
- Show error messages if job fails
- Auto-exit on terminal state (COMPLETED, FAILED, TIMEOUT)

**Priority 2 - Monitoring & Debugging** (~5 hours):

#### `ljc ps [--all] [--executor <id>]`
**List all active/running jobs**
- Query coordinator's JobTracker
- Show: execution_id, state, executor, duration, timeout remaining
- Group by executor
- Like Unix `ps` but for LinearJC jobs

#### `ljc logs <job-id> [--last N] [--failed]`
**Show execution history from coordinator memory**
- Query JobTree.execution_history (last 25h)
- Display: timestamp, outcome, duration, executor
- Calculate success rate
- `--failed`: Show only failures with error messages

#### `ljc kill <execution-id> [--force]`
**Cancel running job execution**
- Send cancel request to coordinator
- Coordinator publishes cancel to executor
- Executor terminates job (SIGTERM → SIGKILL)
- Requires message signing (same auth as deploy)

**Priority 3 - Advanced** (~5 hours):

#### `ljc watch [--jobs j1,j2] [--executors e1,e2]`
**Real-time TUI dashboard of all activity**
- Subscribe to all progress topics
- Display active jobs (live updates)
- Show recent completions
- Show upcoming scheduled runs
- Executor status overview
- Uses: ratatui TUI library

#### `ljc tree [job-id]`
**Visualize job trees and registry dependencies**
- Show job chain: `backup.pool → verify → archive`
- Display scheduling configuration
- Trace data flow through registry keys

**Implementation Architecture**:
- Extend `developer_api.py` with new request handlers
- New MQTT topics: `linearjc/dev/{command}/request|response/{id}`
- Reuse existing HMAC signing (same auth as deploy)
- Coordinator queries internal state: `self.trees`, `self.job_tracker`
- Progress streaming: Subscribe to `linearjc/jobs/progress/+`

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

**Time Estimate**:
- MVP (exec, status, tail): 10 hours
- Monitoring (ps, logs, kill): 5 hours
- Advanced (watch, tree): 5 hours
- **Total: ~20 hours**

**Dependencies**:
- Coordinator must expose internal state via developer API
- No database required (uses in-memory state)
- Progress history limited to 25h (coordinator memory)

**Security**:
- Same HMAC-SHA256 signing as deploy
- Rate limiting on dev commands
- Audit trail with client_id logging
- Dev executions clearly marked in logs

**Design Document**: See `DEVELOPER-COMMANDS-DESIGN.md` for full analysis

**Status**: Design complete, awaiting implementation approval

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
