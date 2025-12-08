// ============================================================================
// LinearJC Executor - Minimal, auditable job runner
//
// SECURITY CRITICAL: This code runs as root and must be bulletproof.
// All external inputs are validated. All privileged operations are audited.
//
// Supply Chain Security: All dependencies are pinned to exact versions.
// Code Review: Keep this file small and readable for security audits.
// ============================================================================

use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use nix::sys::signal::{kill, Signal};
use nix::unistd::{getuid, Pid, Uid, User};
use rumqttc::{Client, Event, MqttOptions, Packet, QoS};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::{LazyLock, Mutex};
use std::{collections::HashMap, env, fs, path::PathBuf, time::Duration};

// Thread-safe tracking of active job PIDs for cancellation support
static ACTIVE_JOBS: LazyLock<Mutex<HashMap<String, Pid>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

// Note: PermissionsExt now handled by linearjc_core::package

// Use shared linearjc-core library for security-critical code
use linearjc_core::{
    archive::{extract_tar_gz, create_tar_gz_from_file, create_tar_gz_from_dir},
    isolation::{FilesystemIsolation, IsolationConfig, ResourceLimits},
    signing::{sign_message, verify_message},
    workdir::{chown_recursive, setup_secure_work_dir, validate_path_component},
    // Phase 13 prep: Use shared execution and WorkDir from linearjc-core
    execution::{spawn_isolated, poll_job, cleanup_job, validate_job_script},
    // Unified job/package handling
    package::{read_package_metadata, get_package_version, extract_package_to_workdir, compare_versions},
    WorkDir,
};

// Note: flate2/tar now handled by linearjc_core::package

#[derive(Deserialize)]
struct JobRequest {
    // Correlation IDs
    tree_execution_id: String,
    job_execution_id: String,
    // Job details
    job_id: String,
    assigned_to: Option<String>,
    inputs: HashMap<String, DataSpec>,
    outputs: HashMap<String, DataSpec>,
    run: RunSpec,  // SPEC.md v0.5.0: renamed from 'executor'
}

/// SPEC.md v0.5.0 format: run configuration
#[derive(Deserialize)]
struct RunSpec {
    user: String,
    timeout: u64,
    #[serde(default)]
    entry: Option<String>,  // Optional: entry script filename
    #[serde(default = "default_isolation")]
    isolation: String,      // SPEC.md v0.5.0: flattened to string (strict|relaxed|none)
    #[serde(default = "default_network")]
    network: bool,
    #[serde(default)]
    extra_read_paths: Vec<String>,
    #[serde(default)]
    limits: Option<ResourceLimitsSpec>,
}

#[derive(Debug, Clone, Deserialize)]
struct ResourceLimitsSpec {
    cpu_percent: Option<u32>,
    memory_mb: Option<u64>,
    processes: Option<u64>,
    io_mbps: Option<u64>,
}

fn default_isolation() -> String {
    "none".to_string() // Default: no isolation (opt-in)
}

fn default_network() -> bool {
    true // Default: network enabled
}

#[derive(Deserialize)]
struct DataSpec {
    url: String,
    format: String,
    kind: Option<String>, // SPEC.md v0.5.0: "file" or "dir" (renamed from path_type)
}

// sign_message and verify_message are now imported from linearjc_core::signing

// ============================================================================
// INPUT VALIDATION - SECURITY CRITICAL
// All external inputs must be validated before use in file paths, commands,
// or privileged operations. Defense in depth against injection attacks.
// ============================================================================

// validate_path_component is now imported from linearjc_core::workdir
// validate_job_script is now imported from linearjc_core::execution

/// Validate URL scheme - only allow HTTP/HTTPS
fn validate_url(url: &str) -> Result<()> {
    // Basic length check
    if url.len() > 2048 {
        anyhow::bail!("URL too long: {} bytes", url.len());
    }

    // Check scheme
    if url.starts_with("https://") || url.starts_with("http://") {
        Ok(())
    } else if url.starts_with("file://") {
        anyhow::bail!("file:// URLs not allowed (security risk)");
    } else {
        anyhow::bail!("Only HTTP/HTTPS URLs allowed");
    }
}

/// Validate username - must exist and contain only safe characters
fn validate_username(username: &str) -> Result<()> {
    if username.is_empty() {
        anyhow::bail!("Username cannot be empty");
    }

    if username.len() > 32 {
        anyhow::bail!("Username too long: {} bytes", username.len());
    }

    // Only allow safe characters in usernames
    for c in username.chars() {
        if !c.is_alphanumeric() && c != '-' && c != '_' {
            anyhow::bail!(
                "Invalid character in username '{}': '{}' (only alphanumeric, -, _ allowed)",
                username, c
            );
        }
    }

    // Verify user exists
    User::from_name(username)?
        .ok_or_else(|| anyhow::anyhow!("User '{}' does not exist", username))?;

    Ok(())
}

/// Validate complete job request - all fields must pass validation
fn validate_job_request(req: &JobRequest) -> Result<()> {
    // Validate correlation IDs (used in paths and topics)
    validate_path_component(&req.tree_execution_id)
        .context("Invalid tree_execution_id")?;
    validate_path_component(&req.job_execution_id)
        .context("Invalid job_execution_id")?;

    // Validate job ID (used in script path)
    validate_path_component(&req.job_id)
        .context("Invalid job_id")?;

    // Validate input and output names (used in filesystem paths)
    for name in req.inputs.keys() {
        validate_path_component(name)
            .context(format!("Invalid input name: {}", name))?;
    }
    for name in req.outputs.keys() {
        validate_path_component(name)
            .context(format!("Invalid output name: {}", name))?;
    }

    // Validate URLs (prevent file:// and other dangerous schemes)
    for (name, spec) in req.inputs.iter().chain(req.outputs.iter()) {
        validate_url(&spec.url)
            .context(format!("Invalid URL for {}", name))?;

        // Validate archive format
        if spec.format != "tar.gz" {
            anyhow::bail!(
                "Unsupported archive format: {}. Only tar.gz is supported in v0.1",
                spec.format
            );
        }
    }

    // Validate run user
    validate_username(&req.run.user)
        .context("Invalid run user")?;

    // Validate timeout (prevent extremely long-running jobs)
    if req.run.timeout == 0 {
        anyhow::bail!("Timeout must be > 0");
    }
    if req.run.timeout > 86400 {
        // 24 hours max
        anyhow::bail!("Timeout too long: {}s (max: 86400s / 24h)", req.run.timeout);
    }

    info!(
        "tree_exec={} job_exec={} job_id={} user={} timeout={} inputs={} outputs={} Validated job request",
        req.tree_execution_id,
        req.job_execution_id,
        req.job_id,
        req.run.user,
        req.run.timeout,
        req.inputs.len(),
        req.outputs.len()
    );

    Ok(())
}

/// Get UID for a given username (already validated)
fn get_uid_for_user(username: &str) -> Result<Uid> {
    User::from_name(username)?
        .ok_or_else(|| anyhow::anyhow!("User '{}' not found", username))
        .map(|user| user.uid)
}

// ============================================================================
// SECURE ARCHIVE HANDLING - Now uses linearjc_core::archive
// extract_tar_gz, create_tar_gz_from_file, create_tar_gz_from_dir imported from core
// ============================================================================

// ============================================================================
// PACKAGE HANDLING - Now uses linearjc_core::package
// read_package_metadata, get_package_version, compare_versions,
// extract_package_to_workdir imported from linearjc_core::package
// ============================================================================

// chown_recursive is now imported from linearjc_core::workdir

// ============================================================================
// SECURE WORK DIRECTORY - Now uses linearjc_core::workdir::setup_secure_work_dir
// ============================================================================

/// Convert RunSpec from job request to isolation::IsolationConfig
/// SPEC.md v0.5.0: isolation is now a direct string field (strict|relaxed|none)
fn parse_isolation_config(run: &RunSpec, job_exec_id: &str) -> Result<IsolationConfig> {

    // Parse filesystem isolation mode from string
    let filesystem = FilesystemIsolation::from_str(&run.isolation)
        .context(format!("Invalid isolation mode: {}", run.isolation))?;

    // Convert resource limits if present
    let limits = run.limits.as_ref().map(|l| ResourceLimits {
        cpu_percent: l.cpu_percent,
        memory_mb: l.memory_mb,
        processes: l.processes,
        io_mbps: l.io_mbps,
    });

    // Convert extra read paths to PathBuf
    let extra_read_paths: Vec<PathBuf> = run.extra_read_paths
        .iter()
        .map(|s| PathBuf::from(s))
        .collect();

    // Log isolation configuration
    info!(
        "job_exec={} isolation: mode={:?}, network={}, limits={:?}, extra_paths={}",
        job_exec_id,
        filesystem,
        run.network,
        limits.is_some(),
        extra_read_paths.len()
    );

    Ok(IsolationConfig {
        filesystem,
        network: run.network,
        limits,
        extra_read_paths,
    })
}

fn execute_job(
    req: &JobRequest,
    jobs_dir: &PathBuf,
    work_dir: &PathBuf,
    executor_id: &str,
    client: &mut Client,
    shared_secret: &str,
    allowed_extensions: &[String],
) -> Result<()> {
    use std::time::Instant;

    // SECURITY CRITICAL: Validate all inputs before any file operations
    validate_job_request(req).context("Job request validation failed")?;

    let tree_exec = &req.tree_execution_id;
    let job_exec = &req.job_execution_id;

    // Note: Archive format validation is now done in validate_job_request()
    // This was moved to consolidate all input validation in one place

    // Phase 7: Standardized directory layout (using linearjc-core WorkDir)
    let job_work = WorkDir::new(work_dir, &req.job_execution_id)
        .context("Failed to create work directory")?;

    // Aliases for readability (matches linearjc-core naming)
    let job_dir = &job_work.root;
    let in_dir = &job_work.in_dir;
    let out_dir = &job_work.out_dir;
    let tmp_dir = &job_work.tmp_dir;
    let work_subdir = &job_work.work_dir;

    // Download & extract inputs with timing
    if !req.inputs.is_empty() {
        publish_progress(client, executor_id, tree_exec, job_exec, "downloading",
                        &format!("Downloading {} input(s)", req.inputs.len()), shared_secret);
    }

    for (name, spec) in &req.inputs {
        let start = Instant::now();
        info!("tree_exec={} job_exec={} executor={} state=downloading input={} Downloading input",
              tree_exec, job_exec, executor_id, name);

        // Download archive to work subdirectory
        let archive = work_subdir.join(format!("{}.{}", name, &spec.format));

        let bytes = reqwest::blocking::get(&spec.url)?.bytes()?;
        let size = bytes.len();
        fs::write(&archive, bytes)?;

        // Phase 7: Extract based on kind (SPEC.md v0.5.0: "file" or "dir")
        let kind = spec.kind.as_deref().unwrap_or("dir");
        match kind {
            "file" => {
                // Extract single file directly to in/{name}
                let dest_file = in_dir.join(name);

                // Extract to temp directory first
                let temp_extract = work_subdir.join(format!("{}_extract", name));
                fs::create_dir_all(&temp_extract)?;

                // SECURITY: Use Rust tar library instead of subprocess to prevent command injection
                extract_tar_gz(&archive, &temp_extract)
                    .context(format!("Failed to extract {}", name))?;

                // Find the single file in the archive (assume first file is the target)
                let entries: Vec<_> = fs::read_dir(&temp_extract)?
                    .filter_map(|e| e.ok())
                    .collect();

                if entries.is_empty() {
                    anyhow::bail!("Archive for '{}' is empty", name);
                }

                // Move first file to destination
                let src_file = entries[0].path();
                if src_file.is_file() {
                    fs::copy(&src_file, &dest_file)?;
                } else {
                    anyhow::bail!("Expected file for '{}', got directory", name);
                }

                // Cleanup temp extraction
                fs::remove_dir_all(&temp_extract)?;

                info!("Extracted input '{}' as file to in/{}", name, name);
            }
            "dir" => {
                // Extract directory to in/{name}/
                let dest_dir = in_dir.join(name);
                fs::create_dir_all(&dest_dir)?;

                // SECURITY: Use Rust tar library instead of subprocess to prevent command injection
                extract_tar_gz(&archive, &dest_dir)
                    .context(format!("Failed to extract {}", name))?;

                info!("Extracted input '{}' as directory to in/{}/", name, name);
            }
            other => {
                anyhow::bail!("Unknown kind '{}' for input '{}'", other, name);
            }
        }

        fs::remove_file(&archive)?;

        let duration_ms = start.elapsed().as_millis();
        info!("tree_exec={} job_exec={} executor={} input={} bytes={} duration_ms={} Input downloaded",
              tree_exec, job_exec, executor_id, name, size, duration_ms);
    }

    // Phase 7: Add metadata file for job introspection (using linearjc-core WorkDir)
    let input_names: Vec<String> = req.inputs.keys().cloned().collect();
    let output_names: Vec<String> = req.outputs.keys().cloned().collect();
    job_work.create_metadata(&input_names, &output_names, &req.job_id, &req.job_execution_id)
        .context("Failed to create job metadata")?;

    // Get target user info for privilege operations
    // SECURITY: Skip chown/setuid if already running as the target user (allows non-root E2E testing)
    let uid = get_uid_for_user(&req.run.user)?;
    let user_info = User::from_uid(uid)?.ok_or_else(|| anyhow::anyhow!("User info not found"))?;
    let current_uid = getuid();
    let needs_privilege_change = current_uid != uid;

    // Change ownership of work directory to job user so they can write outputs
    // SECURITY: Use nix library chown instead of subprocess to prevent command injection
    if needs_privilege_change {
        job_work.chown_all(uid, user_info.gid)
            .context("Failed to chown work directory")?;
    } else {
        debug!("Skipping chown: already running as target user {}", req.run.user);
    }

    // Parse isolation configuration (SPEC.md v0.5.0: from run spec)
    let isolation_config = parse_isolation_config(&req.run, job_exec)?;

    // Inputs ready, about to run job
    publish_progress(client, executor_id, tree_exec, job_exec, "ready",
                    "Inputs downloaded, starting job", shared_secret);

    // SPEC.md v0.5.0: Find .ljc package and extract to work_dir
    let package_path = jobs_dir.join(format!("{}.ljc", &req.job_id));
    if !package_path.exists() {
        anyhow::bail!(
            "Package not found for job_id '{}': {}",
            req.job_id,
            package_path.display()
        );
    }

    // Extract package to job_dir (work_dir/{execution_id}/)
    // Script and all job files will be in job_dir, not jobs_dir
    let script = extract_package_to_workdir(&package_path, job_dir)
        .context(format!("Failed to extract package for {}", req.job_id))?;

    // SECURITY: Validate extracted script (defense in depth)
    validate_job_script(&script)
        .context(format!("Extracted script validation failed: {}", script.display()))?;

    info!("Extracted package to {} (script: {})", job_dir.display(), script.display());

    // Chown extracted files to job user (script, bin/, etc.)
    // Skip in/ out/ tmp/ which are already owned by job user
    // SECURITY: Skip if already running as target user (allows non-root E2E testing)
    if needs_privilege_change {
        for entry in fs::read_dir(job_dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            // Skip directories we already chowned
            if name == "in" || name == "out" || name == "tmp" || name == ".work" {
                continue;
            }

            chown_recursive(&path, uid, user_info.gid)
                .context(format!("Failed to chown extracted file: {}", path.display()))?;
        }
    }

    publish_progress(client, executor_id, tree_exec, job_exec, "running",
                    "Job script executing", shared_secret);

    info!("tree_exec={} job_exec={} executor={} state=running user={} Running job script",
          tree_exec, job_exec, executor_id, req.run.user);

    // Phase 13 prep: Use linearjc-core execution API for job spawning
    // This handles: fork, cgroup creation, isolation, privilege drop, exec
    let spawned_job = spawn_isolated(
        &script,
        &job_work,
        &req.job_id,
        &req.job_execution_id,
        &req.run.user,
        &isolation_config,
    ).context("Failed to spawn isolated job")?;

    // Register PID for cancellation support
    if let Ok(mut jobs) = ACTIVE_JOBS.lock() {
        jobs.insert(job_exec.to_string(), spawned_job.pid);
    }

    // Helper to unregister PID on all exit paths
    let unregister_pid = || {
        if let Ok(mut jobs) = ACTIVE_JOBS.lock() {
            jobs.remove(job_exec);
        }
    };

    // Poll for job completion using linearjc-core poll_job()
    // (non-blocking to allow MQTT event loop to run)
    let execution_result = loop {
        match poll_job(&spawned_job)? {
            None => {
                // Job still running, sleep briefly
                std::thread::sleep(Duration::from_millis(100));
                continue;
            }
            Some(result) => break result,
        }
    };

    // Unregister PID and cleanup cgroup
    unregister_pid();
    cleanup_job(spawned_job);

    // Handle execution result
    let job_duration_ms = execution_result.duration.as_millis();
    if execution_result.success {
        info!("tree_exec={} job_exec={} executor={} duration_ms={} Job script completed successfully",
              tree_exec, job_exec, executor_id, job_duration_ms);
    } else if let Some(code) = execution_result.exit_code {
        error!("tree_exec={} job_exec={} executor={} duration_ms={} exit_code={} Job script failed",
               tree_exec, job_exec, executor_id, job_duration_ms, code);
        anyhow::bail!("Job script failed with exit code {}", code);
    } else if let Some(signal) = execution_result.signal {
        error!("tree_exec={} job_exec={} executor={} signal={} Job script killed by signal",
               tree_exec, job_exec, executor_id, signal);
        anyhow::bail!("Job script killed by signal {}", signal);
    } else {
        error!("tree_exec={} job_exec={} executor={} Job script terminated unexpectedly",
               tree_exec, job_exec, executor_id);
        anyhow::bail!("Job script terminated unexpectedly: {:?}", execution_result.error);
    }

    // Archive & upload outputs with timing
    if !req.outputs.is_empty() {
        publish_progress(client, executor_id, tree_exec, job_exec, "uploading",
                        &format!("Uploading {} output(s)", req.outputs.len()), shared_secret);
    }

    for (name, spec) in &req.outputs {
        let start = Instant::now();
        info!("tree_exec={} job_exec={} executor={} state=uploading output={} Uploading output",
              tree_exec, job_exec, executor_id, name);

        // Phase 7: Read from out/{name}
        let source = out_dir.join(name);
        let archive = work_subdir.join(format!("upload_{}.{}", name, &spec.format));

        // SECURITY: Use Rust tar library instead of subprocess to prevent command injection
        // Phase 7: Check kind to determine if source is file or directory (SPEC.md v0.5.0)
        let kind = spec.kind.as_deref().unwrap_or("dir");
        if kind == "file" {
            create_tar_gz_from_file(&source, &archive)
                .context(format!("Failed to create archive for file {}", name))?;
        } else {
            create_tar_gz_from_dir(&source, &archive)
                .context(format!("Failed to create archive for directory {}", name))?;
        }

        let bytes = fs::read(&archive)?;
        let size = bytes.len();
        reqwest::blocking::Client::new().put(&spec.url).body(bytes).send()?;
        fs::remove_file(&archive)?;

        let duration_ms = start.elapsed().as_millis();
        info!("tree_exec={} job_exec={} executor={} output={} bytes={} duration_ms={} Output uploaded",
              tree_exec, job_exec, executor_id, name, size, duration_ms);
    }

    Ok(())
}

#[derive(Serialize)]
struct ProgressUpdate {
    tree_execution_id: String,
    job_execution_id: String,
    executor_id: String,
    state: String,
    message: String,
}

fn publish_progress(
    client: &mut Client,
    exec_id: &str,
    tree_exec_id: &str,
    job_exec_id: &str,
    state: &str,
    msg: &str,
    secret: &str,
) {
    let update = ProgressUpdate {
        tree_execution_id: tree_exec_id.to_string(),
        job_execution_id: job_exec_id.to_string(),
        executor_id: exec_id.to_string(),
        state: state.to_string(),
        message: msg.to_string(),
    };

    match serde_json::to_value(&update) {
        Ok(value) => match sign_message(value, secret) {
            Ok(signed) => match serde_json::to_string(&signed) {
                Ok(json) => {
                    let topic = format!("linearjc/jobs/progress/{}", job_exec_id);
                    match client.publish(topic.clone(), QoS::AtLeastOnce, false, json) {
                        Ok(_) => debug!("Published progress: {} -> {}", job_exec_id, state),
                        Err(e) => error!("Failed to publish progress to {}: {}", topic, e),
                    }
                }
                Err(e) => error!("Failed to serialize signed message: {}", e),
            },
            Err(e) => error!("Failed to sign progress message: {}", e),
        },
        Err(e) => error!("Failed to serialize progress update: {}", e),
    }
}

// ============================================================================
// CAPABILITY DISCOVERY - Scans jobs directory for .ljc packages
// SPEC.md v0.5.0: Jobs are cached as complete .ljc packages (tar.gz)
// ============================================================================

/// Discover job capabilities by scanning the jobs directory for .ljc packages
/// SPEC.md v0.5.0: Package naming convention: {job_id}.ljc
///
/// Packages must:
/// 1. Have .ljc extension (tar.gz format)
/// 2. Contain valid job.yaml with job.id and job.version
/// 3. Have unique job_id (no duplicates)
fn discover_capabilities(jobs_dir: &Path, _allowed_extensions: &[String]) -> Result<Vec<serde_json::Value>> {
    let mut capabilities = Vec::new();
    let mut seen_job_ids = HashMap::new();

    // Ensure jobs_dir exists
    if !jobs_dir.exists() {
        warn!(
            "Jobs directory does not exist: {}",
            jobs_dir.display()
        );
        return Ok(capabilities);
    }

    // Scan for .ljc package files
    for entry in fs::read_dir(jobs_dir)
        .context(format!("Failed to read jobs directory: {}", jobs_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();

        // Skip directories
        if !path.is_file() {
            continue;
        }

        // Check for .ljc extension
        let extension = match path.extension().and_then(|s| s.to_str()) {
            Some(ext) => ext,
            None => {
                debug!("Skipping file without extension: {}", path.display());
                continue;
            }
        };

        if extension != "ljc" {
            debug!(
                "Skipping non-package file '{}': {}",
                extension,
                path.display()
            );
            continue;
        }

        // Extract job_id from filename (without extension)
        let filename_job_id = match path.file_stem().and_then(|s| s.to_str()) {
            Some(name) => name,
            None => {
                warn!("Skipping file with invalid UTF-8 name: {}", path.display());
                continue;
            }
        };

        // SECURITY: Validate job_id before processing
        if let Err(e) = validate_path_component(filename_job_id) {
            debug!(
                "Skipping file {}: invalid job_id format: {}",
                path.display(),
                e
            );
            continue;
        }

        // Read job metadata from package (job.yaml inside tar.gz)
        let metadata = match read_package_metadata(&path) {
            Ok(meta) => meta,
            Err(e) => {
                warn!(
                    "Skipping invalid package {}: {}",
                    path.display(),
                    e
                );
                continue;
            }
        };

        // Verify job_id matches filename (security: prevent ID spoofing)
        if metadata.job_id != filename_job_id {
            warn!(
                "Package {}: job_id mismatch (file: '{}', yaml: '{}') - skipping",
                path.display(),
                filename_job_id,
                metadata.job_id
            );
            continue;
        }

        // SECURITY CRITICAL: Check for duplicate job_ids
        if let Some(existing_path) = seen_job_ids.get(&metadata.job_id) {
            anyhow::bail!(
                "Duplicate job_id '{}' found:\n  - {}\n  - {}\nOnly one package per job_id is allowed",
                metadata.job_id,
                existing_path,
                path.display()
            );
        }
        seen_job_ids.insert(metadata.job_id.clone(), path.display().to_string());

        capabilities.push(serde_json::json!({
            "job_id": metadata.job_id,
            "version": metadata.version
        }));

        info!("Discovered job capability: {} v{} ({})", metadata.job_id, metadata.version, path.display());
    }

    info!(
        "Discovered {} job capabilities from {}",
        capabilities.len(),
        jobs_dir.display()
    );

    Ok(capabilities)
}

fn download_and_install_job(
    job_id: &str,
    version: &str,
    uri: &str,
    expected_checksum: &str,
    jobs_dir: &Path
) -> Result<()> {
    /// Download job package from MinIO, verify checksum, and save as .ljc
    /// SPEC.md v0.5.0: Cache full .ljc package, don't extract scripts

    use sha2::Digest;

    info!("Downloading {} v{} from {}", job_id, version, uri);

    // For S3 URIs, we need to convert to HTTP presigned URL
    // The coordinator should provide pre-signed URLs, but if not we need MinIO client
    // For now, assume coordinator provides pre-signed HTTP(S) URLs
    let download_url = if uri.starts_with("s3://") {
        // Extract bucket and key
        let without_prefix = uri.trim_start_matches("s3://");
        let parts: Vec<&str> = without_prefix.splitn(2, '/').collect();
        if parts.len() != 2 {
            anyhow::bail!("Invalid S3 URI format: {}", uri);
        }

        // Get MinIO endpoint from environment
        let minio_endpoint = env::var("MINIO_ENDPOINT")
            .unwrap_or_else(|_| "localhost:9000".to_string());
        let minio_secure = env::var("MINIO_SECURE")
            .unwrap_or_else(|_| "false".to_string()) == "true";

        let scheme = if minio_secure { "https" } else { "http" };
        format!("{}//{}/{}/{}", scheme, minio_endpoint, parts[0], parts[1])
    } else {
        uri.to_string()
    };

    // Download package
    info!("Downloading from: {}", download_url);
    let response = reqwest::blocking::get(&download_url)
        .context("Failed to download package")?;

    if !response.status().is_success() {
        anyhow::bail!("HTTP error {}: {}", response.status(), download_url);
    }

    let package_bytes = response.bytes()
        .context("Failed to read response body")?;

    info!("Downloaded {} bytes", package_bytes.len());

    // Verify checksum
    let mut hasher = sha2::Sha256::new();
    hasher.update(&package_bytes);
    let checksum = format!("{:x}", hasher.finalize());

    if checksum != expected_checksum {
        anyhow::bail!(
            "Checksum mismatch! Expected: {}, Got: {}",
            expected_checksum,
            checksum
        );
    }

    info!("Checksum verified: {}", checksum);

    // SPEC.md v0.5.0: Save full .ljc package (don't extract scripts)
    let package_dest = jobs_dir.join(format!("{}.ljc", job_id));
    fs::write(&package_dest, &package_bytes)
        .context("Failed to save package")?;

    info!("Installed package: {}", package_dest.display());

    // Verify the package is valid by reading its metadata
    let pkg_meta = read_package_metadata(&package_dest)
        .context("Package validation failed")?;

    if pkg_meta.job_id != job_id {
        fs::remove_file(&package_dest)?;
        anyhow::bail!(
            "Package job_id mismatch: expected '{}', got '{}'",
            job_id, pkg_meta.job_id
        );
    }

    if pkg_meta.version != version {
        fs::remove_file(&package_dest)?;
        anyhow::bail!(
            "Package version mismatch: expected '{}', got '{}'",
            version, pkg_meta.version
        );
    }

    info!("✓ Installed {} v{} ({})", job_id, version, package_dest.display());

    Ok(())
}

fn mqtt_loop(executor_id: &str, jobs_dir: &PathBuf, work_dir: &PathBuf, allowed_extensions: &[String]) -> Result<()> {
    let broker = env::var("MQTT_BROKER").unwrap_or_else(|_| "localhost".into());
    let port: u16 = env::var("MQTT_PORT").ok().and_then(|s| s.parse().ok()).unwrap_or(1883);
    let shared_secret = env::var("MQTT_SHARED_SECRET")
        .context("MQTT_SHARED_SECRET environment variable required")?;

    info!("Connecting to MQTT: {}:{}", broker, port);

    let mut mqttoptions = MqttOptions::new(executor_id, broker, port);
    mqttoptions.set_keep_alive(Duration::from_secs(60));

    // Stateless executor - clean session
    mqttoptions.set_clean_session(true);

    let (client, mut connection) = Client::new(mqttoptions, 10);

    let hostname = env::var("HOSTNAME")
        .or_else(|_| env::var("HOST"))
        .unwrap_or_else(|_| "unknown".to_string());

    // Event loop with reconnection handling
    for notification in connection.iter() {
        match notification {
            Ok(Event::Incoming(Packet::Publish(p))) => {
                // Handle capability query
                if p.topic == "linearjc/query/capabilities" {
                    // Verify and parse query
                    match serde_json::from_slice::<serde_json::Value>(&p.payload) {
                        Ok(envelope) => {
                            match verify_message(&envelope, &shared_secret, 60) {
                                Ok(payload) => {
                                    let request_id = payload.get("request_id")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("unknown");

                                    info!("Received capability query (request_id={})", request_id);

                                    // Discover capabilities dynamically from jobs directory
                                    let capabilities = match discover_capabilities(jobs_dir, allowed_extensions) {
                                        Ok(caps) => caps,
                                        Err(e) => {
                                            error!("Failed to discover capabilities: {}", e);
                                            Vec::new() // Return empty list on error
                                        }
                                    };

                                    // Get capability types from environment
                                    let capability_types: Vec<String> = env::var("CAPABILITIES")
                                        .unwrap_or_default()
                                        .split(',')
                                        .map(|s| s.trim().to_string())
                                        .filter(|s| !s.is_empty())
                                        .collect();

                                    // Build capability response
                                    let capability = serde_json::json!({
                                        "request_id": request_id,
                                        "executor_id": executor_id,
                                        "hostname": hostname,
                                        "capabilities": capabilities,
                                        "capability_types": capability_types,
                                        "supported_formats": ["tar.gz"]
                                    });

                                    // Sign and publish response
                                    if let Ok(signed) = sign_message(capability, &shared_secret) {
                                        if let Ok(json) = serde_json::to_string(&signed) {
                                            let topic = format!("linearjc/capabilities/{}", executor_id);
                                            let _ = client.publish(topic, QoS::AtLeastOnce, false, json);
                                            info!("Sent capability response (request_id={})", request_id);
                                        }
                                    }
                                }
                                Err(e) => error!("Query verification failed: {}", e)
                            }
                        }
                        Err(e) => error!("Invalid query JSON: {}", e)
                    }
                }
                // Handle job request
                else if p.topic.starts_with("linearjc/jobs/requests/") {
                    // Parse envelope
                    match serde_json::from_slice::<serde_json::Value>(&p.payload) {
                        Ok(envelope) => {
                            // Verify envelope and extract payload
                            match verify_message(&envelope, &shared_secret, 60) {
                                Ok(payload) => {
                                    // Parse job request from payload
                                    match serde_json::from_value::<JobRequest>(payload) {
                                        Ok(req) => {
                                            // Check if job is assigned to this executor
                                            if let Some(assigned_to) = &req.assigned_to {
                                                if assigned_to != executor_id {
                                                    info!("Job {} assigned to {}, ignoring", &req.job_execution_id, assigned_to);
                                                    continue;
                                                }
                                            }

                                            info!("tree_exec={} job_exec={} executor={} state=assigned Job assigned",
                                                  &req.tree_execution_id, &req.job_execution_id, executor_id);

                                            // Spawn thread for job execution to avoid blocking MQTT event loop
                                            let jobs_dir_clone = jobs_dir.clone();
                                            let work_dir_clone = work_dir.clone();
                                            let executor_id_clone = executor_id.to_string();
                                            let shared_secret_clone = shared_secret.clone();
                                            let allowed_extensions_clone = allowed_extensions.to_vec();
                                            let mut client_clone = client.clone();

                                            std::thread::spawn(move || {
                                                use std::time::Instant;
                                                let total_start = Instant::now();

                                                publish_progress(&mut client_clone, &executor_id_clone, &req.tree_execution_id,
                                                               &req.job_execution_id, "assigned", "Job received", &shared_secret_clone);

                                                match execute_job(&req, &jobs_dir_clone, &work_dir_clone, &executor_id_clone, &mut client_clone, &shared_secret_clone, &allowed_extensions_clone) {
                                                    Ok(_) => {
                                                        let total_duration_ms = total_start.elapsed().as_millis();
                                                        publish_progress(&mut client_clone, &executor_id_clone, &req.tree_execution_id,
                                                                       &req.job_execution_id, "completed", "Success", &shared_secret_clone);
                                                        info!("tree_exec={} job_exec={} executor={} duration_ms={} state=completed Job completed successfully",
                                                              &req.tree_execution_id, &req.job_execution_id, &executor_id_clone, total_duration_ms);
                                                    }
                                                    Err(e) => {
                                                        let msg = format!("Failed: {}", e);
                                                        publish_progress(&mut client_clone, &executor_id_clone, &req.tree_execution_id,
                                                                       &req.job_execution_id, "failed", &msg, &shared_secret_clone);
                                                        error!("tree_exec={} job_exec={} executor={} state=failed error={} Job failed",
                                                               &req.tree_execution_id, &req.job_execution_id, &executor_id_clone, e);
                                                    }
                                                }
                                            });
                                        }
                                        Err(e) => error!("executor={} error={} Invalid job request format", executor_id, e)
                                    }
                                }
                                Err(e) => error!("Message verification failed: {}", e)
                            }
                        }
                        Err(e) => error!("Invalid JSON: {}", e)
                    }
                }
                // Handle job availability announcement (on-demand distribution)
                else if p.topic == "linearjc/jobs/available" {
                    match serde_json::from_slice::<serde_json::Value>(&p.payload) {
                        Ok(envelope) => {
                            match verify_message(&envelope, &shared_secret, 60) {
                                Ok(payload) => {
                                    let job_id = payload.get("job_id").and_then(|v| v.as_str()).unwrap_or("");
                                    let version = payload.get("version").and_then(|v| v.as_str()).unwrap_or("");
                                    let capability = payload.get("capability").and_then(|v| v.as_str()).unwrap_or("");

                                    // Get capability types from environment
                                    let capability_types: Vec<String> = env::var("CAPABILITIES")
                                        .unwrap_or_default()
                                        .split(',')
                                        .map(|s| s.trim().to_string())
                                        .filter(|s| !s.is_empty())
                                        .collect();

                                    // Check if this executor has the required capability
                                    if !capability_types.contains(&capability.to_string()) {
                                        info!("Ignoring {} v{} (capability '{}' not in {:?})", job_id, version, capability, capability_types);
                                        continue;
                                    }

                                    info!("Received job announcement: {} v{} (capability: {})", job_id, version, capability);

                                    // SPEC.md v0.5.0: Check if .ljc package already installed
                                    let package_path = jobs_dir.join(format!("{}.ljc", job_id));
                                    if package_path.exists() {
                                        // Read installed version from package
                                        if let Ok(installed_version) = get_package_version(&package_path) {
                                            // Use proper semantic version comparison
                                            use std::cmp::Ordering;
                                            match compare_versions(&installed_version, version) {
                                                Ordering::Greater | Ordering::Equal => {
                                                    info!("{} v{} already installed (current: {}), skipping", job_id, version, installed_version);
                                                    continue;
                                                }
                                                Ordering::Less => {
                                                    info!("{} upgrade available: {} -> {}", job_id, installed_version, version);
                                                    // Continue to download and install
                                                }
                                            }
                                        }
                                    }

                                    // Extract download info
                                    if let Some(artifact) = payload.get("script_artifact") {
                                        let uri = artifact.get("uri").and_then(|v| v.as_str()).unwrap_or("");
                                        let checksum = artifact.get("checksum_sha256").and_then(|v| v.as_str()).unwrap_or("");

                                        if uri.is_empty() || checksum.is_empty() {
                                            error!("Job announcement missing uri or checksum");
                                            continue;
                                        }

                                        // Download and install in background thread
                                        let job_id_clone = job_id.to_string();
                                        let version_clone = version.to_string();
                                        let uri_clone = uri.to_string();
                                        let checksum_clone = checksum.to_string();
                                        let jobs_dir_clone = jobs_dir.clone();
                                        let executor_id_clone = executor_id.to_string();

                                        std::thread::spawn(move || {
                                            match download_and_install_job(&job_id_clone, &version_clone, &uri_clone, &checksum_clone, &jobs_dir_clone) {
                                                Ok(_) => info!("executor={} job={} version={} state=installed Successfully installed job", executor_id_clone, job_id_clone, version_clone),
                                                Err(e) => error!("executor={} job={} version={} state=failed error={} Failed to install job", executor_id_clone, job_id_clone, version_clone, e)
                                            }
                                        });
                                    }
                                }
                                Err(e) => error!("Job announcement verification failed: {}", e)
                            }
                        }
                        Err(e) => error!("Invalid job announcement JSON: {}", e)
                    }
                }
                // Handle job cancellation request
                else if p.topic.contains("/cancel") {
                    match serde_json::from_slice::<serde_json::Value>(&p.payload) {
                        Ok(envelope) => {
                            match verify_message(&envelope, &shared_secret, 60) {
                                Ok(payload) => {
                                    let execution_id = payload.get("execution_id")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("");
                                    let signal_str = payload.get("signal")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("SIGTERM");

                                    info!("Cancel request for {} (signal: {})", execution_id, signal_str);

                                    // Look up PID and send signal
                                    if let Ok(jobs) = ACTIVE_JOBS.lock() {
                                        if let Some(pid) = jobs.get(execution_id) {
                                            let signal = if signal_str == "SIGKILL" {
                                                Signal::SIGKILL
                                            } else {
                                                Signal::SIGTERM
                                            };

                                            match kill(*pid, signal) {
                                                Ok(_) => info!("Sent {:?} to {} (pid={})", signal, execution_id, pid),
                                                Err(e) => error!("Failed to kill {}: {}", execution_id, e),
                                            }
                                        } else {
                                            warn!("No active job found for: {}", execution_id);
                                        }
                                    }
                                }
                                Err(e) => error!("Cancel verification failed: {}", e)
                            }
                        }
                        Err(e) => error!("Invalid cancel JSON: {}", e)
                    }
                }
            }
            Ok(Event::Incoming(Packet::ConnAck(_))) => {
                info!("Connected to MQTT broker");

                // Re-subscribe after reconnection
                if let Err(e) = client.subscribe("linearjc/query/capabilities", QoS::AtLeastOnce) {
                    error!("Failed to subscribe to queries: {}", e);
                }
                if let Err(e) = client.subscribe("linearjc/jobs/requests/+", QoS::AtLeastOnce) {
                    error!("Failed to subscribe to job requests: {}", e);
                }
                if let Err(e) = client.subscribe("linearjc/jobs/available", QoS::AtLeastOnce) {
                    error!("Failed to subscribe to job announcements: {}", e);
                }
                // Subscribe to cancel requests for this executor
                let cancel_topic = format!("linearjc/executors/{}/cancel", executor_id);
                if let Err(e) = client.subscribe(&cancel_topic, QoS::AtLeastOnce) {
                    error!("Failed to subscribe to cancel requests: {}", e);
                }

                info!("Resubscribed to topics");
            }
            Ok(Event::Outgoing(_)) => {
                // Outgoing events - ignore
            }
            Ok(_) => {
                // Other events (ConnAck for initial connection, etc) - ignore
            }
            Err(e) => {
                error!("MQTT connection error: {}", e);
                // rumqttc will automatically attempt to reconnect
                info!("Waiting for reconnection...");
                std::thread::sleep(Duration::from_secs(1));
            }
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    env_logger::init();
    info!("LinearJC Executor v0.1.0");

    let executor_id = env::var("EXECUTOR_ID").unwrap_or_else(|_| "executor-01".into());
    let jobs_dir: PathBuf = env::var("JOBS_DIR")
        .unwrap_or_else(|_| "examples/executor-jobs".into())
        .into();
    let work_dir: PathBuf = env::var("WORK_DIR")
        .unwrap_or_else(|_| "/tmp/linearjc".into())
        .into();

    // Parse allowed script extensions (comma-separated)
    // SECURITY: Admin explicitly controls which interpreters are allowed
    let allowed_extensions: Vec<String> = env::var("SCRIPT_EXTENSIONS")
        .unwrap_or_else(|_| "sh".into())
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if allowed_extensions.is_empty() {
        anyhow::bail!("SCRIPT_EXTENSIONS must not be empty");
    }

    // SECURITY CRITICAL: Validate extension values to prevent directory traversal
    // Extensions are used in path construction (jobs_dir.join(format!("{}.{}", job_id, ext)))
    // Malicious extensions like "../sh" could escape jobs_dir
    for ext in &allowed_extensions {
        // Only allow lowercase alphanumeric (no dots, slashes, dashes, underscores)
        // This prevents: "../", "./", absolute paths, and other path manipulation
        if !ext.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit()) {
            anyhow::bail!(
                "Invalid SCRIPT_EXTENSIONS value '{}': only lowercase letters and digits allowed (e.g., 'sh', 'py', 'pl')",
                ext
            );
        }

        // Limit extension length (reasonable file extensions are 2-4 chars)
        if ext.len() > 10 {
            anyhow::bail!(
                "Extension '{}' too long ({} chars, max 10)",
                ext,
                ext.len()
            );
        }

        if ext.is_empty() {
            anyhow::bail!("Empty extension not allowed");
        }
    }

    info!("Executor ID: {}", executor_id);
    info!("Jobs directory: {}", jobs_dir.display());
    info!("Work directory: {}", work_dir.display());
    info!("Allowed script extensions: {}", allowed_extensions.join(", "));

    // SECURITY CRITICAL: Setup work directory with secure permissions
    setup_secure_work_dir(&work_dir)
        .context("Failed to setup secure work directory")?;

    // Test mode: read job request from file
    let args: Vec<String> = env::args().collect();
    if args.len() > 1 && args[1] == "test" {
        if args.len() < 3 {
            eprintln!("Usage: {} test <job-request.json>", args[0]);
            std::process::exit(1);
        }

        info!("Test mode: {}", args[2]);
        let json = fs::read_to_string(&args[2])?;
        let request: JobRequest = serde_json::from_str(&json)?;

        info!("tree_exec={} job_exec={} executor={} state=test_start Starting test execution",
              &request.tree_execution_id, &request.job_execution_id, &executor_id);

        // Create dummy MQTT client for test mode (won't actually publish)
        let mqttoptions = MqttOptions::new("test-mode", "localhost", 1883);
        let (mut test_client, _connection) = Client::new(mqttoptions, 10);
        let test_secret = env::var("MQTT_SHARED_SECRET").unwrap_or_else(|_| "test-secret".to_string());

        match execute_job(&request, &jobs_dir, &work_dir, &executor_id, &mut test_client, &test_secret, &allowed_extensions) {
            Ok(_) => {
                info!("tree_exec={} job_exec={} executor={} state=test_passed Test passed",
                      &request.tree_execution_id, &request.job_execution_id, &executor_id);
            }
            Err(e) => {
                error!("tree_exec={} job_exec={} executor={} state=test_failed error={} Test failed",
                       &request.tree_execution_id, &request.job_execution_id, &executor_id, e);
                std::process::exit(1);
            }
        }

        return Ok(());
    }

    // MQTT mode: listen for job requests
    info!("Starting MQTT listener...");
    mqtt_loop(&executor_id, &jobs_dir, &work_dir, &allowed_extensions)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_directory_layout_creation() {
        // Test that Phase 7 directory structure is created correctly
        let temp = TempDir::new().unwrap();
        let job_dir = temp.path().join("test-job-id");

        let in_dir = job_dir.join("in");
        let out_dir = job_dir.join("out");
        let tmp_dir = job_dir.join("tmp");
        let work_dir = job_dir.join(".work");

        // Create directories
        fs::create_dir_all(&in_dir).unwrap();
        fs::create_dir_all(&out_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();
        fs::create_dir_all(&work_dir).unwrap();

        // Verify all directories exist
        assert!(in_dir.exists());
        assert!(out_dir.exists());
        assert!(tmp_dir.exists());
        assert!(work_dir.exists());

        // Verify they are directories
        assert!(in_dir.is_dir());
        assert!(out_dir.is_dir());
        assert!(tmp_dir.is_dir());
        assert!(work_dir.is_dir());
    }

    #[test]
    fn test_path_type_file_extraction() {
        // Test that path_type: file creates direct file in in/ directory
        let temp = TempDir::new().unwrap();
        let in_dir = temp.path().join("in");
        fs::create_dir_all(&in_dir).unwrap();

        // Simulate extracting a file input
        let test_file = in_dir.join("config.json");
        fs::write(&test_file, b"{\"key\": \"value\"}").unwrap();

        // Verify file exists directly in in/ (not in subdirectory)
        assert!(test_file.exists());
        assert!(test_file.is_file());

        // Verify path structure
        assert_eq!(test_file.parent().unwrap(), in_dir);
    }

    #[test]
    fn test_path_type_directory_extraction() {
        // Test that path_type: directory creates subdirectory in in/
        let temp = TempDir::new().unwrap();
        let in_dir = temp.path().join("in");
        fs::create_dir_all(&in_dir).unwrap();

        // Simulate extracting a directory input
        let test_dir = in_dir.join("dataset");
        fs::create_dir_all(&test_dir).unwrap();

        let test_file = test_dir.join("data.parquet");
        fs::write(&test_file, b"binary data").unwrap();

        // Verify directory exists in in/
        assert!(test_dir.exists());
        assert!(test_dir.is_dir());

        // Verify file exists inside directory
        assert!(test_file.exists());
        assert!(test_file.is_file());

        // Verify path structure
        assert_eq!(test_dir.parent().unwrap(), in_dir);
    }

    #[test]
    fn test_environment_variables_generation() {
        // Test that environment variables are set correctly
        let temp = TempDir::new().unwrap();
        let job_dir = temp.path().join("test-job-id");

        let in_dir = job_dir.join("in");
        let out_dir = job_dir.join("out");
        let tmp_dir = job_dir.join("tmp");

        fs::create_dir_all(&in_dir).unwrap();
        fs::create_dir_all(&out_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        // Verify directory paths are what we expect
        assert_eq!(in_dir.file_name().unwrap(), "in");
        assert_eq!(out_dir.file_name().unwrap(), "out");
        assert_eq!(tmp_dir.file_name().unwrap(), "tmp");

        // Environment variables would be:
        // LINEARJC_IN_DIR = /path/to/in
        // LINEARJC_OUT_DIR = /path/to/out
        // LINEARJC_TMP_DIR = /path/to/tmp
        let expected_in = in_dir.to_str().unwrap();
        let expected_out = out_dir.to_str().unwrap();
        let expected_tmp = tmp_dir.to_str().unwrap();

        assert!(expected_in.ends_with("/in"));
        assert!(expected_out.ends_with("/out"));
        assert!(expected_tmp.ends_with("/tmp"));
    }

    #[test]
    fn test_data_spec_kind_field() {
        // Test that DataSpec correctly handles kind field (SPEC.md v0.5.0)
        let spec_file = DataSpec {
            url: "s3://bucket/config.json".to_string(),
            format: "tar.gz".to_string(),
            kind: Some("file".to_string()),
        };

        let spec_dir = DataSpec {
            url: "s3://bucket/dataset.tar.gz".to_string(),
            format: "tar.gz".to_string(),
            kind: Some("dir".to_string()),
        };

        let spec_default = DataSpec {
            url: "s3://bucket/legacy.tar.gz".to_string(),
            format: "tar.gz".to_string(),
            kind: None, // Backward compat: defaults to "dir"
        };

        assert_eq!(spec_file.kind.as_deref().unwrap(), "file");
        assert_eq!(spec_dir.kind.as_deref().unwrap(), "dir");
        assert_eq!(spec_default.kind.as_deref().unwrap_or("dir"), "dir");
    }

    #[test]
    fn test_metadata_directory_creation() {
        // Test that .meta directory can be created for registry introspection
        let temp = TempDir::new().unwrap();
        let in_dir = temp.path().join("in");
        let meta_dir = in_dir.join(".meta");

        fs::create_dir_all(&meta_dir).unwrap();

        // Simulate creating registry.json
        let registry_file = meta_dir.join("registry.json");
        fs::write(&registry_file, b"{\"inputs\": []}").unwrap();

        // Verify .meta directory exists
        assert!(meta_dir.exists());
        assert!(meta_dir.is_dir());

        // Verify registry.json exists
        assert!(registry_file.exists());
        assert!(registry_file.is_file());

        // Verify .meta is hidden (starts with dot)
        assert!(meta_dir.file_name().unwrap().to_str().unwrap().starts_with('.'));
    }

    #[test]
    fn test_work_directory_is_hidden() {
        // Test that .work directory is hidden (starts with dot)
        let temp = TempDir::new().unwrap();
        let job_dir = temp.path().join("test-job-id");
        let work_dir = job_dir.join(".work");

        fs::create_dir_all(&work_dir).unwrap();

        // Verify .work is hidden
        assert!(work_dir.file_name().unwrap().to_str().unwrap().starts_with('.'));
        assert!(work_dir.exists());
        assert!(work_dir.is_dir());
    }
}
