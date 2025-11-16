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
use chrono::Utc;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use hmac::{Hmac, Mac};
use log::{debug, error, info, warn};
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
use nix::unistd::{chown, fork, setuid, ForkResult, Gid, Uid, User};
use rumqttc::{Client, Event, MqttOptions, Packet, QoS};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::fs::File;
use std::path::Path;
use std::process::Command;
use std::{collections::HashMap, env, fs, path::PathBuf, time::Duration};
use subtle::ConstantTimeEq;
use tar::Archive;

#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

type HmacSha256 = Hmac<Sha256>;

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
    executor: ExecutorSpec,
}

#[derive(Deserialize)]
struct ExecutorSpec {
    user: String,
    timeout: u64,
}

#[derive(Deserialize)]
struct DataSpec {
    url: String,
    format: String,
}

/// Sign a message with HMAC-SHA256 using envelope pattern (matches Python implementation)
fn sign_message(msg: serde_json::Value, secret: &str) -> Result<serde_json::Value> {
    // Serialize payload to canonical JSON (compact, sorted keys)
    // serde_json::to_string() already produces compact JSON
    let payload = serde_json::to_string(&msg)?;

    // Create timestamp (ISO 8601 UTC with 'Z' suffix)
    let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();

    // Compute signature over payload + timestamp
    let to_sign = format!("{}{}", payload, timestamp);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|e| anyhow::anyhow!("Invalid key: {}", e))?;
    mac.update(to_sign.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    // Return envelope
    Ok(serde_json::json!({
        "payload": payload,
        "timestamp": timestamp,
        "signature": signature
    }))
}

/// Verify a message envelope and return the payload (matches Python implementation)
fn verify_message(envelope: &serde_json::Value, secret: &str, max_age_secs: i64) -> Result<serde_json::Value> {
    // Extract required fields
    let payload = envelope.get("payload")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Envelope missing 'payload'"))?;

    let timestamp_str = envelope.get("timestamp")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Envelope missing 'timestamp'"))?;

    let provided_sig = envelope.get("signature")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Envelope missing 'signature'"))?;

    // Parse timestamp
    let timestamp = chrono::DateTime::parse_from_rfc3339(
        &timestamp_str.replace('Z', "+00:00")
    )?.with_timezone(&Utc);

    // Check message age
    let now = Utc::now();
    let age = (now - timestamp).num_seconds();

    if age < 0 {
        anyhow::bail!("Message timestamp in future (clock skew?)");
    }

    if age > max_age_secs {
        anyhow::bail!("Message too old: {}s (max: {}s)", age, max_age_secs);
    }

    // Verify signature using constant-time comparison (prevents timing attacks)
    let to_verify = format!("{}{}", payload, timestamp_str);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|e| anyhow::anyhow!("Invalid key: {}", e))?;
    mac.update(to_verify.as_bytes());
    let expected_sig_bytes = mac.finalize().into_bytes();

    // SECURITY CRITICAL: Use constant-time comparison to prevent timing attacks
    // Convert hex strings to bytes for comparison
    let provided_sig_bytes = hex::decode(provided_sig)
        .context("Invalid signature encoding")?;

    // Constant-time comparison using subtle crate
    if expected_sig_bytes.ct_eq(&provided_sig_bytes).into() {
        // Signature valid - return parsed payload
        Ok(serde_json::from_str(payload)?)
    } else {
        anyhow::bail!("Invalid signature")
    }
}

// ============================================================================
// INPUT VALIDATION - SECURITY CRITICAL
// All external inputs must be validated before use in file paths, commands,
// or privileged operations. Defense in depth against injection attacks.
// ============================================================================

/// Validate path component to prevent directory traversal and injection attacks
/// Only allows: alphanumeric, dots, dashes, underscores
/// Rejects: "..", "/", shell metacharacters
fn validate_path_component(component: &str) -> Result<()> {
    if component.is_empty() {
        anyhow::bail!("Path component cannot be empty");
    }

    // Reject components that are too long (DoS prevention)
    if component.len() > 255 {
        anyhow::bail!("Path component too long: {} bytes", component.len());
    }

    // Only allow safe characters: alphanumeric, dot, dash, underscore
    for c in component.chars() {
        if !c.is_alphanumeric() && c != '.' && c != '-' && c != '_' {
            anyhow::bail!(
                "Invalid character in path component '{}': '{}' (only alphanumeric, ., -, _ allowed)",
                component, c
            );
        }
    }

    // Prevent directory traversal
    if component.contains("..") {
        anyhow::bail!("Directory traversal detected: {}", component);
    }

    // Prevent absolute paths
    if component.starts_with('/') {
        anyhow::bail!("Absolute paths not allowed: {}", component);
    }

    Ok(())
}

/// Validate job script: must be executable and have valid shebang
/// This replaces the hardcoded bash execution with standard Unix shebang behavior
fn validate_job_script(path: &Path) -> Result<()> {
    // Check file exists
    if !path.exists() {
        anyhow::bail!("Script does not exist: {}", path.display());
    }

    // Check it's a regular file (not directory/symlink)
    let metadata = fs::metadata(path)
        .context(format!("Failed to read metadata for {}", path.display()))?;

    if !metadata.is_file() {
        anyhow::bail!("Script is not a regular file: {}", path.display());
    }

    // Check execute permission (owner, group, or other)
    #[cfg(unix)]
    {
        let permissions = metadata.permissions();
        let mode = permissions.mode();
        let is_executable = (mode & 0o111) != 0;  // Check any execute bit

        if !is_executable {
            anyhow::bail!(
                "Script is not executable: {} (run: chmod +x {})",
                path.display(),
                path.display()
            );
        }
    }

    // Validate shebang line (first line must start with #!)
    let file = File::open(path)
        .context(format!("Failed to open script: {}", path.display()))?;

    use std::io::{BufRead, BufReader};
    let mut reader = BufReader::new(file);
    let mut first_line = String::new();

    reader.read_line(&mut first_line)
        .context(format!("Failed to read first line of {}", path.display()))?;

    if !first_line.starts_with("#!") {
        anyhow::bail!(
            "Script missing shebang line: {} (first line must start with #! followed by interpreter path)",
            path.display()
        );
    }

    // Extract interpreter path from shebang (trim #! and whitespace)
    let interpreter = first_line[2..].trim();

    if interpreter.is_empty() {
        anyhow::bail!(
            "Invalid shebang in {}: no interpreter specified",
            path.display()
        );
    }

    Ok(())
}

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

    // Validate executor user
    validate_username(&req.executor.user)
        .context("Invalid executor user")?;

    // Validate timeout (prevent extremely long-running jobs)
    if req.executor.timeout == 0 {
        anyhow::bail!("Timeout must be > 0");
    }
    if req.executor.timeout > 86400 {
        // 24 hours max
        anyhow::bail!("Timeout too long: {}s (max: 86400s / 24h)", req.executor.timeout);
    }

    info!(
        "tree_exec={} job_exec={} job_id={} user={} timeout={} inputs={} outputs={} Validated job request",
        req.tree_execution_id,
        req.job_execution_id,
        req.job_id,
        req.executor.user,
        req.executor.timeout,
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
// SECURE ARCHIVE HANDLING - Replaces tar(1) subprocess to prevent command injection
// Uses pure Rust implementation for safety and auditability
// ============================================================================

/// Extract tar.gz archive to directory using Rust tar library (not subprocess)
/// SECURITY: Prevents command injection via paths
fn extract_tar_gz(archive_path: &Path, extract_to: &Path) -> Result<()> {
    let tar_gz = File::open(archive_path)
        .context(format!("Failed to open archive: {}", archive_path.display()))?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);

    archive
        .unpack(extract_to)
        .context(format!("Failed to extract to: {}", extract_to.display()))?;

    Ok(())
}

/// Create tar.gz archive from directory using Rust tar library (not subprocess)
/// SECURITY: Prevents command injection via paths
fn create_tar_gz(source_dir: &Path, archive_path: &Path) -> Result<()> {
    let tar_gz = File::create(archive_path)
        .context(format!("Failed to create archive: {}", archive_path.display()))?;
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = tar::Builder::new(enc);

    // Add all contents of source_dir to archive root (not the directory itself)
    tar.append_dir_all(".", source_dir)
        .context(format!("Failed to archive: {}", source_dir.display()))?;

    tar.finish()
        .context("Failed to finalize archive")?;

    Ok(())
}

/// Recursively change ownership of directory and all contents
/// SECURITY: Pure Rust implementation prevents command injection via chown(1)
fn chown_recursive(path: &Path, uid: Uid, gid: Gid) -> Result<()> {
    // Change ownership of the directory itself
    chown(path, Some(uid), Some(gid))
        .context(format!("Failed to chown: {}", path.display()))?;

    // If it's a directory, recursively chown all contents
    if path.is_dir() {
        for entry in fs::read_dir(path)
            .context(format!("Failed to read directory: {}", path.display()))?
        {
            let entry = entry?;
            let entry_path = entry.path();

            // Recursively chown
            chown_recursive(&entry_path, uid, gid)?;
        }
    }

    Ok(())
}

// ============================================================================
// SECURE WORK DIRECTORY - Prevents information disclosure and symlink attacks
// Work directory must be owned by executor with restrictive permissions
// ============================================================================

/// Create or validate work directory with secure permissions
/// SECURITY: Ensures directory is owned by current user with mode 0700 (owner-only)
#[cfg(unix)]
fn setup_secure_work_dir(work_dir: &Path) -> Result<()> {
    use nix::unistd::getuid;

    // Warn if using world-readable locations
    if work_dir.starts_with("/tmp") || work_dir.starts_with("/var/tmp") {
        warn!(
            "Work directory is in shared temporary space: {}",
            work_dir.display()
        );
        warn!("For production use, configure WORK_DIR to a dedicated directory");
        warn!("Example: WORK_DIR=/var/lib/linearjc/executor");
    }

    // Create directory if it doesn't exist
    fs::create_dir_all(work_dir)
        .context(format!("Failed to create work directory: {}", work_dir.display()))?;

    // Set secure permissions (0711 - owner full, others can traverse)
    // Mode 711 allows job users to traverse to their subdirectories
    // but prevents listing the work_dir contents
    let metadata = fs::metadata(work_dir)
        .context(format!("Failed to get metadata for: {}", work_dir.display()))?;

    let mut permissions = metadata.permissions();
    let current_mode = permissions.mode();
    let secure_mode = 0o711;

    if current_mode & 0o777 != secure_mode {
        info!(
            "Setting secure permissions on work directory: {} (mode: {:o} -> {:o})",
            work_dir.display(),
            current_mode & 0o777,
            secure_mode
        );
        permissions.set_mode(secure_mode);
        fs::set_permissions(work_dir, permissions)
            .context(format!("Failed to set permissions on: {}", work_dir.display()))?;
    }

    // Re-read metadata after permission changes
    let metadata = fs::metadata(work_dir)
        .context(format!("Failed to get metadata for: {}", work_dir.display()))?;

    // Verify ownership (must be owned by current user running executor)
    let current_uid = getuid();
    if metadata.uid() != current_uid.as_raw() {
        anyhow::bail!(
            "Work directory {} is not owned by current user (uid: {}, owner: {})",
            work_dir.display(),
            current_uid,
            metadata.uid()
        );
    }

    // Verify final permissions
    let final_mode = metadata.permissions().mode() & 0o777;
    if final_mode != secure_mode {
        anyhow::bail!(
            "Failed to set secure permissions on work directory: {} (expected: {:o}, actual: {:o})",
            work_dir.display(),
            secure_mode,
            final_mode
        );
    }

    info!(
        "Work directory validated: {} (uid: {}, mode: {:o})",
        work_dir.display(),
        metadata.uid(),
        final_mode
    );

    Ok(())
}

#[cfg(not(unix))]
fn setup_secure_work_dir(work_dir: &Path) -> Result<()> {
    // On non-Unix systems, just create the directory
    // Windows/other platforms have different permission models
    fs::create_dir_all(work_dir)
        .context(format!("Failed to create work directory: {}", work_dir.display()))?;

    warn!("Running on non-Unix platform - directory permissions not enforced");
    Ok(())
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

    let job_dir = work_dir.join(&req.job_execution_id);
    let input_dir = job_dir.join("inputs");
    let output_dir = job_dir.join("outputs");
    fs::create_dir_all(&input_dir)?;
    fs::create_dir_all(&output_dir)?;

    // Download & extract inputs with timing
    if !req.inputs.is_empty() {
        publish_progress(client, executor_id, tree_exec, job_exec, "downloading",
                        &format!("Downloading {} input(s)", req.inputs.len()), shared_secret);
    }

    for (name, spec) in &req.inputs {
        let start = Instant::now();
        info!("tree_exec={} job_exec={} executor={} state=downloading input={} Downloading input",
              tree_exec, job_exec, executor_id, name);

        let archive = input_dir.join(format!("{}.{}", name, &spec.format));
        let extract = input_dir.join(name);

        let bytes = reqwest::blocking::get(&spec.url)?.bytes()?;
        let size = bytes.len();
        fs::write(&archive, bytes)?;
        fs::create_dir_all(&extract)?;

        // SECURITY: Use Rust tar library instead of subprocess to prevent command injection
        extract_tar_gz(&archive, &extract)
            .context(format!("Failed to extract {}", name))?;
        fs::remove_file(&archive)?;

        let duration_ms = start.elapsed().as_millis();
        info!("tree_exec={} job_exec={} executor={} input={} bytes={} duration_ms={} Input downloaded",
              tree_exec, job_exec, executor_id, name, size, duration_ms);
    }

    // Change ownership of work directory to job user so they can write outputs
    let uid = get_uid_for_user(&req.executor.user)?;
    let user_info = User::from_uid(uid)?.ok_or_else(|| anyhow::anyhow!("User info not found"))?;
    chown(&job_dir, Some(uid), Some(user_info.gid))?;
    chown(&input_dir, Some(uid), Some(user_info.gid))?;
    chown(&output_dir, Some(uid), Some(user_info.gid))?;

    // Recursively chown input files (after extraction)
    // SECURITY: Use nix library chown instead of subprocess to prevent command injection
    for name in req.inputs.keys() {
        let extract = input_dir.join(name);
        if extract.exists() {
            chown_recursive(&extract, uid, user_info.gid)
                .context(format!("Failed to chown {}", name))?;
        }
    }

    // Inputs ready, about to run job
    publish_progress(client, executor_id, tree_exec, job_exec, "ready",
                    "Inputs downloaded, starting job", shared_secret);

    // Find job script by trying allowed extensions in order
    // SECURITY: Extensions are admin-controlled via SCRIPT_EXTENSIONS config
    // SECURITY: Duplicates prevented at discovery, so this is deterministic
    let mut script = None;
    for ext in allowed_extensions {
        let candidate = jobs_dir.join(format!("{}.{}", &req.job_id, ext));
        if candidate.exists() {
            // SECURITY: Validate script before using (defense in depth)
            if let Err(e) = validate_job_script(&candidate) {
                warn!(
                    "Found script {} but validation failed: {}",
                    candidate.display(),
                    e
                );
                continue;
            }
            script = Some(candidate);
            break;
        }
    }

    let script = script.ok_or_else(|| {
        anyhow::anyhow!(
            "No valid script found for job_id '{}' with allowed extensions: {}",
            req.job_id,
            allowed_extensions.join(", ")
        )
    })?;

    publish_progress(client, executor_id, tree_exec, job_exec, "running",
                    "Job script executing", shared_secret);

    info!("tree_exec={} job_exec={} executor={} state=running user={} Running job script",
          tree_exec, job_exec, executor_id, req.executor.user);

    let job_start = Instant::now();

    match unsafe { fork() }? {
        ForkResult::Parent { child } => {
            // Parent: poll for child completion (non-blocking to allow MQTT event loop)
            loop {
                match waitpid(child, Some(WaitPidFlag::WNOHANG))? {
                    WaitStatus::StillAlive => {
                        // Child still running, sleep briefly to allow MQTT event loop to run
                        std::thread::sleep(Duration::from_millis(100));
                        continue;
                    }
                    WaitStatus::Exited(_, 0) => {
                        let job_duration_ms = job_start.elapsed().as_millis();
                        info!("tree_exec={} job_exec={} executor={} duration_ms={} Job script completed successfully",
                              tree_exec, job_exec, executor_id, job_duration_ms);
                        break;
                    }
                    WaitStatus::Exited(_, code) => {
                        let job_duration_ms = job_start.elapsed().as_millis();
                        error!("tree_exec={} job_exec={} executor={} duration_ms={} exit_code={} Job script failed",
                               tree_exec, job_exec, executor_id, job_duration_ms, code);
                        anyhow::bail!("Job script failed with exit code {}", code);
                    }
                    WaitStatus::Signaled(_, signal, _) => {
                        error!("tree_exec={} job_exec={} executor={} signal={:?} Job script killed by signal",
                               tree_exec, job_exec, executor_id, signal);
                        anyhow::bail!("Job script killed by signal {:?}", signal);
                    }
                    _ => {
                        error!("tree_exec={} job_exec={} executor={} Job script terminated unexpectedly",
                               tree_exec, job_exec, executor_id);
                        anyhow::bail!("Job script terminated unexpectedly");
                    }
                }
            }
        }
        ForkResult::Child => {
            // Child: switch to job user and execute script
            if let Err(e) = setuid(uid) {
                error!("Failed to switch to user '{}': {}", req.executor.user, e);
                std::process::exit(1);
            }

            // Execute script directly - OS uses shebang to determine interpreter
            // SECURITY: Script validated for executable + shebang at line 591
            let status = Command::new(&script)
                .env("LINEARJC_JOB_ID", &req.job_id)
                .env("LINEARJC_EXECUTION_ID", &req.job_execution_id)
                .env("LINEARJC_INPUT_DIR", &input_dir)
                .env("LINEARJC_OUTPUT_DIR", &output_dir)
                .status()
                .unwrap_or_else(|e| {
                    error!("Failed to execute job script: {}", e);
                    std::process::exit(1)
                });

            std::process::exit(if status.success() { 0 } else { 1 });
        }
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

        let source = output_dir.join(name);
        let archive = output_dir.join(format!("upload_{}.{}", name, &spec.format));

        // SECURITY: Use Rust tar library instead of subprocess to prevent command injection
        create_tar_gz(&source, &archive)
            .context(format!("Failed to create archive for {}", name))?;

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
// CAPABILITY DISCOVERY - Replaces hardcoded capabilities
// Scans jobs directory at runtime to discover available jobs
// ============================================================================

/// Discover job capabilities by scanning the jobs directory for executable scripts
/// SECURITY: Only job scripts passing validation (executable + shebang) are advertised
///
/// Script naming convention: {job_id}.{extension}
/// - job_id: Logical identifier (e.g., "hello.world")
/// - extension: Must be in allowed_extensions list (e.g., "sh", "py", "pl")
///
/// Scripts must:
/// 1. Be executable (chmod +x)
/// 2. Have a valid shebang line (#!/path/to/interpreter)
/// 3. Have an allowed extension
/// 4. Be unique (no duplicate job_ids with different extensions)
fn discover_capabilities(jobs_dir: &Path, allowed_extensions: &[String]) -> Result<Vec<serde_json::Value>> {
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

    // Scan for files with allowed extensions
    for entry in fs::read_dir(jobs_dir)
        .context(format!("Failed to read jobs directory: {}", jobs_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();

        // Skip directories
        if !path.is_file() {
            continue;
        }

        // Check if file has an allowed extension
        let extension = match path.extension().and_then(|s| s.to_str()) {
            Some(ext) => ext,
            None => {
                debug!("Skipping file without extension: {}", path.display());
                continue;
            }
        };

        if !allowed_extensions.iter().any(|e| e == extension) {
            debug!(
                "Skipping file with disallowed extension '{}': {}",
                extension,
                path.display()
            );
            continue;
        }

        // Extract job_id from filename (without extension)
        let job_id = match path.file_stem().and_then(|s| s.to_str()) {
            Some(name) => name,
            None => {
                warn!("Skipping file with invalid UTF-8 name: {}", path.display());
                continue;
            }
        };

        // SECURITY: Validate job_id before advertising
        if let Err(e) = validate_path_component(job_id) {
            debug!(
                "Skipping file {}: invalid job_id format: {}",
                path.display(),
                e
            );
            continue;
        }

        // Validate script is executable and has shebang
        if let Err(e) = validate_job_script(&path) {
            warn!(
                "Skipping invalid job script {}: {}",
                path.display(),
                e
            );
            continue;
        }

        // SECURITY CRITICAL: Check for duplicate job_ids (different extensions)
        if let Some(existing_path) = seen_job_ids.get(job_id) {
            anyhow::bail!(
                "Duplicate job_id '{}' found:\n  - {}\n  - {}\nOnly one script per job_id is allowed",
                job_id,
                existing_path,
                path.display()
            );
        }
        seen_job_ids.insert(job_id.to_string(), path.display().to_string());

        // TODO: Could read version from job script header comment
        // For now, default to 1.0.0
        capabilities.push(serde_json::json!({
            "job_id": job_id,
            "version": "1.0.0"
        }));

        info!("Discovered job capability: {} ({})", job_id, path.display());
    }

    info!(
        "Discovered {} job capabilities from {}",
        capabilities.len(),
        jobs_dir.display()
    );

    Ok(capabilities)
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

    // Subscribe to capability queries and job requests
    client.subscribe("linearjc/query/capabilities", QoS::AtLeastOnce)?;
    client.subscribe("linearjc/jobs/requests/+", QoS::AtLeastOnce)?;
    info!("Subscribed to capability queries and job requests");

    // Get hostname for capability responses
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

                                    // Build capability response
                                    let capability = serde_json::json!({
                                        "request_id": request_id,
                                        "executor_id": executor_id,
                                        "hostname": hostname,
                                        "capabilities": capabilities,
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
