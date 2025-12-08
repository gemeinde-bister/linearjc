//! Process execution for LinearJC.
//!
//! Provides secure job execution with:
//! - Fork/exec model for isolation
//! - Privilege dropping (setuid to job user)
//! - Script validation (shebang, permissions)
//! - Timeout enforcement
//!
//! This module is shared between executor and ljc tool.
//!
//! # Architecture
//!
//! The execution model supports two use patterns:
//!
//! 1. **Simple blocking execution** (`execute_isolated`, `execute_simple`):
//!    For tools like `ljc test` that just need to run a job and get the result.
//!
//! 2. **Spawned execution** (`spawn_isolated`, `wait_for_job`):
//!    For the executor which needs to track PIDs for cancellation and poll
//!    non-blocking while processing MQTT events.

use anyhow::{Context, Result};
use log::{debug, error, info, warn};
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

#[cfg(unix)]
use cgroups_rs::Cgroup;
#[cfg(unix)]
use nix::sys::signal::{kill, Signal};
#[cfg(unix)]
use nix::sys::wait::{waitpid, WaitPidFlag, WaitStatus};
#[cfg(unix)]
use nix::unistd::{fork, getuid, setuid, ForkResult, Pid, Uid, User};
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use crate::isolation::{self, IsolationConfig};
use crate::workdir::WorkDir;

/// Result of job execution.
#[derive(Debug)]
pub struct ExecutionResult {
    /// Whether the job succeeded (exit code 0)
    pub success: bool,
    /// Exit code (if exited normally)
    pub exit_code: Option<i32>,
    /// Signal number (if killed by signal)
    pub signal: Option<i32>,
    /// Duration of execution
    pub duration: Duration,
    /// Error message (if failed)
    pub error: Option<String>,
}

/// Handle for a spawned isolated job process.
///
/// Returned by `spawn_isolated()` to allow the caller to:
/// - Track the process ID for cancellation
/// - Poll for completion non-blocking
/// - Clean up resources after completion
///
/// Use `wait_for_job()` or `poll_job()` to check completion status.
#[cfg(unix)]
pub struct SpawnedJob {
    /// Child process ID
    pub pid: Pid,
    /// Cgroup handle (if resource limits were applied)
    pub cgroup: Option<Cgroup>,
    /// When the job was started
    pub start_time: Instant,
}

/// Validate job script: must be executable and have valid shebang.
///
/// SECURITY: Validates script before execution to prevent:
/// - Non-executable files being passed to shell
/// - Missing shebang causing shell interpretation of arbitrary code
///
/// # Arguments
/// * `path` - Path to the script file
pub fn validate_job_script(path: &Path) -> Result<()> {
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

    // Check execute permission
    #[cfg(unix)]
    {
        let permissions = metadata.permissions();
        let mode = permissions.mode();
        let is_executable = (mode & 0o111) != 0;

        if !is_executable {
            anyhow::bail!(
                "Script is not executable: {} (run: chmod +x {})",
                path.display(),
                path.display()
            );
        }
    }

    // Validate shebang line
    let file = File::open(path)
        .context(format!("Failed to open script: {}", path.display()))?;

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

    // Extract interpreter path from shebang
    let interpreter = first_line[2..].trim();

    if interpreter.is_empty() {
        anyhow::bail!(
            "Invalid shebang in {}: no interpreter specified",
            path.display()
        );
    }

    debug!("Validated script: {} (interpreter: {})", path.display(), interpreter.split_whitespace().next().unwrap_or(""));

    Ok(())
}

/// Validate username and return UID.
///
/// SECURITY: Validates username contains only safe characters
/// and user exists on the system.
#[cfg(unix)]
pub fn validate_and_get_uid(username: &str) -> Result<Uid> {
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

    // Verify user exists and get UID
    User::from_name(username)?
        .ok_or_else(|| anyhow::anyhow!("User '{}' does not exist", username))
        .map(|user| user.uid)
}

#[cfg(not(unix))]
pub fn validate_and_get_uid(username: &str) -> Result<u32> {
    if username.is_empty() {
        anyhow::bail!("Username cannot be empty");
    }
    warn!("User validation not supported on this platform");
    Ok(0)
}

// ============================================================================
// Spawned Execution API (for executor with custom wait loops)
// ============================================================================

/// Spawn an isolated job process without blocking.
///
/// This function:
/// 1. Validates the script
/// 2. Creates cgroup (if limits specified)
/// 3. Forks a child process
/// 4. In child: applies isolation, drops privileges, execs script
/// 5. In parent: returns SpawnedJob handle immediately
///
/// The caller is responsible for:
/// - Tracking the PID for cancellation
/// - Calling `poll_job()` or `wait_for_job()` to check completion
/// - Calling `cleanup_job()` after completion
///
/// # Arguments
/// * `script_path` - Path to the executable script
/// * `work_dir` - Work directory structure
/// * `job_id` - Job identifier (for environment)
/// * `execution_id` - Execution identifier (for environment)
/// * `username` - User to run the job as
/// * `isolation_config` - Isolation settings (Landlock, network, cgroups)
///
/// # Returns
/// SpawnedJob handle for tracking and cleanup.
#[cfg(unix)]
pub fn spawn_isolated(
    script_path: &Path,
    work_dir: &WorkDir,
    job_id: &str,
    execution_id: &str,
    username: &str,
    isolation_config: &IsolationConfig,
) -> Result<SpawnedJob> {
    // Validate script
    validate_job_script(script_path)?;

    // Get target user info
    let uid = validate_and_get_uid(username)?;
    let current_uid = getuid();
    let needs_privilege_change = current_uid != uid;

    // Apply cgroup limits (before fork so parent can track)
    let cgroup = if let Some(ref limits) = isolation_config.limits {
        info!("Applying cgroup resource limits for {}", execution_id);
        Some(isolation::apply_cgroup_limits(execution_id, limits)?)
    } else {
        None
    };

    let start = Instant::now();

    match unsafe { fork() }? {
        ForkResult::Parent { child } => {
            // Parent: return handle immediately
            Ok(SpawnedJob {
                pid: child,
                cgroup,
                start_time: start,
            })
        }
        ForkResult::Child => {
            // Child: Apply isolation and execute (never returns)
            run_child_process(
                script_path,
                work_dir,
                job_id,
                execution_id,
                isolation_config,
                needs_privilege_change,
                uid,
            );
        }
    }
}

/// Child process execution (called after fork, never returns).
///
/// Applies isolation, drops privileges, and execs the script.
/// This is factored out for reuse between spawn_isolated and execute_isolated.
#[cfg(unix)]
fn run_child_process(
    script_path: &Path,
    work_dir: &WorkDir,
    job_id: &str,
    execution_id: &str,
    isolation_config: &IsolationConfig,
    needs_privilege_change: bool,
    uid: Uid,
) -> ! {
    // 1. Apply network isolation (before Landlock)
    if let Err(e) = isolation::isolate_network(isolation_config.network) {
        error!("Failed to apply network isolation: {}", e);
        std::process::exit(1);
    }

    // 2. Apply Landlock filesystem policy
    if let Err(e) = isolation::apply_landlock_policy(
        isolation_config,
        &work_dir.in_dir,
        &work_dir.out_dir,
        &work_dir.tmp_dir,
    ) {
        error!("Failed to apply Landlock policy: {}", e);
        std::process::exit(1);
    }

    // 3. Drop privileges
    if needs_privilege_change {
        if let Err(e) = setuid(uid) {
            error!("Failed to setuid: {}", e);
            std::process::exit(1);
        }
    }

    // 4. Execute script with environment variables
    let env_vars = work_dir.env_vars(job_id, execution_id);

    let mut cmd = Command::new(script_path);
    cmd.current_dir(&work_dir.root);

    for (key, value) in env_vars {
        cmd.env(key, value);
    }

    let status = cmd.status().unwrap_or_else(|e| {
        error!("Failed to execute job script: {}", e);
        std::process::exit(1);
    });

    std::process::exit(if status.success() { 0 } else { 1 });
}

/// Poll a spawned job for completion (non-blocking).
///
/// Returns `Some(ExecutionResult)` if the job has completed, `None` if still running.
///
/// # Arguments
/// * `job` - The spawned job handle
#[cfg(unix)]
pub fn poll_job(job: &SpawnedJob) -> Result<Option<ExecutionResult>> {
    match waitpid(job.pid, Some(WaitPidFlag::WNOHANG))? {
        WaitStatus::StillAlive => Ok(None),
        WaitStatus::Exited(_, code) => Ok(Some(ExecutionResult {
            success: code == 0,
            exit_code: Some(code),
            signal: None,
            duration: job.start_time.elapsed(),
            error: if code != 0 {
                Some(format!("Job exited with code {}", code))
            } else {
                None
            },
        })),
        WaitStatus::Signaled(_, signal, _) => Ok(Some(ExecutionResult {
            success: false,
            exit_code: None,
            signal: Some(signal as i32),
            duration: job.start_time.elapsed(),
            error: Some(format!("Job killed by signal {:?}", signal)),
        })),
        _ => Ok(Some(ExecutionResult {
            success: false,
            exit_code: None,
            signal: None,
            duration: job.start_time.elapsed(),
            error: Some("Job terminated unexpectedly".to_string()),
        })),
    }
}

/// Wait for a spawned job to complete with timeout (blocking).
///
/// Polls the job until completion or timeout.
///
/// # Arguments
/// * `job` - The spawned job handle
/// * `timeout` - Maximum time to wait
#[cfg(unix)]
pub fn wait_for_job(job: &SpawnedJob, timeout: Duration) -> Result<ExecutionResult> {
    loop {
        // Check if job has completed
        if let Some(result) = poll_job(job)? {
            return Ok(result);
        }

        // Check timeout
        if job.start_time.elapsed() > timeout {
            // Kill job on timeout
            let _ = kill(job.pid, Signal::SIGKILL);
            return Ok(ExecutionResult {
                success: false,
                exit_code: None,
                signal: Some(9),
                duration: job.start_time.elapsed(),
                error: Some(format!("Job timed out after {:?}", timeout)),
            });
        }

        // Sleep briefly before next poll
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Kill a spawned job process.
///
/// Sends SIGTERM (or SIGKILL if force=true) to the job process.
///
/// # Arguments
/// * `job` - The spawned job handle
/// * `force` - If true, send SIGKILL instead of SIGTERM
#[cfg(unix)]
pub fn kill_job(job: &SpawnedJob, force: bool) -> Result<()> {
    let signal = if force { Signal::SIGKILL } else { Signal::SIGTERM };
    kill(job.pid, signal).context(format!("Failed to kill job pid={}", job.pid))?;
    Ok(())
}

/// Clean up resources for a spawned job (cgroup, etc).
///
/// Should be called after the job has completed.
///
/// # Arguments
/// * `job` - The spawned job handle (consumed)
#[cfg(unix)]
pub fn cleanup_job(job: SpawnedJob) {
    if let Some(cg) = job.cgroup {
        if let Err(e) = isolation::cleanup_cgroup(cg) {
            warn!("Failed to cleanup cgroup: {}", e);
        }
    }
}

// ============================================================================
// Blocking Execution API (for simple tools like ljc test)
// ============================================================================

/// Execute a job script with isolation (blocking).
///
/// Convenience wrapper around `spawn_isolated()` + `wait_for_job()` + `cleanup_job()`.
/// For simple tools like `ljc test` that just need to run a job and get the result.
///
/// For the executor which needs to track PIDs for cancellation and poll
/// non-blocking while processing MQTT events, use the spawned execution API:
/// `spawn_isolated()`, `poll_job()`, `wait_for_job()`, `cleanup_job()`.
///
/// # Arguments
/// * `script_path` - Path to the executable script
/// * `work_dir` - Work directory structure
/// * `job_id` - Job identifier (for environment)
/// * `execution_id` - Execution identifier (for environment)
/// * `username` - User to run the job as
/// * `isolation_config` - Isolation settings (Landlock, network, cgroups)
/// * `timeout` - Maximum execution time
///
/// # Returns
/// ExecutionResult with exit status and timing.
#[cfg(unix)]
pub fn execute_isolated(
    script_path: &Path,
    work_dir: &WorkDir,
    job_id: &str,
    execution_id: &str,
    username: &str,
    isolation_config: &IsolationConfig,
    timeout: Duration,
) -> Result<ExecutionResult> {
    // Spawn the job (forks, applies isolation in child, returns handle to parent)
    let job = spawn_isolated(
        script_path,
        work_dir,
        job_id,
        execution_id,
        username,
        isolation_config,
    )?;

    // Wait for completion with timeout
    let result = wait_for_job(&job, timeout)?;

    // Cleanup resources (cgroup, etc)
    cleanup_job(job);

    Ok(result)
}

/// Execute a job script without isolation (for local testing).
///
/// Simpler execution path that doesn't fork or apply isolation.
/// Useful for `ljc test --no-isolation`.
pub fn execute_simple(
    script_path: &Path,
    work_dir: &WorkDir,
    job_id: &str,
    execution_id: &str,
    timeout: Duration,
) -> Result<ExecutionResult> {
    // Validate script
    validate_job_script(script_path)?;

    let start = Instant::now();

    let env_vars = work_dir.env_vars(job_id, execution_id);

    let mut cmd = Command::new(script_path);
    cmd.current_dir(&work_dir.root);

    for (key, value) in env_vars {
        cmd.env(key, value);
    }

    // Execute with timeout using spawn + wait
    let mut child = cmd.spawn()
        .context(format!("Failed to spawn script: {}", script_path.display()))?;

    loop {
        match child.try_wait()? {
            Some(status) => {
                let code = status.code().unwrap_or(-1);
                return Ok(ExecutionResult {
                    success: status.success(),
                    exit_code: Some(code),
                    signal: None,
                    duration: start.elapsed(),
                    error: if !status.success() {
                        Some(format!("Job exited with code {}", code))
                    } else {
                        None
                    },
                });
            }
            None => {
                if start.elapsed() > timeout {
                    let _ = child.kill();
                    return Ok(ExecutionResult {
                        success: false,
                        exit_code: None,
                        signal: Some(9),
                        duration: start.elapsed(),
                        error: Some(format!("Job timed out after {:?}", timeout)),
                    });
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_validate_script_with_shebang() {
        let temp = TempDir::new().unwrap();
        let script = temp.path().join("test.sh");

        fs::write(&script, "#!/bin/sh\necho hello").unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        }

        assert!(validate_job_script(&script).is_ok());
    }

    #[test]
    fn test_validate_script_missing_shebang() {
        let temp = TempDir::new().unwrap();
        let script = temp.path().join("test.sh");

        fs::write(&script, "echo hello").unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        }

        let result = validate_job_script(&script);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("shebang"));
    }

    #[cfg(unix)]
    #[test]
    fn test_validate_script_not_executable() {
        let temp = TempDir::new().unwrap();
        let script = temp.path().join("test.sh");

        fs::write(&script, "#!/bin/sh\necho hello").unwrap();
        // Don't set executable permission

        let result = validate_job_script(&script);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not executable"));
    }

    #[test]
    fn test_execute_simple() {
        let temp = TempDir::new().unwrap();

        // Create work directory
        let work_dir = WorkDir::new(temp.path(), "test-exec").unwrap();

        // Create script
        let script = work_dir.root.join("test.sh");
        fs::write(&script, "#!/bin/sh\necho hello > $LJC_OUT/result.txt").unwrap();

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&script, fs::Permissions::from_mode(0o755)).unwrap();
        }

        let result = execute_simple(
            &script,
            &work_dir,
            "test.job",
            "exec-123",
            Duration::from_secs(10),
        ).unwrap();

        assert!(result.success, "Expected success but got: {:?}", result.error);
        assert_eq!(result.exit_code, Some(0));

        // Check output was created
        let output_file = work_dir.out_dir.join("result.txt");
        assert!(output_file.exists(), "Output file should exist");
    }
}
