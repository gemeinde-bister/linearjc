// SPDX-License-Identifier: MIT
// Process isolation module for LinearJC executor
// Provides filesystem (Landlock), resource (cgroups), and network (namespaces) isolation

use anyhow::{Context, Result, bail};
use log::{info, warn};
use std::path::{Path, PathBuf};
use landlock::{
    Access, AccessFs, PathBeneath, PathFd, Ruleset, RulesetAttr,
    RulesetCreatedAttr, ABI,
};
use nix::sched::{unshare, CloneFlags};
use cgroups_rs::{
    cpu::CpuController, hierarchies, memory::MemController, pid::PidController, Cgroup,
    CgroupPid, MaxValue,
};

/// Filesystem isolation mode
#[derive(Debug, Clone, PartialEq)]
pub enum FilesystemIsolation {
    /// Strict mode: Only /in (RO), /out (RW), /tmp (RW), system libs
    Strict,
    /// Relaxed mode: Strict + extra_read_paths from job config
    Relaxed,
    /// No isolation: Only Unix permissions (for jobs needing flexibility)
    None,
}

impl FilesystemIsolation {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "strict" => Ok(FilesystemIsolation::Strict),
            "relaxed" => Ok(FilesystemIsolation::Relaxed),
            "none" => Ok(FilesystemIsolation::None),
            _ => bail!("Invalid filesystem isolation mode: {}", s),
        }
    }
}

/// Resource limits for cgroups
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// CPU limit as percentage of one core (0-100)
    pub cpu_percent: Option<u32>,
    /// Memory limit in MB
    pub memory_mb: Option<u64>,
    /// Maximum number of processes (prevents fork bombs)
    pub processes: Option<u64>,
    /// I/O bandwidth limit in MB/s (optional)
    pub io_mbps: Option<u64>,
}

/// Complete isolation configuration
#[derive(Debug, Clone)]
pub struct IsolationConfig {
    /// Filesystem isolation mode
    pub filesystem: FilesystemIsolation,
    /// Network access enabled
    pub network: bool,
    /// Resource limits (optional)
    pub limits: Option<ResourceLimits>,
    /// Extra read-only paths (for relaxed mode)
    pub extra_read_paths: Vec<PathBuf>,
}

impl Default for IsolationConfig {
    fn default() -> Self {
        IsolationConfig {
            filesystem: FilesystemIsolation::None, // v0.4.0-alpha: opt-in
            network: true,
            limits: None,
            extra_read_paths: Vec::new(),
        }
    }
}

/// Apply Landlock filesystem isolation policy
///
/// This creates a Landlock ruleset that restricts filesystem access to:
/// - /in (read-only) - Job inputs
/// - /out (read-write) - Job outputs
/// - /tmp (read-write) - Temporary files
/// - System libraries (read-only + execute) - /lib*, /usr/lib*, /bin, /usr/bin
/// - Extra paths (read-only + execute) - For relaxed mode
///
/// Once applied, the process cannot access any other filesystem locations.
/// This is enforced at the kernel level and cannot be bypassed.
///
/// Requires Linux 5.13+ with Landlock LSM enabled.
pub fn apply_landlock_policy(
    config: &IsolationConfig,
    in_dir: &Path,
    out_dir: &Path,
    tmp_dir: &Path,
) -> Result<()> {
    // Skip if isolation disabled
    if config.filesystem == FilesystemIsolation::None {
        info!("Landlock disabled for this job (isolation: none)");
        return Ok(());
    }

    // Check if Landlock is supported
    let abi = ABI::V4;

    // Create ruleset with ABI V4 (Linux 6.7+) or fallback to V3/V2/V1
    let mut ruleset = Ruleset::default()
        .handle_access(AccessFs::from_all(abi))
        .context("Failed to create Landlock ruleset - Landlock not supported on this kernel")?
        .create()
        .context("Failed to create Landlock ruleset")?;

    // 1. Allow read-only access to inputs
    if in_dir.exists() {
        ruleset = ruleset
            .add_rule(PathBeneath::new(
                PathFd::new(in_dir)?,
                AccessFs::ReadFile | AccessFs::ReadDir,
            ))
            .context("Failed to add Landlock rule for /in")?;
    }

    // 2. Allow read-write access to outputs
    if out_dir.exists() {
        ruleset = ruleset
            .add_rule(PathBeneath::new(
                PathFd::new(out_dir)?,
                AccessFs::from_all(abi), // Full write access
            ))
            .context("Failed to add Landlock rule for /out")?;
    }

    // 3. Allow read-write access to tmp
    if tmp_dir.exists() {
        ruleset = ruleset
            .add_rule(PathBeneath::new(
                PathFd::new(tmp_dir)?,
                AccessFs::from_all(abi),
            ))
            .context("Failed to add Landlock rule for /tmp")?;
    }

    // 4. System libraries and binaries (read-only + execute)
    let system_paths = vec![
        "/lib",
        "/lib64",
        "/usr/lib",
        "/usr/lib64",
        "/bin",
        "/usr/bin",
        "/sbin",
        "/usr/sbin",
    ];

    for path_str in system_paths {
        let path = Path::new(path_str);
        if path.exists() {
            ruleset = ruleset
                .add_rule(PathBeneath::new(
                    PathFd::new(path)?,
                    AccessFs::ReadFile | AccessFs::ReadDir | AccessFs::Execute,
                ))
                .with_context(|| format!("Failed to add Landlock rule for {}", path_str))?;
        }
    }

    // 5. Extra read paths (for relaxed mode only)
    if config.filesystem == FilesystemIsolation::Relaxed {
        for extra_path in &config.extra_read_paths {
            if extra_path.exists() {
                ruleset = ruleset
                    .add_rule(PathBeneath::new(
                        PathFd::new(extra_path)?,
                        AccessFs::ReadFile | AccessFs::ReadDir | AccessFs::Execute,
                    ))
                    .with_context(|| {
                        format!("Failed to add Landlock rule for extra path: {:?}", extra_path)
                    })?;
            } else {
                warn!("Extra read path does not exist: {:?}", extra_path);
            }
        }
    }

    // 6. Apply policy (irreversible!)
    ruleset
        .restrict_self()
        .context("Failed to apply Landlock restrictions")?;

    info!(
        "Landlock policy applied: mode={:?}, extra_paths={}",
        config.filesystem,
        config.extra_read_paths.len()
    );

    Ok(())
}

/// Apply network namespace isolation
///
/// If network is disabled, creates a new network namespace with no network access.
/// The job will only have access to the loopback interface.
///
/// If network is enabled, no isolation is applied (job uses host network).
pub fn isolate_network(enable_network: bool) -> Result<()> {
    if !enable_network {
        // Create new network namespace (no network except loopback)
        unshare(CloneFlags::CLONE_NEWNET)
            .context("Failed to create network namespace")?;

        info!("Network isolated (no network access)");
    } else {
        info!("Network enabled (host network)");
    }

    Ok(())
}

/// Apply cgroups v2 resource limits
///
/// Creates a cgroup under linearjc/{job_id} and applies CPU, memory, and process limits.
/// The cgroup is automatically added to the current process.
///
/// # Arguments
/// * `job_id` - Unique job identifier (used as cgroup name)
/// * `limits` - Resource limits to apply
///
/// # Returns
/// The created Cgroup instance (must be kept alive and cleaned up after job completes)
///
/// # Errors
/// - If cgroups v2 is not available
/// - If controllers are not enabled
/// - If setting limits fails
pub fn apply_cgroup_limits(
    job_id: &str,
    limits: &ResourceLimits,
) -> Result<Cgroup> {
    // Get cgroup hierarchies (v2 auto-detected)
    let hier = hierarchies::auto();

    // Create cgroup under linearjc/{job_id}
    let cgroup_name = format!("linearjc/{}", job_id);
    let cgroup = Cgroup::new(hier, &cgroup_name)
        .with_context(|| format!("Failed to create cgroup: {}", cgroup_name))?;

    // Apply CPU limit (if specified)
    if let Some(cpu_percent) = limits.cpu_percent {
        if let Some(cpu_controller) = cgroup.controller_of::<CpuController>() {
            // CFS quota/period mechanism:
            // quota = (cpu_percent / 100) * period
            // period = 100000 microseconds (100ms standard)
            let period: i64 = 100_000; // 100ms
            let quota = (cpu_percent as i64) * 1000; // microseconds

            cpu_controller.set_cfs_quota(quota)
                .with_context(|| format!("Failed to set CPU quota: {}%", cpu_percent))?;
            cpu_controller.set_cfs_period(period as u64)
                .context("Failed to set CPU period")?;

            info!("CPU limit applied: {}%", cpu_percent);
        } else {
            warn!("CPU controller not available, CPU limit skipped");
        }
    }

    // Apply memory limit (if specified)
    if let Some(memory_mb) = limits.memory_mb {
        if let Some(mem_controller) = cgroup.controller_of::<MemController>() {
            let bytes = memory_mb * 1024 * 1024;

            mem_controller.set_limit(bytes as i64)
                .with_context(|| format!("Failed to set memory limit: {} MB", memory_mb))?;

            info!("Memory limit applied: {} MB", memory_mb);
        } else {
            warn!("Memory controller not available, memory limit skipped");
        }
    }

    // Apply process limit (if specified) - prevents fork bombs
    if let Some(max_pids) = limits.processes {
        if let Some(pids_controller) = cgroup.controller_of::<PidController>() {
            pids_controller.set_pid_max(MaxValue::Value(max_pids as i64))
                .with_context(|| format!("Failed to set process limit: {}", max_pids))?;

            info!("Process limit applied: {} processes", max_pids);
        } else {
            warn!("PID controller not available, process limit skipped");
        }
    }

    // Apply I/O limit (if specified)
    if let Some(io_mbps) = limits.io_mbps {
        // I/O limits require device-specific configuration
        // This is more complex and device-dependent, so we warn for now
        warn!("I/O limits not yet fully implemented: {} MB/s requested", io_mbps);
    }

    // Add current process to the cgroup
    let pid = std::process::id();
    cgroup.add_task(CgroupPid::from(pid as u64))
        .with_context(|| format!("Failed to add process {} to cgroup", pid))?;

    info!("Process {} added to cgroup: {}", pid, cgroup_name);

    Ok(cgroup)
}

/// Cleanup cgroup after job completion
///
/// Removes the cgroup from the system. This should be called after the job process exits.
///
/// # Arguments
/// * `cgroup` - The Cgroup instance returned by apply_cgroup_limits
///
/// # Errors
/// - If cgroup removal fails (non-fatal, logged as warning)
pub fn cleanup_cgroup(cgroup: Cgroup) -> Result<()> {
    // Delete the cgroup
    match cgroup.delete() {
        Ok(_) => {
            info!("Cgroup cleaned up successfully");
            Ok(())
        }
        Err(e) => {
            // Non-fatal error - kernel will clean up eventually
            warn!("Failed to cleanup cgroup: {} (non-fatal)", e);
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_filesystem_isolation_from_str() {
        assert_eq!(
            FilesystemIsolation::from_str("strict").unwrap(),
            FilesystemIsolation::Strict
        );
        assert_eq!(
            FilesystemIsolation::from_str("RELAXED").unwrap(),
            FilesystemIsolation::Relaxed
        );
        assert_eq!(
            FilesystemIsolation::from_str("none").unwrap(),
            FilesystemIsolation::None
        );
        assert!(FilesystemIsolation::from_str("invalid").is_err());
    }

    #[test]
    fn test_isolation_config_default() {
        let config = IsolationConfig::default();
        assert_eq!(config.filesystem, FilesystemIsolation::None);
        assert_eq!(config.network, true);
        assert!(config.limits.is_none());
        assert_eq!(config.extra_read_paths.len(), 0);
    }

    #[test]
    fn test_landlock_policy_none_mode() {
        // Test that None mode does not apply Landlock
        let temp = TempDir::new().unwrap();
        let in_dir = temp.path().join("in");
        let out_dir = temp.path().join("out");
        let tmp_dir = temp.path().join("tmp");

        fs::create_dir_all(&in_dir).unwrap();
        fs::create_dir_all(&out_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let config = IsolationConfig {
            filesystem: FilesystemIsolation::None,
            network: true,
            limits: None,
            extra_read_paths: Vec::new(),
        };

        // Should succeed without applying Landlock
        let result = apply_landlock_policy(&config, &in_dir, &out_dir, &tmp_dir);
        assert!(result.is_ok());
    }

    #[test]
    fn test_landlock_policy_strict_mode() {
        // Test that Strict mode builds policy with correct paths
        let temp = TempDir::new().unwrap();
        let in_dir = temp.path().join("in");
        let out_dir = temp.path().join("out");
        let tmp_dir = temp.path().join("tmp");

        fs::create_dir_all(&in_dir).unwrap();
        fs::create_dir_all(&out_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let config = IsolationConfig {
            filesystem: FilesystemIsolation::Strict,
            network: true,
            limits: None,
            extra_read_paths: Vec::new(),
        };

        // Should build policy without error (actual enforcement requires kernel support)
        // We're testing the logic, not the syscalls
        let result = apply_landlock_policy(&config, &in_dir, &out_dir, &tmp_dir);

        // On systems without Landlock, this will log a warning and succeed
        // On systems with Landlock, this will succeed with policy applied
        assert!(result.is_ok());
    }

    #[test]
    fn test_landlock_policy_relaxed_mode_with_extra_paths() {
        // Test that Relaxed mode includes extra_read_paths
        let temp = TempDir::new().unwrap();
        let in_dir = temp.path().join("in");
        let out_dir = temp.path().join("out");
        let tmp_dir = temp.path().join("tmp");

        fs::create_dir_all(&in_dir).unwrap();
        fs::create_dir_all(&out_dir).unwrap();
        fs::create_dir_all(&tmp_dir).unwrap();

        let config = IsolationConfig {
            filesystem: FilesystemIsolation::Relaxed,
            network: false,
            limits: None,
            extra_read_paths: vec![
                PathBuf::from("/usr/local/bin/mytool"),
                PathBuf::from("/opt/data"),
            ],
        };

        // Should succeed and include extra paths in policy
        let result = apply_landlock_policy(&config, &in_dir, &out_dir, &tmp_dir);
        assert!(result.is_ok());
    }

    #[test]
    fn test_resource_limits_parsing() {
        // Test ResourceLimits struct construction
        let limits = ResourceLimits {
            cpu_percent: Some(50),
            memory_mb: Some(512),
            processes: Some(10),
            io_mbps: Some(100),
        };

        assert_eq!(limits.cpu_percent, Some(50));
        assert_eq!(limits.memory_mb, Some(512));
        assert_eq!(limits.processes, Some(10));
        assert_eq!(limits.io_mbps, Some(100));
    }

    #[test]
    fn test_resource_limits_none() {
        // Test that None limits work
        let limits = ResourceLimits {
            cpu_percent: None,
            memory_mb: None,
            processes: None,
            io_mbps: None,
        };

        assert!(limits.cpu_percent.is_none());
        assert!(limits.memory_mb.is_none());
        assert!(limits.processes.is_none());
        assert!(limits.io_mbps.is_none());
    }

    #[test]
    fn test_isolation_config_with_all_options() {
        // Test full IsolationConfig with all options set
        let config = IsolationConfig {
            filesystem: FilesystemIsolation::Strict,
            network: false,
            limits: Some(ResourceLimits {
                cpu_percent: Some(25),
                memory_mb: Some(256),
                processes: Some(5),
                io_mbps: Some(50),
            }),
            extra_read_paths: vec![PathBuf::from("/custom/path")],
        };

        assert_eq!(config.filesystem, FilesystemIsolation::Strict);
        assert_eq!(config.network, false);
        assert!(config.limits.is_some());

        let limits = config.limits.unwrap();
        assert_eq!(limits.cpu_percent, Some(25));
        assert_eq!(limits.memory_mb, Some(256));
        assert_eq!(limits.processes, Some(5));

        assert_eq!(config.extra_read_paths.len(), 1);
        assert_eq!(config.extra_read_paths[0], PathBuf::from("/custom/path"));
    }


    #[test]
    fn test_network_isolation_flag() {
        // Test network isolation configuration
        let config_isolated = IsolationConfig {
            filesystem: FilesystemIsolation::None,
            network: false,  // Isolated
            limits: None,
            extra_read_paths: Vec::new(),
        };

        let config_host = IsolationConfig {
            filesystem: FilesystemIsolation::None,
            network: true,  // Host network
            limits: None,
            extra_read_paths: Vec::new(),
        };

        assert_eq!(config_isolated.network, false);
        assert_eq!(config_host.network, true);
    }

    // Note: Testing actual Landlock enforcement, cgroups limits, and network
    // namespace isolation requires root privileges and kernel support.
    // These tests will be added in integration tests that run in the proper
    // environment (see tests/isolation_integration.rs).
}
