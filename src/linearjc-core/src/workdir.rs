//! Work directory management for LinearJC.
//!
//! Provides standardized directory layout for job execution:
//! - `in/` - Input data (read-only during execution)
//! - `out/` - Output data (job writes results here)
//! - `tmp/` - Temporary files (job scratch space)
//! - `.work/` - Internal use (archives, metadata)
//!
//! This module is shared between executor and ljc tool.

use anyhow::{Context, Result};
use log::{debug, info, warn};
use std::fs;
use std::path::{Path, PathBuf};

#[cfg(unix)]
use nix::unistd::{chown, Gid, Uid};
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};

/// Represents a work directory with standardized layout.
///
/// Phase 7 SPEC.md v0.5.0 compliant directory structure:
/// ```text
/// {work_dir}/{execution_id}/
/// ├── in/           # Inputs (read-only)
/// │   ├── {input1}/
/// │   └── .meta/    # Registry metadata
/// ├── out/          # Outputs (writable)
/// │   └── {output1}/
/// ├── tmp/          # Temporary files
/// └── .work/        # Internal (archives)
/// ```
#[derive(Debug, Clone)]
pub struct WorkDir {
    /// Root directory for this job execution
    pub root: PathBuf,
    /// Input directory (in/)
    pub in_dir: PathBuf,
    /// Output directory (out/)
    pub out_dir: PathBuf,
    /// Temporary directory (tmp/)
    pub tmp_dir: PathBuf,
    /// Internal work directory (.work/)
    pub work_dir: PathBuf,
}

impl WorkDir {
    /// Create a new work directory structure.
    ///
    /// # Arguments
    /// * `base_dir` - Base work directory (e.g., /var/lib/linearjc/work)
    /// * `execution_id` - Unique execution identifier
    ///
    /// # Returns
    /// WorkDir with all directories created.
    pub fn new(base_dir: &Path, execution_id: &str) -> Result<Self> {
        let root = base_dir.join(execution_id);
        let in_dir = root.join("in");
        let out_dir = root.join("out");
        let tmp_dir = root.join("tmp");
        let work_dir = root.join(".work");

        // Create all directories
        fs::create_dir_all(&in_dir)
            .context(format!("Failed to create in directory: {}", in_dir.display()))?;
        fs::create_dir_all(&out_dir)
            .context(format!("Failed to create out directory: {}", out_dir.display()))?;
        fs::create_dir_all(&tmp_dir)
            .context(format!("Failed to create tmp directory: {}", tmp_dir.display()))?;
        fs::create_dir_all(&work_dir)
            .context(format!("Failed to create .work directory: {}", work_dir.display()))?;

        info!(
            "Created work directory: {} (in, out, tmp, .work)",
            root.display()
        );

        Ok(Self {
            root,
            in_dir,
            out_dir,
            tmp_dir,
            work_dir,
        })
    }

    /// Create metadata directory inside in/ for registry introspection.
    ///
    /// Creates `in/.meta/` directory with registry.json containing
    /// job metadata for script introspection.
    pub fn create_metadata(&self, inputs: &[String], outputs: &[String], job_id: &str, execution_id: &str) -> Result<()> {
        let meta_dir = self.in_dir.join(".meta");
        fs::create_dir_all(&meta_dir)
            .context(format!("Failed to create .meta directory: {}", meta_dir.display()))?;

        let metadata = serde_json::json!({
            "inputs": inputs,
            "outputs": outputs,
            "job_id": job_id,
            "execution_id": execution_id,
        });

        let registry_file = meta_dir.join("registry.json");
        fs::write(&registry_file, serde_json::to_string_pretty(&metadata)?)
            .context(format!("Failed to write registry.json: {}", registry_file.display()))?;

        debug!("Created registry metadata: {}", registry_file.display());
        Ok(())
    }

    /// Change ownership of all directories to specified user.
    ///
    /// SECURITY: Uses nix library chown (not subprocess) to prevent command injection.
    ///
    /// # Arguments
    /// * `uid` - User ID to own the directories
    /// * `gid` - Group ID to own the directories
    #[cfg(unix)]
    pub fn chown_all(&self, uid: Uid, gid: Gid) -> Result<()> {
        chown(&self.root, Some(uid), Some(gid))
            .context(format!("Failed to chown root: {}", self.root.display()))?;

        chown(&self.in_dir, Some(uid), Some(gid))
            .context(format!("Failed to chown in_dir: {}", self.in_dir.display()))?;

        chown(&self.out_dir, Some(uid), Some(gid))
            .context(format!("Failed to chown out_dir: {}", self.out_dir.display()))?;

        chown(&self.tmp_dir, Some(uid), Some(gid))
            .context(format!("Failed to chown tmp_dir: {}", self.tmp_dir.display()))?;

        // Recursively chown in/ contents (after inputs are extracted)
        chown_recursive(&self.in_dir, uid, gid)
            .context("Failed to chown in/ contents")?;

        debug!("Changed ownership of work directory to uid={} gid={}", uid, gid);
        Ok(())
    }

    #[cfg(not(unix))]
    pub fn chown_all(&self, _uid: u32, _gid: u32) -> Result<()> {
        warn!("chown not supported on this platform");
        Ok(())
    }

    /// Clean up the work directory after job completion.
    pub fn cleanup(&self) -> Result<()> {
        if self.root.exists() {
            fs::remove_dir_all(&self.root)
                .context(format!("Failed to cleanup work directory: {}", self.root.display()))?;
            info!("Cleaned up work directory: {}", self.root.display());
        }
        Ok(())
    }

    /// Get environment variables for the job script.
    ///
    /// Returns a vector of (key, value) pairs for:
    /// - Short form: LJC_IN, LJC_OUT, LJC_TMP, LJC_JOB_ID, LJC_EXECUTION_ID
    /// - Long form: LINEARJC_IN_DIR, LINEARJC_OUT_DIR, LINEARJC_TMP_DIR
    /// - Deprecated: LINEARJC_INPUT_DIR, LINEARJC_OUTPUT_DIR
    pub fn env_vars(&self, job_id: &str, execution_id: &str) -> Vec<(String, String)> {
        vec![
            // Short form (SPEC.md v0.5.0)
            ("LJC_IN".to_string(), self.in_dir.to_string_lossy().to_string()),
            ("LJC_OUT".to_string(), self.out_dir.to_string_lossy().to_string()),
            ("LJC_TMP".to_string(), self.tmp_dir.to_string_lossy().to_string()),
            ("LJC_JOB_ID".to_string(), job_id.to_string()),
            ("LJC_EXECUTION_ID".to_string(), execution_id.to_string()),
            // Long form
            ("LINEARJC_IN_DIR".to_string(), self.in_dir.to_string_lossy().to_string()),
            ("LINEARJC_OUT_DIR".to_string(), self.out_dir.to_string_lossy().to_string()),
            ("LINEARJC_TMP_DIR".to_string(), self.tmp_dir.to_string_lossy().to_string()),
            ("LINEARJC_JOB_ID".to_string(), job_id.to_string()),
            ("LINEARJC_EXECUTION_ID".to_string(), execution_id.to_string()),
            // Deprecated (backward compat)
            ("LINEARJC_INPUT_DIR".to_string(), self.in_dir.to_string_lossy().to_string()),
            ("LINEARJC_OUTPUT_DIR".to_string(), self.out_dir.to_string_lossy().to_string()),
        ]
    }
}

/// Setup secure base work directory with proper permissions.
///
/// SECURITY: Ensures directory is owned by current user with mode 0711
/// (owner full access, others can traverse but not list).
///
/// # Arguments
/// * `work_dir` - Path to base work directory
#[cfg(unix)]
pub fn setup_secure_work_dir(work_dir: &Path) -> Result<()> {
    use nix::unistd::getuid;

    // Warn if using world-readable locations
    if work_dir.starts_with("/tmp") || work_dir.starts_with("/var/tmp") {
        warn!(
            "Work directory is in shared temporary space: {}",
            work_dir.display()
        );
        warn!("For production use, configure WORK_DIR to a dedicated directory");
    }

    // Create directory if it doesn't exist
    fs::create_dir_all(work_dir)
        .context(format!("Failed to create work directory: {}", work_dir.display()))?;

    // Set secure permissions (0711 - owner full, others can traverse)
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

    // Verify ownership (must be owned by current user)
    let metadata = fs::metadata(work_dir)?;
    let current_uid = getuid();
    if metadata.uid() != current_uid.as_raw() {
        anyhow::bail!(
            "Work directory {} is not owned by current user (uid: {}, owner: {})",
            work_dir.display(),
            current_uid,
            metadata.uid()
        );
    }

    info!(
        "Work directory validated: {} (uid: {}, mode: {:o})",
        work_dir.display(),
        metadata.uid(),
        metadata.permissions().mode() & 0o777
    );

    Ok(())
}

#[cfg(not(unix))]
pub fn setup_secure_work_dir(work_dir: &Path) -> Result<()> {
    fs::create_dir_all(work_dir)
        .context(format!("Failed to create work directory: {}", work_dir.display()))?;
    warn!("Running on non-Unix platform - directory permissions not enforced");
    Ok(())
}

/// Recursively change ownership of directory and all contents.
///
/// SECURITY: Uses nix library chown (not subprocess) to prevent command injection.
#[cfg(unix)]
pub fn chown_recursive(path: &Path, uid: Uid, gid: Gid) -> Result<()> {
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
            chown_recursive(&entry_path, uid, gid)?;
        }
    }

    Ok(())
}

#[cfg(not(unix))]
pub fn chown_recursive(_path: &Path, _uid: u32, _gid: u32) -> Result<()> {
    warn!("chown_recursive not supported on this platform");
    Ok(())
}

/// Validate path component to prevent directory traversal and injection attacks.
///
/// SECURITY: Only allows alphanumeric, dots, dashes, underscores.
/// Rejects "..", "/", shell metacharacters.
pub fn validate_path_component(component: &str) -> Result<()> {
    if component.is_empty() {
        anyhow::bail!("Path component cannot be empty");
    }

    // Reject components that are too long (DoS prevention)
    if component.len() > 255 {
        anyhow::bail!("Path component too long: {} bytes", component.len());
    }

    // Only allow safe characters
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_workdir_creation() {
        let temp = TempDir::new().unwrap();
        let workdir = WorkDir::new(temp.path(), "test-exec-123").unwrap();

        assert!(workdir.root.exists());
        assert!(workdir.in_dir.exists());
        assert!(workdir.out_dir.exists());
        assert!(workdir.tmp_dir.exists());
        assert!(workdir.work_dir.exists());

        assert!(workdir.in_dir.ends_with("in"));
        assert!(workdir.out_dir.ends_with("out"));
        assert!(workdir.tmp_dir.ends_with("tmp"));
        assert!(workdir.work_dir.ends_with(".work"));
    }

    #[test]
    fn test_metadata_creation() {
        let temp = TempDir::new().unwrap();
        let workdir = WorkDir::new(temp.path(), "test-exec-456").unwrap();

        workdir.create_metadata(
            &["input1".to_string(), "input2".to_string()],
            &["output1".to_string()],
            "test.job",
            "test-exec-456"
        ).unwrap();

        let meta_file = workdir.in_dir.join(".meta").join("registry.json");
        assert!(meta_file.exists());

        let contents = fs::read_to_string(&meta_file).unwrap();
        assert!(contents.contains("input1"));
        assert!(contents.contains("output1"));
        assert!(contents.contains("test.job"));
    }

    #[test]
    fn test_env_vars() {
        let temp = TempDir::new().unwrap();
        let workdir = WorkDir::new(temp.path(), "test-exec").unwrap();

        let vars = workdir.env_vars("my.job", "exec-123");

        // Check short form
        assert!(vars.iter().any(|(k, _)| k == "LJC_IN"));
        assert!(vars.iter().any(|(k, _)| k == "LJC_OUT"));
        assert!(vars.iter().any(|(k, _)| k == "LJC_TMP"));

        // Check long form
        assert!(vars.iter().any(|(k, _)| k == "LINEARJC_IN_DIR"));
        assert!(vars.iter().any(|(k, _)| k == "LINEARJC_JOB_ID"));
    }

    #[test]
    fn test_cleanup() {
        let temp = TempDir::new().unwrap();
        let workdir = WorkDir::new(temp.path(), "test-cleanup").unwrap();

        assert!(workdir.root.exists());
        workdir.cleanup().unwrap();
        assert!(!workdir.root.exists());
    }

    #[test]
    fn test_validate_path_component_valid() {
        assert!(validate_path_component("hello").is_ok());
        assert!(validate_path_component("hello.world").is_ok());
        assert!(validate_path_component("hello-world").is_ok());
        assert!(validate_path_component("hello_world").is_ok());
        assert!(validate_path_component("hello123").is_ok());
    }

    #[test]
    fn test_validate_path_component_invalid() {
        assert!(validate_path_component("").is_err());
        assert!(validate_path_component("..").is_err());
        assert!(validate_path_component("../etc").is_err());
        assert!(validate_path_component("/etc").is_err());
        assert!(validate_path_component("hello world").is_err());
        assert!(validate_path_component("hello;rm -rf /").is_err());
    }
}
