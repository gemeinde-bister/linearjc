//! Package handling for LinearJC.
//!
//! Provides functions for reading and extracting .ljc packages, used by both:
//! - Executor (production job runner)
//! - ljc tool (development CLI)
//!
//! A .ljc package is a tar.gz archive containing:
//! - manifest.yaml - Package metadata (job_id, version, checksum)
//! - job.yaml - Job configuration
//! - script.sh - Entry script (or custom entry from job.yaml)
//! - bin/ - Optional binaries
//! - lib/ - Optional libraries

use anyhow::{Context, Result};
use log::info;
use std::cmp::Ordering;
use std::fs::File;
use std::path::{Path, PathBuf};

use flate2::read::GzDecoder;
use tar::Archive;

use crate::archive::read_file_from_archive;
use crate::job::{parse_job_yaml, JobFile, RunSpec};

// ============================================================================
// Package metadata reading
// ============================================================================
//
// Note: Packages do NOT contain manifest.yaml (legacy format).
// All metadata comes from job.yaml inside the package.

/// Metadata extracted from a .ljc package
#[derive(Debug, Clone)]
pub struct PackageMetadata {
    /// Job identifier
    pub job_id: String,
    /// Job version
    pub version: String,
    /// Runtime configuration (if present in job.yaml)
    pub run: Option<RunSpec>,
    /// Full job specification
    pub job: JobFile,
}

/// Read metadata from a .ljc package without extracting.
///
/// Reads job.yaml from inside the archive to get job ID, version, and run config.
///
/// # Arguments
/// * `package_path` - Path to the .ljc package file
pub fn read_package_metadata(package_path: &Path) -> Result<PackageMetadata> {
    // Read job.yaml from archive
    let contents = read_file_from_archive(package_path, "job.yaml")
        .context(format!("Failed to read job.yaml from package: {}", package_path.display()))?;

    let job = parse_job_yaml(&contents)
        .context(format!("Failed to parse job.yaml in package: {}", package_path.display()))?;

    Ok(PackageMetadata {
        job_id: job.job.id.clone(),
        version: job.job.version.clone(),
        run: job.job.run.clone(),
        job,
    })
}

/// Get just the version from a .ljc package.
///
/// Convenience function when only version is needed.
pub fn get_package_version(package_path: &Path) -> Result<String> {
    let metadata = read_package_metadata(package_path)?;
    Ok(metadata.version)
}

// ============================================================================
// Package extraction
// ============================================================================

/// Extract a .ljc package to a work directory.
///
/// Extracts all runtime files (script, bin/, lib/) but excludes metadata files
/// (job.yaml, manifest.yaml, data-registry-fragment.yaml).
///
/// # Arguments
/// * `package_path` - Path to the .ljc package file
/// * `work_dir` - Directory to extract files into
///
/// # Returns
/// Path to the entry script (e.g., work_dir/script.sh)
pub fn extract_package_to_workdir(package_path: &Path, work_dir: &Path) -> Result<PathBuf> {
    // First read metadata to get entry script name
    let metadata = read_package_metadata(package_path)?;
    let entry_script = metadata.run
        .as_ref()
        .and_then(|r| r.entry.clone())
        .unwrap_or_else(|| "script.sh".to_string());

    info!(
        "Extracting package {} to {} (entry: {})",
        package_path.display(),
        work_dir.display(),
        entry_script
    );

    // Open and extract the archive
    let tar_gz = File::open(package_path)
        .context(format!("Failed to open package: {}", package_path.display()))?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);

    // Files to exclude from extraction (metadata, not needed at runtime)
    let exclude_files = ["job.yaml", "manifest.yaml", "data-registry-fragment.yaml"];

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.to_path_buf();
        let path_str = path.to_string_lossy().to_string();

        // Skip metadata files
        if exclude_files.iter().any(|&f| path_str == f) {
            continue;
        }

        // SECURITY: Prevent path traversal attacks
        if path_str.contains("..") {
            anyhow::bail!("Path traversal detected in archive: {}", path_str);
        }

        // Build destination path
        let dest = work_dir.join(&path);

        // Create parent directories if needed
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }

        // Extract based on entry type
        if entry.header().entry_type().is_dir() {
            std::fs::create_dir_all(&dest)?;
        } else {
            // Extract file
            let mut file = File::create(&dest)
                .context(format!("Failed to create file: {}", dest.display()))?;
            std::io::copy(&mut entry, &mut file)?;

            // Preserve permissions on Unix
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                if let Ok(mode) = entry.header().mode() {
                    std::fs::set_permissions(&dest, std::fs::Permissions::from_mode(mode))?;
                }
            }
        }
    }

    // Return path to entry script
    let script_path = work_dir.join(&entry_script);
    if !script_path.exists() {
        anyhow::bail!(
            "Entry script '{}' not found in package {}",
            entry_script,
            package_path.display()
        );
    }

    Ok(script_path)
}

// ============================================================================
// Version comparison
// ============================================================================

/// Compare two semantic versions.
///
/// Parses versions as dot-separated numbers and compares component by component.
/// Non-numeric parts are ignored.
///
/// # Examples
/// ```
/// use linearjc_core::package::compare_versions;
/// use std::cmp::Ordering;
///
/// assert_eq!(compare_versions("1.0.0", "1.0.1"), Ordering::Less);
/// assert_eq!(compare_versions("2.0.0", "1.9.9"), Ordering::Greater);
/// assert_eq!(compare_versions("1.0.0", "1.0.0"), Ordering::Equal);
/// ```
pub fn compare_versions(v1: &str, v2: &str) -> Ordering {
    let parse = |v: &str| -> Vec<u32> {
        v.split('.')
            .filter_map(|s| s.parse::<u32>().ok())
            .collect()
    };

    let parts1 = parse(v1);
    let parts2 = parse(v2);

    // Compare component by component
    let max_len = parts1.len().max(parts2.len());
    for i in 0..max_len {
        let p1 = parts1.get(i).copied().unwrap_or(0);
        let p2 = parts2.get(i).copied().unwrap_or(0);
        match p1.cmp(&p2) {
            Ordering::Equal => continue,
            other => return other,
        }
    }

    Ordering::Equal
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_versions_equal() {
        assert_eq!(compare_versions("1.0.0", "1.0.0"), Ordering::Equal);
        assert_eq!(compare_versions("2.1.3", "2.1.3"), Ordering::Equal);
    }

    #[test]
    fn test_compare_versions_less() {
        assert_eq!(compare_versions("1.0.0", "1.0.1"), Ordering::Less);
        assert_eq!(compare_versions("1.0.0", "1.1.0"), Ordering::Less);
        assert_eq!(compare_versions("1.0.0", "2.0.0"), Ordering::Less);
        assert_eq!(compare_versions("1.9.9", "2.0.0"), Ordering::Less);
    }

    #[test]
    fn test_compare_versions_greater() {
        assert_eq!(compare_versions("1.0.1", "1.0.0"), Ordering::Greater);
        assert_eq!(compare_versions("1.1.0", "1.0.0"), Ordering::Greater);
        assert_eq!(compare_versions("2.0.0", "1.0.0"), Ordering::Greater);
        assert_eq!(compare_versions("2.0.0", "1.9.9"), Ordering::Greater);
    }

    #[test]
    fn test_compare_versions_different_lengths() {
        assert_eq!(compare_versions("1.0", "1.0.0"), Ordering::Equal);
        assert_eq!(compare_versions("1.0.0", "1.0"), Ordering::Equal);
        assert_eq!(compare_versions("1.0.1", "1.0"), Ordering::Greater);
        assert_eq!(compare_versions("1.0", "1.0.1"), Ordering::Less);
    }
}
