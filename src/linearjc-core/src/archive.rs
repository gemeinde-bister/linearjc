//! Secure archive handling for LinearJC.
//!
//! All transfers use tar.gz format with security checks:
//! - Symlinks are BLOCKED to prevent directory escape attacks
//! - Path traversal is validated (no ../)
//! - Uses Rust tar library (not subprocess) to prevent command injection
//!
//! This module is shared between executor and ljc tool.

use anyhow::{Context, Result};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use tar::{Archive, Builder};

/// Extract tar.gz archive to directory using Rust tar library.
///
/// SECURITY: Prevents command injection via paths (no subprocess).
///
/// # Arguments
/// * `archive_path` - Path to the tar.gz file
/// * `extract_to` - Directory to extract contents to
///
/// # Errors
/// Returns error if archive cannot be opened or extracted.
pub fn extract_tar_gz(archive_path: &Path, extract_to: &Path) -> Result<()> {
    let tar_gz = File::open(archive_path)
        .context(format!("Failed to open archive: {}", archive_path.display()))?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);

    archive
        .unpack(extract_to)
        .context(format!("Failed to extract to: {}", extract_to.display()))?;

    Ok(())
}

/// Extract tar.gz archive with security validation.
///
/// SECURITY: Validates each entry for:
/// - Symlinks (blocked)
/// - Path traversal (blocked)
/// - Absolute paths (blocked)
///
/// # Arguments
/// * `archive_path` - Path to the tar.gz file
/// * `extract_to` - Directory to extract contents to
///
/// # Errors
/// Returns error if archive contains unsafe entries.
pub fn extract_tar_gz_secure(archive_path: &Path, extract_to: &Path) -> Result<()> {
    let tar_gz = File::open(archive_path)
        .context(format!("Failed to open archive: {}", archive_path.display()))?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);

    let extract_to_resolved = extract_to.canonicalize()
        .unwrap_or_else(|_| extract_to.to_path_buf());

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;
        let path_str = path.to_string_lossy();

        // SECURITY: Block symlinks
        if entry.header().entry_type().is_symlink() || entry.header().entry_type().is_hard_link() {
            anyhow::bail!(
                "Archive contains symlink: {} (symlinks blocked for security)",
                path_str
            );
        }

        // SECURITY: Block absolute paths
        if path_str.starts_with('/') || path_str.starts_with('\\') {
            anyhow::bail!(
                "Archive contains absolute path: {} (only relative paths allowed)",
                path_str
            );
        }

        // SECURITY: Block path traversal
        if path_str.contains("..") {
            anyhow::bail!(
                "Archive contains path traversal: {} (.. not allowed)",
                path_str
            );
        }

        // Validate final path is within extract_to
        let dest = extract_to.join(&*path);
        let dest_resolved = dest.canonicalize().unwrap_or(dest.clone());

        if !dest_resolved.starts_with(&extract_to_resolved) {
            anyhow::bail!(
                "Archive entry escapes destination: {} -> {}",
                path_str,
                dest_resolved.display()
            );
        }

        // Extract safely
        entry.unpack_in(extract_to)?;
    }

    Ok(())
}

/// Create tar.gz archive from a single file.
///
/// SECURITY: Uses Rust tar library (not subprocess) to prevent command injection.
///
/// # Arguments
/// * `source_file` - Path to the file to archive
/// * `archive_path` - Destination path for the archive
pub fn create_tar_gz_from_file(source_file: &Path, archive_path: &Path) -> Result<()> {
    let tar_gz = File::create(archive_path)
        .context(format!("Failed to create archive: {}", archive_path.display()))?;
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = Builder::new(enc);

    // Get just the filename for the archive entry
    let filename = source_file.file_name()
        .ok_or_else(|| anyhow::anyhow!("Invalid filename"))?;

    // Add the file to the archive root
    let mut file = File::open(source_file)
        .context(format!("Failed to open file: {}", source_file.display()))?;
    tar.append_file(filename, &mut file)
        .context(format!("Failed to archive file: {}", source_file.display()))?;

    tar.finish()
        .context("Failed to finalize archive")?;

    Ok(())
}

/// Create tar.gz archive from directory contents.
///
/// SECURITY: Uses Rust tar library (not subprocess) to prevent command injection.
///
/// Archives the contents of the directory, not the directory itself.
///
/// # Arguments
/// * `source_dir` - Path to the directory to archive
/// * `archive_path` - Destination path for the archive
pub fn create_tar_gz_from_dir(source_dir: &Path, archive_path: &Path) -> Result<()> {
    let tar_gz = File::create(archive_path)
        .context(format!("Failed to create archive: {}", archive_path.display()))?;
    let enc = GzEncoder::new(tar_gz, Compression::default());
    let mut tar = Builder::new(enc);

    // Add all contents of source_dir to archive root (not the directory itself)
    tar.append_dir_all(".", source_dir)
        .context(format!("Failed to archive: {}", source_dir.display()))?;

    tar.finish()
        .context("Failed to finalize archive")?;

    Ok(())
}

/// Read a specific file from inside a tar.gz archive.
///
/// # Arguments
/// * `archive_path` - Path to the tar.gz file
/// * `filename` - Name of the file to read (can be at any level)
///
/// # Returns
/// Contents of the file as a String
pub fn read_file_from_archive(archive_path: &Path, filename: &str) -> Result<String> {
    let tar_gz = File::open(archive_path)
        .context(format!("Failed to open archive: {}", archive_path.display()))?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);

    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?;

        // Look for file at any level
        if path.file_name().map(|n| n == filename).unwrap_or(false) {
            let mut contents = String::new();
            entry.read_to_string(&mut contents)?;
            return Ok(contents);
        }
    }

    anyhow::bail!("File '{}' not found in archive: {}", filename, archive_path.display())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_create_and_extract_file() {
        let temp = TempDir::new().unwrap();

        // Create source file
        let source = temp.path().join("test.txt");
        fs::write(&source, "hello world").unwrap();

        // Create archive
        let archive = temp.path().join("test.tar.gz");
        create_tar_gz_from_file(&source, &archive).unwrap();

        // Extract archive
        let extract_dir = temp.path().join("extracted");
        fs::create_dir_all(&extract_dir).unwrap();
        extract_tar_gz(&archive, &extract_dir).unwrap();

        // Verify contents
        let extracted = extract_dir.join("test.txt");
        assert!(extracted.exists());
        assert_eq!(fs::read_to_string(&extracted).unwrap(), "hello world");
    }

    #[test]
    fn test_create_and_extract_dir() {
        let temp = TempDir::new().unwrap();

        // Create source directory with files
        let source_dir = temp.path().join("source");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(source_dir.join("file1.txt"), "content 1").unwrap();
        fs::write(source_dir.join("file2.txt"), "content 2").unwrap();

        // Create archive
        let archive = temp.path().join("dir.tar.gz");
        create_tar_gz_from_dir(&source_dir, &archive).unwrap();

        // Extract archive
        let extract_dir = temp.path().join("extracted");
        fs::create_dir_all(&extract_dir).unwrap();
        extract_tar_gz(&archive, &extract_dir).unwrap();

        // Verify contents
        assert!(extract_dir.join("file1.txt").exists());
        assert!(extract_dir.join("file2.txt").exists());
        assert_eq!(fs::read_to_string(extract_dir.join("file1.txt")).unwrap(), "content 1");
        assert_eq!(fs::read_to_string(extract_dir.join("file2.txt")).unwrap(), "content 2");
    }

    #[test]
    fn test_read_file_from_archive() {
        let temp = TempDir::new().unwrap();

        // Create source directory with job.yaml
        let source_dir = temp.path().join("source");
        fs::create_dir_all(&source_dir).unwrap();
        fs::write(source_dir.join("job.yaml"), "job:\n  id: test.job\n  version: 1.0.0").unwrap();

        // Create archive
        let archive = temp.path().join("test.tar.gz");
        create_tar_gz_from_dir(&source_dir, &archive).unwrap();

        // Read job.yaml from archive
        let contents = read_file_from_archive(&archive, "job.yaml").unwrap();
        assert!(contents.contains("test.job"));
    }
}
