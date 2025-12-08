use anyhow::{Context, Result, bail};
use colored::Colorize;
use flate2::read::GzDecoder;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use tar::Archive;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

// Use shared types from linearjc-core
use linearjc_core::job::{parse_job_yaml, JobFile};

/// Extract a .ljc package to local directory with executor-like structure
pub fn run(package: &Path, output: Option<&Path>) -> Result<()> {
    // Validate package exists
    if !package.exists() {
        bail!("Package not found: {}", package.display());
    }

    println!("{}", format!("Extracting package: {}", package.display()).bright_blue());
    println!("{}", "=".repeat(60).dimmed());
    println!();

    // Determine output directory
    let output_dir = if let Some(path) = output {
        path.to_path_buf()
    } else {
        PathBuf::from(".extract")
    };

    // Create temp directory for extraction
    let temp_dir = tempfile::tempdir()
        .context("Failed to create temporary directory")?;

    // Extract tar.gz to temp
    println!("{}", "Extracting package...".dimmed());
    let tar_gz = File::open(package)
        .with_context(|| format!("Failed to open {}", package.display()))?;
    let tar = GzDecoder::new(tar_gz);
    let mut archive = Archive::new(tar);

    archive.unpack(temp_dir.path())
        .context("Failed to extract package")?;

    println!("  {} Package extracted to temp", "✓".green());
    println!();

    // Read package metadata from job.yaml
    // Note: packages do NOT contain manifest.yaml (legacy format removed)
    println!("{}", "Reading package metadata...".dimmed());

    let job: JobFile = {
        let job_path = temp_dir.path().join("job.yaml");
        if !job_path.exists() {
            bail!("job.yaml not found in package");
        }
        let content = fs::read_to_string(&job_path)?;
        parse_job_yaml(&content)
            .context("Failed to parse job.yaml")?
    };

    println!("  {} Job: {} v{}", "→".dimmed(), job.job.id, job.job.version);
    println!("  {} Reads: {:?}", "→".dimmed(), job.job.reads);
    println!("  {} Writes: {:?}", "→".dimmed(), job.job.writes);
    println!();

    // Create executor-like structure
    println!("{}", "Creating executor directory structure...".dimmed());

    let work_dir = output_dir.join("work").join("test-execution-id");
    let in_dir = work_dir.join("in");
    let out_dir = work_dir.join("out");
    let tmp_dir = work_dir.join("tmp");

    fs::create_dir_all(&in_dir)
        .context("Failed to create in directory")?;
    fs::create_dir_all(&out_dir)
        .context("Failed to create out directory")?;
    fs::create_dir_all(&tmp_dir)
        .context("Failed to create tmp directory")?;

    println!("  {} {}/", "✓".green(), work_dir.display());
    println!();

    // Create input placeholders (directories - user populates with test data)
    println!("{}", "Creating input placeholders...".dimmed());

    for registry_key in &job.job.reads {
        let dir_path = in_dir.join(registry_key);
        fs::create_dir_all(&dir_path)?;
        fs::write(
            dir_path.join("README.txt"),
            format!(
                "# Placeholder directory for input: {}\n# \
                 Add your test data here before running.\n# \
                 For file inputs, replace this directory with a file.\n",
                registry_key
            )
        )?;
        println!("  {} in/{}/ (directory)", "→".dimmed(), registry_key);
    }

    println!();

    // Create output directories
    println!("{}", "Creating output structure...".dimmed());

    for registry_key in &job.job.writes {
        let dir_path = out_dir.join(registry_key);
        fs::create_dir_all(&dir_path)?;
        println!("  {} out/{}/ (directory)", "→".dimmed(), registry_key);
    }

    println!();

    // Copy script
    println!("{}", "Copying job script...".dimmed());

    let src_script = temp_dir.path().join("script.sh");
    let dst_script = work_dir.join("script.sh");

    if !src_script.exists() {
        bail!("script.sh not found in package");
    }

    fs::copy(&src_script, &dst_script)
        .context("Failed to copy script.sh")?;

    // Make script executable
    #[cfg(unix)]
    {
        let metadata = fs::metadata(&dst_script)?;
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&dst_script, permissions)?;
    }

    println!("  {} script.sh (executable)", "✓".green());
    println!();

    // Copy bin/ directory if exists
    let src_bin = temp_dir.path().join("bin");
    if src_bin.exists() && src_bin.is_dir() {
        println!("{}", "Copying bin/ directory...".dimmed());
        let dst_bin = work_dir.join("bin");
        copy_dir_recursive(&src_bin, &dst_bin)?;
        println!("  {} bin/ (bundled binaries)", "✓".green());
        println!();
    }

    // Copy data/ directory if exists
    let src_data = temp_dir.path().join("data");
    if src_data.exists() && src_data.is_dir() {
        println!("{}", "Copying data/ directory...".dimmed());
        let dst_data = work_dir.join("data");
        copy_dir_recursive(&src_data, &dst_data)?;
        println!("  {} data/ (bundled data)", "✓".green());
        println!();
    }

    // Create run.sh helper
    println!("{}", "Creating run.sh helper...".dimmed());

    let run_script = format!(
        r#"#!/bin/sh
# Test script for: {}
# Run this to execute job locally (without isolation)

export LINEARJC_JOB_ID="{}"
export LINEARJC_EXECUTION_ID="test-execution-id"

# SPEC.md v0.5.0: Relative paths - scripts use in/, out/, tmp/, bin/, data/
# No environment variables needed for paths

echo "=========================================="
echo "Running job: {}"
echo "=========================================="
echo ""
echo "Inputs:"
ls -la in/
echo ""
echo "Outputs (before):"
ls -la out/
echo ""
echo "Executing script..."
echo ""
./script.sh
EXIT_CODE=$?
echo ""
echo "=========================================="
echo "Job completed with exit code: $EXIT_CODE"
echo "=========================================="
echo ""
echo "Outputs (after):"
ls -la out/
exit $EXIT_CODE
"#,
        job.job.id,
        job.job.id,
        job.job.id
    );

    let run_path = work_dir.join("run.sh");
    fs::write(&run_path, run_script)
        .context("Failed to write run.sh")?;

    // Make run.sh executable
    #[cfg(unix)]
    {
        let metadata = fs::metadata(&run_path)?;
        let mut permissions = metadata.permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(&run_path, permissions)?;
    }

    println!("  {} run.sh (executable)", "✓".green());
    println!();

    // Success message
    println!("{}", "✓ Package extracted successfully".green());
    println!();
    println!("Structure:");
    println!("  {}/", work_dir.display().to_string().bright_white());
    println!("  ├── in/          {}", "# Inputs (add test data here)".dimmed());
    println!("  ├── out/         {}", "# Outputs (job writes here)".dimmed());
    println!("  ├── tmp/         {}", "# Temp space".dimmed());
    println!("  ├── script.sh    {}", "# Job script".dimmed());
    println!("  └── run.sh       {}", "# Helper to test locally".dimmed());
    println!();
    println!("Test locally:");
    println!("  {}", format!("cd {}", work_dir.display()).bright_white());
    println!("  {}", "# Add test data to in/ directories".yellow());
    println!("  {}", "./run.sh".bright_white());
    println!();
    println!("{}", "Note: Local execution does NOT apply isolation (landlock, cgroups, network).".yellow());
    println!("{}", "      For isolated testing, deploy to a Phase 7 executor.".yellow());

    Ok(())
}

/// Recursively copy a directory
fn copy_dir_recursive(src: &Path, dst: &Path) -> Result<()> {
    fs::create_dir_all(dst)?;
    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let src_path = entry.path();
        let dst_path = dst.join(entry.file_name());

        if src_path.is_dir() {
            copy_dir_recursive(&src_path, &dst_path)?;
        } else {
            fs::copy(&src_path, &dst_path)?;

            // Preserve executable bit
            #[cfg(unix)]
            {
                let src_meta = fs::metadata(&src_path)?;
                let src_mode = src_meta.permissions().mode();
                if src_mode & 0o111 != 0 {
                    let mut perms = fs::metadata(&dst_path)?.permissions();
                    perms.set_mode(src_mode);
                    fs::set_permissions(&dst_path, perms)?;
                }
            }
        }
    }
    Ok(())
}

// Note: Package parsing types now imported from linearjc_core::job and linearjc_core::package
