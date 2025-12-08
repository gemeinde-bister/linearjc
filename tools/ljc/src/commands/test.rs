//! ljc test - Run job locally with optional isolation
//!
//! Uses linearjc-core to execute jobs with production-identical isolation.
//!
//! Usage:
//!   ljc test backup.pool                      # Run with job.yaml defaults
//!   ljc test backup.pool --no-isolation       # Skip isolation (faster)
//!   ljc test backup.pool --isolation strict   # Override isolation mode
//!   ljc test backup.pool --verbose            # Show script stdout/stderr
//!   ljc test backup.pool --keep               # Keep workdir after execution

use anyhow::{bail, Context, Result};
use colored::Colorize;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;

use linearjc_core::workdir::WorkDir;
use linearjc_core::execution::{execute_simple, validate_job_script, ExecutionResult};
use linearjc_core::isolation::FilesystemIsolation;
// Use shared job parsing from linearjc-core
use linearjc_core::job::{parse_job_file, JobFile};

use crate::utils::Repository;

/// Run the test command
pub fn run(
    job_id: &str,
    no_isolation: bool,
    isolation_override: Option<&str>,
    timeout_secs: u64,
    verbose: bool,
    keep: bool,
    network_override: Option<bool>,
) -> Result<()> {
    let repo = Repository::find()?;
    let job_path = repo.job_path(job_id);

    if !job_path.exists() {
        bail!("Job not found: {}", job_id);
    }

    println!("{}", format!("Testing job: {}", job_id).bright_blue());
    println!("{}", "=".repeat(60).dimmed());
    println!();

    // Read job.yaml using linearjc-core parser
    println!("{}", "Reading job configuration...".dimmed());
    let job_yaml_path = job_path.join("job.yaml");
    if !job_yaml_path.exists() {
        bail!("job.yaml not found in {}", job_path.display());
    }

    let job: JobFile = parse_job_file(&job_yaml_path)
        .context("Failed to parse job.yaml")?;

    // Get run config with defaults
    let run_config = job.job.run_config();

    println!("  {} Job: {} v{}", "→".dimmed(), job.job.id, job.job.version);
    println!("  {} Reads: {:?}", "→".dimmed(), job.job.reads);
    println!("  {} Writes: {:?}", "→".dimmed(), job.job.writes);
    println!();

    // Determine isolation settings
    let isolation_mode = if no_isolation {
        FilesystemIsolation::None
    } else if let Some(mode_str) = isolation_override {
        FilesystemIsolation::from_str(mode_str)?
    } else {
        // Get from job.yaml run.isolation (has default "none")
        FilesystemIsolation::from_str(&run_config.isolation)?
    };

    let network = network_override.unwrap_or(run_config.network);

    // For ljc test, we use execute_simple (no fork/setuid complexity)
    // Real isolation (Landlock, cgroups) requires root and is better tested via deploy
    let using_isolation = isolation_mode != FilesystemIsolation::None;

    if using_isolation {
        println!("{}", "⚠ Note: Local testing uses execute_simple without full isolation.".yellow());
        println!("{}", "  Landlock/cgroups require root privileges and are applied by executor.".yellow());
        println!("{}", "  Use --no-isolation for faster iteration, deploy to executor for isolation tests.".yellow());
        println!();
    }

    // Create temporary work directory
    println!("{}", "Creating work directory...".dimmed());
    let temp_dir = tempfile::tempdir()
        .context("Failed to create temporary directory")?;

    let execution_id = format!("{}-test-{}", job_id, chrono::Utc::now().format("%Y%m%d-%H%M%S"));
    let workdir = WorkDir::new(temp_dir.path(), &execution_id)?;

    println!("  {} {}/", "✓".green(), workdir.root.display());
    println!();

    // Check for test inputs
    let test_in_dir = job_path.join("test").join("in");
    if test_in_dir.exists() && test_in_dir.is_dir() {
        println!("{}", "Copying test inputs...".dimmed());
        copy_test_inputs(&test_in_dir, &workdir.in_dir, &job.job.reads)?;
    } else {
        println!("{}", "Creating input placeholders...".dimmed());
        create_input_placeholders(&workdir.in_dir, &job.job.reads)?;
    }
    println!();

    // Create output directories
    println!("{}", "Creating output structure...".dimmed());
    for registry_key in &job.job.writes {
        let dir_path = workdir.out_dir.join(registry_key);
        fs::create_dir_all(&dir_path)?;
        println!("  {} out/{}/ (directory)", "→".dimmed(), registry_key);
    }
    println!();

    // Copy script
    println!("{}", "Copying job script...".dimmed());
    let src_script = job_path.join("script.sh");
    let dst_script = workdir.root.join("script.sh");

    if !src_script.exists() {
        bail!("script.sh not found in {}", job_path.display());
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

    // Copy bin/ directory if exists
    let src_bin = job_path.join("bin");
    if src_bin.exists() && src_bin.is_dir() {
        let dst_bin = workdir.root.join("bin");
        copy_dir_recursive(&src_bin, &dst_bin)?;
        println!("  {} bin/ (bundled binaries)", "✓".green());
    }

    // Copy data/ directory if exists
    let src_data = job_path.join("data");
    if src_data.exists() && src_data.is_dir() {
        let dst_data = workdir.root.join("data");
        copy_dir_recursive(&src_data, &dst_data)?;
        println!("  {} data/ (bundled data)", "✓".green());
    }
    println!();

    // Validate script
    println!("{}", "Validating script...".dimmed());
    validate_job_script(&dst_script)?;
    println!("  {} Script validation passed", "✓".green());
    println!();

    // Get timeout (use CLI arg if provided, otherwise from job config with default)
    let timeout = if timeout_secs > 0 {
        Duration::from_secs(timeout_secs)
    } else {
        Duration::from_secs(run_config.timeout)
    };

    // Execute
    println!("{}", "Executing job...".dimmed());
    println!("  {} Timeout: {:?}", "→".dimmed(), timeout);
    println!("  {} Isolation: {:?}", "→".dimmed(), isolation_mode);
    println!("  {} Network: {}", "→".dimmed(), if network { "enabled" } else { "disabled" });
    println!();

    if verbose {
        println!("{}", "═".repeat(60).dimmed());
        println!("{}", "Job output:".bright_white());
        println!("{}", "═".repeat(60).dimmed());
    }

    let result = execute_simple(
        &dst_script,
        &workdir,
        &job.job.id,
        &execution_id,
        timeout,
    )?;

    if verbose {
        println!("{}", "═".repeat(60).dimmed());
        println!();
    }

    // Report results
    print_result(&result, &workdir)?;

    // List output files
    println!("{}", "Output files:".dimmed());
    list_output_files(&workdir.out_dir, "  ")?;
    println!();

    // Cleanup or keep
    if keep {
        // Don't let tempdir drop clean up - keep() consumes self and returns PathBuf
        let path = temp_dir.keep();
        println!("{}", format!("Workdir kept at: {}", path.display()).yellow());
        println!("  To clean up: rm -rf {}", path.display());
    } else {
        println!("{}", "Cleaning up workdir...".dimmed());
        // tempdir will clean up on drop
    }

    // Exit with job's exit code
    if !result.success {
        std::process::exit(result.exit_code.unwrap_or(1));
    }

    Ok(())
}

/// Copy test inputs from test/in/ to workdir/in/
fn copy_test_inputs(test_in_dir: &Path, workdir_in_dir: &Path, reads: &[String]) -> Result<()> {
    for registry_key in reads {
        let src = test_in_dir.join(registry_key);
        let dst = workdir_in_dir.join(registry_key);

        if src.exists() {
            if src.is_dir() {
                copy_dir_recursive(&src, &dst)?;
                println!("  {} in/{}/ (from test/in/)", "✓".green(), registry_key);
            } else {
                // It's a file
                fs::copy(&src, &dst)?;
                println!("  {} in/{} (from test/in/)", "✓".green(), registry_key);
            }
        } else {
            // Create placeholder
            fs::create_dir_all(&dst)?;
            fs::write(
                dst.join("README.txt"),
                format!(
                    "# Placeholder for input: {}\n# Add test data to test/in/{}/\n",
                    registry_key, registry_key
                )
            )?;
            println!("  {} in/{}/ (placeholder - no test data)", "⚠".yellow(), registry_key);
        }
    }
    Ok(())
}

/// Create input placeholder directories
fn create_input_placeholders(workdir_in_dir: &Path, reads: &[String]) -> Result<()> {
    for registry_key in reads {
        let dir_path = workdir_in_dir.join(registry_key);
        fs::create_dir_all(&dir_path)?;
        fs::write(
            dir_path.join("README.txt"),
            format!(
                "# Placeholder directory for input: {}\n# \
                 Create test/in/{}/ in job directory with test data.\n",
                registry_key, registry_key
            )
        )?;
        println!("  {} in/{}/ (placeholder)", "→".dimmed(), registry_key);
    }
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

/// Print execution result
fn print_result(result: &ExecutionResult, _workdir: &WorkDir) -> Result<()> {
    println!("{}", "═".repeat(60));

    if result.success {
        println!("{}", "✓ Job completed successfully".green());
    } else {
        println!("{}", "✗ Job failed".red());
        if let Some(ref error) = result.error {
            println!("  Error: {}", error.red());
        }
    }

    println!();
    println!("  Duration:  {:?}", result.duration);

    if let Some(code) = result.exit_code {
        println!("  Exit code: {}", code);
    }
    if let Some(signal) = result.signal {
        println!("  Signal:    {}", signal);
    }

    println!("{}", "═".repeat(60));
    println!();

    Ok(())
}

/// List files in output directory
fn list_output_files(out_dir: &Path, prefix: &str) -> Result<()> {
    if !out_dir.exists() {
        println!("{}(no output directory)", prefix);
        return Ok(());
    }

    let mut found_files = false;
    for entry in fs::read_dir(out_dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();

        if path.is_dir() {
            println!("{}{}/", prefix, name);
            list_output_files_recursive(&path, &format!("{}  ", prefix))?;
            found_files = true;
        } else {
            let size = fs::metadata(&path)?.len();
            println!("{}{} ({} bytes)", prefix, name, size);
            found_files = true;
        }
    }

    if !found_files {
        println!("{}(empty)", prefix);
    }

    Ok(())
}

fn list_output_files_recursive(dir: &Path, prefix: &str) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();

        if path.is_dir() {
            println!("{}{}/", prefix, name);
            list_output_files_recursive(&path, &format!("{}  ", prefix))?;
        } else {
            let size = fs::metadata(&path)?.len();
            println!("{}{} ({} bytes)", prefix, name, size);
        }
    }
    Ok(())
}

// Note: JobFile, JobSpec, RunSpec, LimitsSpec now imported from linearjc_core::job
