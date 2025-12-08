use anyhow::{bail, Context, Result};
use colored::Colorize;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::fs::{self, File};
use std::path::Path;
use tar::Builder;

use crate::utils::Repository;

pub fn run(job_id: &str, output: Option<&Path>) -> Result<()> {
    let repo = Repository::find()?;
    let job_path = repo.job_path(job_id);

    if !job_path.exists() {
        bail!("Job not found: {}", job_id);
    }

    println!("{}", format!("Building package: {}", job_id).bright_blue());
    println!("{}", "=".repeat(60).dimmed());
    println!();

    // Check required files
    println!("{}", "Checking required files...".dimmed());
    let required_files = vec!["job.yaml", "script.sh"];

    let mut all_present = true;
    for file in &required_files {
        let file_path = job_path.join(file);
        if file_path.exists() {
            println!("  {} {}", "✓".green(), file);
        } else {
            println!("  {} {} {}", "✗".red(), file, "(required)".red());
            all_present = false;
        }
    }

    if !all_present {
        bail!("Missing required files - cannot build package");
    }

    println!();

    // Determine output path
    let output_path = if let Some(path) = output {
        path.to_path_buf()
    } else {
        repo.dist_dir.join(format!("{}.ljc", job_id))
    };

    println!("Output: {}", output_path.display().to_string().bright_white());
    println!();

    // Create tar.gz archive
    println!("{}", "Creating package...".dimmed());

    let tar_file = File::create(&output_path)
        .with_context(|| format!("Failed to create {}", output_path.display()))?;

    let encoder = GzEncoder::new(tar_file, Compression::default());
    let mut tar = Builder::new(encoder);

    // Add required files (always included)
    for file in &["job.yaml", "script.sh"] {
        let file_path = job_path.join(file);
        tar.append_path_with_name(&file_path, file)
            .with_context(|| format!("Failed to add {}", file))?;
        println!("  {} {}", "→".dimmed(), file);
    }

    // Add bin/ directory if exists (bundled binaries)
    let bin_dir = job_path.join("bin");
    if bin_dir.exists() && bin_dir.is_dir() {
        tar.append_dir_all("bin", &bin_dir)
            .context("Failed to add bin/ directory")?;
        println!("  {} {}", "→".dimmed(), "bin/");
    }

    // Add data/ directory if exists (bundled static data)
    let data_dir = job_path.join("data");
    if data_dir.exists() && data_dir.is_dir() {
        tar.append_dir_all("data", &data_dir)
            .context("Failed to add data/ directory")?;
        println!("  {} {}", "→".dimmed(), "data/");
    }

    // Finish the archive
    let encoder = tar.into_inner()
        .context("Failed to finalize tar archive")?;

    // Ensure all data is written
    drop(encoder);

    println!();

    // Get package size
    let metadata = fs::metadata(&output_path)?;
    let size = metadata.len();

    println!("{}", "✓ Package built successfully".green());
    println!();
    println!("Package: {}", output_path.display().to_string().bright_white());
    println!("Size:    {} bytes ({:.2} KB)", size, size as f64 / 1024.0);
    println!();
    println!("Next steps:");
    println!("  ljc validate {}       {}", job_id, "# Validate before deployment".dimmed());
    println!("  ljc deploy {} --to <coordinator>", output_path.display());

    Ok(())
}
