use anyhow::{bail, Result};
use colored::Colorize;
use std::fs;
use std::os::unix::fs::PermissionsExt;

use crate::utils::Repository;

pub fn run(job_id: &str) -> Result<()> {
    let repo = Repository::find()?;
    let job_path = repo.job_path(job_id);

    if job_path.exists() {
        bail!("Job directory already exists: {}", job_path.display());
    }

    println!("{}", format!("Creating new job: {}", job_id).bright_blue());
    println!("{}", "=".repeat(60).dimmed());
    println!();

    // Create job directory
    fs::create_dir_all(&job_path)?;
    println!("Created: {}", job_path.display().to_string().bright_white());
    println!();

    // Generate template files
    create_job_yaml(&job_path, job_id)?;
    create_script_sh(&job_path, job_id)?;

    // Create test/ directory with in/ subdirectory
    let test_in_path = job_path.join("test").join("in");
    fs::create_dir_all(&test_in_path)?;
    println!("  {} test/in/", "✓".green());

    println!();
    println!("{}", "✓ Job structure created".green());
    println!();
    println!("Next steps:");
    println!("  1. Define registers in:  {}", "registry.yaml".dimmed());
    println!("  2. Update job.yaml:      {}", format!("{}/job.yaml", job_path.display()).dimmed());
    println!("  3. Implement job logic:  {}", format!("{}/script.sh", job_path.display()).dimmed());
    println!("  4. Add test inputs:      {}", format!("{}/test/in/", job_path.display()).dimmed());
    println!("  5. Validate:             {}", format!("ljc validate {}", job_id).dimmed());
    println!("  6. Build:                {}", format!("ljc build {}", job_id).dimmed());

    Ok(())
}

fn create_job_yaml(job_path: &std::path::Path, job_id: &str) -> Result<()> {
    let content = format!(r#"job:
  id: {job_id}
  version: 1.0.0

  reads: []           # Input registers (from registry.yaml)
  writes: []          # Output registers (this job owns these)

  depends: []         # Jobs that must complete first

  schedule:
    min_daily: 1      # Minimum executions per 24h
    max_daily: 5      # Maximum executions per 24h

  run:
    user: nobody      # Execution user
    timeout: 300      # Seconds
    # entry: script.sh           # Entry script (default)
    # binaries: []               # Bundled binaries in bin/

    # Process isolation (optional)
    # isolation: none            # strict | relaxed | none
    # network: true              # Allow network access
    # extra_read_paths: []       # For relaxed mode only
    # limits:
    #   cpu_percent: 100
    #   memory_mb: 512
    #   processes: 100
"#, job_id = job_id);

    fs::write(job_path.join("job.yaml"), content)?;
    println!("  {} job.yaml", "✓".green());
    Ok(())
}

fn create_script_sh(job_path: &std::path::Path, job_id: &str) -> Result<()> {
    let content = format!(r#"#!/bin/sh
# {job_id} - Job execution script
set -e

echo "=== {job_id} ==="
echo "Started: $(date)"

# Workdir structure (all paths relative):
#   in/       - Input data (read-only, populated by executor)
#   out/      - Output data (write here)
#   tmp/      - Scratch space (use freely)
#   bin/      - Bundled binaries (if any)
#   data/     - Bundled static data (if any)

# Example: Read input
# cat in/my_input

# Example: Use bundled binary
# ./bin/processor --input in/data --output out/result

# Example: Write output
# echo "result" > out/my_output

# TODO: Implement your job logic here
echo "Processing..."

# Validate outputs exist before exiting
# [ -f out/my_output ] || exit 1

echo "Completed: $(date)"
exit 0
"#, job_id = job_id);

    let script_path = job_path.join("script.sh");
    fs::write(&script_path, content)?;

    // Make executable
    let mut perms = fs::metadata(&script_path)?.permissions();
    perms.set_mode(0o755);
    fs::set_permissions(&script_path, perms)?;

    println!("  {} script.sh {}", "✓".green(), "(executable)".dimmed());
    Ok(())
}

