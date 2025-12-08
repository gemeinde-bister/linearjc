use anyhow::{bail, Context, Result};
use colored::Colorize;
use std::fs;

use crate::utils::Repository;

pub fn run(job_id: &str) -> Result<()> {
    let repo = Repository::find()?;
    let job_path = repo.job_path(job_id);

    if !job_path.exists() {
        bail!("Job not found: {}", job_id);
    }

    // Read job.yaml
    let job_yaml = job_path.join("job.yaml");
    if !job_yaml.exists() {
        bail!("Missing job.yaml in {}", job_path.display());
    }

    let content = fs::read_to_string(&job_yaml)
        .context("Failed to read job.yaml")?;
    let data: serde_yaml::Value = serde_yaml::from_str(&content)
        .context("Failed to parse job.yaml")?;

    let job = data.get("job")
        .context("Missing 'job' section in job.yaml")?;

    // Get version as string (handle both string and number formats)
    let version_str = if let Some(v) = job.get("version") {
        match v {
            serde_yaml::Value::String(s) => s.clone(),
            serde_yaml::Value::Number(n) => n.to_string(),
            _ => "unknown".to_string(),
        }
    } else {
        "unknown".to_string()
    };

    let min_daily = job.get("schedule")
        .and_then(|s| s.get("min_daily"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let max_daily = job.get("schedule")
        .and_then(|s| s.get("max_daily"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    // Get run section info
    let user = job.get("run")
        .and_then(|r| r.get("user"))
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    let timeout = job.get("run")
        .and_then(|r| r.get("timeout"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    let isolation = job.get("run")
        .and_then(|r| r.get("isolation"))
        .and_then(|v| v.as_str())
        .unwrap_or("none");

    // Print header
    println!();
    println!("{} v{}", job_id.bright_blue().bold(), version_str.bright_white());
    println!("{}", "=".repeat(60).dimmed());
    println!();

    // Basic info
    println!("{}:", "Schedule".bright_white());
    println!("  Runs:    {}-{} per day", min_daily, max_daily);
    println!();

    println!("{}:", "Run".bright_white());
    println!("  User:      {}", user);
    println!("  Timeout:   {}s", timeout);
    println!("  Isolation: {}", isolation);
    println!();

    // Reads
    if let Some(reads) = job.get("reads").and_then(|r| r.as_sequence()) {
        println!("{}:", "Reads".bright_white());
        for item in reads {
            if let Some(key_str) = item.as_str() {
                println!("  - {}", key_str.bright_white());
            }
        }
        println!();
    }

    // Writes
    if let Some(writes) = job.get("writes").and_then(|w| w.as_sequence()) {
        println!("{}:", "Writes".bright_white());
        for item in writes {
            if let Some(key_str) = item.as_str() {
                println!("  - {}", key_str.bright_white());
            }
        }
        println!();
    }

    // Files
    println!("{}:", "Files".bright_white());
    let files = ["job.yaml", "script.sh"];
    for file in &files {
        let file_path = job_path.join(file);
        if file_path.exists() {
            println!("  {} {}", "✓".green(), file);
        } else {
            println!("  {} {}", "✗".red(), file);
        }
    }
    // Optional directories
    let bin_dir = job_path.join("bin");
    if bin_dir.exists() && bin_dir.is_dir() {
        println!("  {} {}", "✓".green(), "bin/");
    }
    let data_dir = job_path.join("data");
    if data_dir.exists() && data_dir.is_dir() {
        println!("  {} {}", "✓".green(), "data/");
    }
    let test_dir = job_path.join("test");
    if test_dir.exists() && test_dir.is_dir() {
        println!("  {} {}", "✓".green(), "test/");
    }
    println!();

    // Check if built
    let package_path = repo.dist_dir.join(format!("{}.ljc", job_id));
    if package_path.exists() {
        let size = fs::metadata(&package_path)
            .ok()
            .map(|m| m.len())
            .unwrap_or(0);
        println!("{}: {}", "Package".bright_white(), format!("{} ({} bytes)", package_path.display(), size).green());
    } else {
        println!("{}: {}", "Package".bright_white(), "Not built".yellow());
        println!("  Build with: ljc build {}", job_id);
    }
    println!();

    Ok(())
}
