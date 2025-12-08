use anyhow::Result;
use colored::Colorize;
use std::fs;

use crate::utils::Repository;

pub fn run() -> Result<()> {
    let repo = Repository::find()?;
    let jobs = repo.list_jobs()?;

    if jobs.is_empty() {
        println!("{}", "No jobs found".dimmed());
        println!();
        println!("Create a job with: ljc new <job-id>");
        return Ok(());
    }

    println!("{}", "Jobs in repository:".bright_blue());
    println!("{}", "=".repeat(60).dimmed());
    println!();

    for job_id in &jobs {
        let job_path = repo.job_path(job_id);

        // Try to read version from job.yaml
        let version = read_job_version(&job_path);

        // Check if package exists
        let package_path = repo.dist_dir.join(format!("{}.ljc", job_id));
        let has_package = package_path.exists();

        print!("  {} ", job_id.bright_white());

        if let Some(v) = version {
            print!("v{} ", v.dimmed());
        }

        if has_package {
            print!("{}", "[built]".green());
        }

        println!();
    }

    println!();
    println!("Total: {} job(s)", jobs.len());
    println!();
    println!("Commands:");
    println!("  ljc info <job-id>      {}", "Show job details".dimmed());
    println!("  ljc validate --all     {}", "Validate all jobs".dimmed());
    println!("  ljc build <job-id>     {}", "Build package".dimmed());

    Ok(())
}

fn read_job_version(job_path: &std::path::Path) -> Option<String> {
    let job_yaml = job_path.join("job.yaml");
    if !job_yaml.exists() {
        return None;
    }

    let content = fs::read_to_string(job_yaml).ok()?;
    let data: serde_yaml::Value = serde_yaml::from_str(&content).ok()?;

    data.get("job")
        .and_then(|j| j.get("version"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}
