use anyhow::{bail, Context, Result};
use colored::Colorize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::io::{BufRead, BufReader};

use crate::utils::{Repository, YamlUtils};

#[derive(Debug)]
struct JobValidation {
    job_id: String,
    errors: Vec<String>,
    warnings: Vec<String>,
    reads: Vec<String>,
    writes: Vec<String>,
}

impl JobValidation {
    fn new(job_id: String) -> Self {
        Self {
            job_id,
            errors: Vec::new(),
            warnings: Vec::new(),
            reads: Vec::new(),
            writes: Vec::new(),
        }
    }

    fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }
}

pub fn run(job_id: Option<&str>, all: bool) -> Result<()> {
    let repo = Repository::find()?;

    // Determine which jobs to validate
    let jobs_to_validate = if all || job_id.is_none() {
        repo.list_jobs()?
    } else if let Some(id) = job_id {
        if !repo.job_path(id).exists() {
            bail!("Job not found: {}", id);
        }
        vec![id.to_string()]
    } else {
        bail!("Specify a job ID or use --all");
    };

    if jobs_to_validate.is_empty() {
        println!("{}", "No jobs to validate".yellow());
        return Ok(());
    }

    println!("{}", "Validating jobs...".bright_blue());
    println!("{}", "=".repeat(60).dimmed());
    println!();

    // Load registry
    let registry = YamlUtils::parse_registry(&repo.registry_file)?;

    // Validate each job
    let mut validations = Vec::new();
    for job_id in &jobs_to_validate {
        let validation = validate_job(&repo, job_id, &registry)?;
        validations.push(validation);
    }

    // Check for output conflicts across jobs
    check_output_conflicts(&mut validations);

    // Check for orphaned registry entries
    let orphaned = check_orphaned_entries(&registry, &validations);

    // Display results
    for validation in &validations {
        display_validation(validation);
    }

    // Summary
    println!();
    println!("{}", "Registry Check:".bright_white());
    println!("{}", "─".repeat(60).dimmed());

    let total_errors: usize = validations.iter().map(|v| v.errors.len()).sum();
    let total_warnings: usize = validations.iter().map(|v| v.warnings.len()).sum();

    if total_errors == 0 {
        println!("{}", "✓ No output conflicts".green());
    }

    if !orphaned.is_empty() {
        println!("{} Orphaned entries (not used by any job):", "⚠".yellow());
        for key in &orphaned {
            println!("  - {}", key.dimmed());
        }
    }

    println!();
    println!("{}", "Summary:".bright_white());
    println!("  Jobs:            {}", validations.len());
    println!("  Registry entries: {}", registry.len());
    println!("  Orphaned:        {}", orphaned.len());
    println!("  Errors:          {}", if total_errors == 0 { total_errors.to_string().green() } else { total_errors.to_string().red() });
    println!("  Warnings:        {}", if total_warnings == 0 { total_warnings.to_string().green() } else { total_warnings.to_string().yellow() });

    println!();

    if total_errors > 0 {
        bail!("Validation failed with {} error(s)", total_errors);
    }

    println!("{}", "✓ Validation passed".green());
    Ok(())
}

fn validate_job(repo: &Repository, job_id: &str, registry: &BTreeMap<String, serde_yaml::Value>) -> Result<JobValidation> {
    let mut validation = JobValidation::new(job_id.to_string());
    let job_path = repo.job_path(job_id);

    // Level 1: Structure validation
    let required_files = vec!["job.yaml", "script.sh"];
    for file in &required_files {
        let file_path = job_path.join(file);
        if !file_path.exists() {
            validation.errors.push(format!("Missing required file: {}", file));
        }
    }

    // Validate script.sh has shebang (required by executor)
    let script_path = job_path.join("script.sh");
    if script_path.exists() {
        if let Err(e) = validate_shebang(&script_path) {
            validation.errors.push(format!("script.sh: {}", e));
        }
    }

    // If job.yaml missing, can't continue
    if !job_path.join("job.yaml").exists() {
        return Ok(validation);
    }

    // Parse job.yaml
    let job_yaml_path = job_path.join("job.yaml");
    let content = fs::read_to_string(&job_yaml_path)
        .context("Failed to read job.yaml")?;

    let data: serde_yaml::Value = match serde_yaml::from_str(&content) {
        Ok(d) => d,
        Err(e) => {
            validation.errors.push(format!("Invalid YAML syntax: {}", e));
            return Ok(validation);
        }
    };

    let job = match data.get("job") {
        Some(j) => j,
        None => {
            validation.errors.push("Missing 'job' section in job.yaml".to_string());
            return Ok(validation);
        }
    };

    // Check required fields
    if job.get("id").is_none() {
        validation.errors.push("Missing job.id field".to_string());
    }
    if job.get("version").is_none() {
        validation.errors.push("Missing job.version field".to_string());
    }

    // Check run section
    if job.get("run").is_none() {
        validation.errors.push("Missing job.run section".to_string());
    } else {
        let run = job.get("run").unwrap();
        if run.get("user").is_none() {
            validation.errors.push("Missing job.run.user field".to_string());
        }
        if run.get("timeout").is_none() {
            validation.errors.push("Missing job.run.timeout field".to_string());
        }
    }

    // Check schedule section
    if job.get("schedule").is_none() {
        validation.errors.push("Missing job.schedule section".to_string());
    } else {
        let schedule = job.get("schedule").unwrap();
        if schedule.get("min_daily").is_none() {
            validation.errors.push("Missing job.schedule.min_daily field".to_string());
        }
        if schedule.get("max_daily").is_none() {
            validation.errors.push("Missing job.schedule.max_daily field".to_string());
        }
    }

    // Level 2: Registry consistency
    // Parse reads (list of register names)
    if let Some(reads) = job.get("reads").and_then(|r| r.as_sequence()) {
        for item in reads {
            if let Some(key_str) = item.as_str() {
                validation.reads.push(key_str.to_string());

                // Check if registry entry exists
                if !registry.contains_key(key_str) {
                    validation.errors.push(format!("Register '{}' (in reads) not found in registry", key_str));
                }
            }
        }
    }

    // Parse writes (list of register names)
    if let Some(writes) = job.get("writes").and_then(|w| w.as_sequence()) {
        for item in writes {
            if let Some(key_str) = item.as_str() {
                validation.writes.push(key_str.to_string());

                // Check if registry entry exists
                if !registry.contains_key(key_str) {
                    validation.errors.push(format!("Register '{}' (in writes) not found in registry", key_str));
                }
            }
        }
    }

    Ok(validation)
}

fn check_output_conflicts(validations: &mut [JobValidation]) {
    let mut writes_map: HashMap<String, Vec<String>> = HashMap::new();

    // Build map of writes -> jobs (single writer rule)
    for validation in validations.iter() {
        for write_reg in &validation.writes {
            writes_map.entry(write_reg.clone())
                .or_insert_with(Vec::new)
                .push(validation.job_id.clone());
        }
    }

    // Check for conflicts (single writer rule violation)
    for (register, jobs) in writes_map {
        if jobs.len() > 1 {
            let error_msg = format!(
                "Single writer violation: '{}' written by multiple jobs: {}",
                register,
                jobs.join(", ")
            );

            // Add error to all conflicting jobs
            for job_id in &jobs {
                if let Some(validation) = validations.iter_mut().find(|v| &v.job_id == job_id) {
                    validation.errors.push(error_msg.clone());
                }
            }
        }
    }
}

fn check_orphaned_entries(registry: &BTreeMap<String, serde_yaml::Value>, validations: &[JobValidation]) -> Vec<String> {
    let mut used_keys = HashSet::new();

    // Collect all used registry keys
    for validation in validations {
        for read_reg in &validation.reads {
            used_keys.insert(read_reg.clone());
        }
        for write_reg in &validation.writes {
            used_keys.insert(write_reg.clone());
        }
    }

    // Find orphaned entries
    let mut orphaned = Vec::new();
    for key in registry.keys() {
        if !used_keys.contains(key) {
            orphaned.push(key.clone());
        }
    }

    orphaned.sort();
    orphaned
}

fn validate_shebang(script_path: &std::path::Path) -> Result<(), String> {
    // Read first line
    let file = fs::File::open(script_path)
        .map_err(|e| format!("Failed to open: {}", e))?;

    let mut reader = BufReader::new(file);
    let mut first_line = String::new();

    reader.read_line(&mut first_line)
        .map_err(|e| format!("Failed to read first line: {}", e))?;

    // Check shebang presence
    if !first_line.starts_with("#!") {
        return Err(
            "Missing shebang line (first line must start with #! followed by interpreter path)"
                .to_string()
        );
    }

    // Check interpreter specified
    let interpreter = first_line[2..].trim();
    if interpreter.is_empty() {
        return Err("Invalid shebang: no interpreter specified".to_string());
    }

    // Common valid shebangs
    let valid_patterns = [
        "/bin/sh", "/bin/bash", "/bin/dash",
        "/usr/bin/env sh", "/usr/bin/env bash",
        "/usr/bin/python", "/usr/bin/env python",
        "/usr/bin/perl", "/usr/bin/env perl",
    ];

    // Warn if unusual shebang (but don't error - executor will handle it)
    let is_common = valid_patterns.iter().any(|p| interpreter.starts_with(p));
    if !is_common {
        // This is just informational - executor validates interpreter existence
        // So we don't fail here, just accept it
    }

    Ok(())
}

fn display_validation(validation: &JobValidation) {
    if validation.is_valid() && validation.warnings.is_empty() {
        println!("{} {}", "✓".green(), validation.job_id.bright_white());
        if !validation.reads.is_empty() {
            println!("  Reads:  {}", validation.reads.join(", ").dimmed());
        }
        if !validation.writes.is_empty() {
            println!("  Writes: {}", validation.writes.join(", ").dimmed());
        }
        println!();
    } else {
        if validation.is_valid() {
            println!("{} {}", "⚠".yellow(), validation.job_id.bright_white());
        } else {
            println!("{} {}", "✗".red(), validation.job_id.bright_white());
        }

        for error in &validation.errors {
            println!("  {} {}", "✗".red(), error.red());
        }

        for warning in &validation.warnings {
            println!("  {} {}", "⚠".yellow(), warning.yellow());
        }

        if !validation.reads.is_empty() {
            println!("  Reads:  {}", validation.reads.join(", ").dimmed());
        }
        if !validation.writes.is_empty() {
            println!("  Writes: {}", validation.writes.join(", ").dimmed());
        }

        println!();
    }
}
