//! Query job scheduling status from coordinator.
//!
//! Usage:
//!   ljc status <job-id>     # Status for specific job
//!   ljc status --all        # Status for all jobs
//!   ljc status --all --json # JSON output

use anyhow::{Context, Result};
use colored::Colorize;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

use crate::config::Config;
use crate::mqtt_client::MqttClient;

/// Query job status from coordinator
pub fn run(job_id: Option<&str>, all: bool, json_output: bool) -> Result<()> {
    // Validate args
    if job_id.is_none() && !all {
        anyhow::bail!("Must specify job_id or use --all");
    }

    println!("{}", "Querying job status...".bright_blue());
    println!();

    // 1. Load configuration
    println!("  {} Loading configuration...", "→".bright_blue());
    let config = Config::load().context(
        "Failed to load configuration.\n\
         Set LINEARJC_SECRET environment variable and ensure .ljcconfig exists.",
    )?;

    println!(
        "    {} Coordinator: {}",
        "✓".green(),
        config.coordinator.id.bright_white()
    );
    println!();

    // 2. Connect to MQTT
    println!("  {} Connecting to MQTT broker...", "→".bright_blue());
    let mqtt = MqttClient::connect(config.clone())
        .context("Failed to connect to MQTT broker")?;

    mqtt.wait_connected(Duration::from_secs(10))
        .context("Failed to establish MQTT connection")?;

    println!("    {} Connected", "✓".green());
    println!();

    // 3. Build and send request
    println!("  {} Sending status request...", "→".bright_blue());
    let request_id = Uuid::new_v4().to_string();

    let request = json!({
        "request_id": request_id,
        "action": "status",
        "client_id": config.mqtt.client_id,
        "job_id": job_id,
        "all": all,
    });

    let request_topic = format!("linearjc/dev/status/request/{}", config.coordinator.id);
    let response_topic = format!("linearjc/dev/status/response/{}", config.mqtt.client_id);

    // Subscribe BEFORE publish
    mqtt.subscribe(&response_topic)
        .context("Failed to subscribe to response topic")?;

    mqtt.publish_signed(&request_topic, request)
        .context("Failed to publish status request")?;

    println!("    {} Request sent", "✓".green());
    println!();

    // 4. Wait for response
    println!("  {} Waiting for response...", "→".bright_blue());
    let timeout = Duration::from_secs(30);
    let response = mqtt
        .wait_for_response(&response_topic, &request_id, timeout)
        .context("Failed to receive response from coordinator")?;

    // 5. Handle response
    let success = response.get("success").and_then(|v| v.as_bool()).unwrap_or(false);

    if !success {
        let error = response.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
        mqtt.disconnect()?;
        anyhow::bail!("Query failed: {}", error);
    }

    let data = response.get("data").cloned().unwrap_or(json!({}));

    // 6. Display results
    println!();
    if json_output {
        println!("{}", serde_json::to_string_pretty(&data)?);
    } else {
        display_status(&data)?;
    }

    mqtt.disconnect()?;
    Ok(())
}

fn display_status(data: &serde_json::Value) -> Result<()> {
    if let Some(jobs) = data.get("jobs") {
        // Multiple jobs (--all)
        let empty_vec = vec![];
        let jobs_arr = jobs.as_array().unwrap_or(&empty_vec);
        println!("{}", "═".repeat(70).bright_blue());
        println!("  {} jobs found", jobs_arr.len());
        println!("{}", "═".repeat(70).bright_blue());
        println!();

        for job in jobs_arr {
            display_single_job(job);
            println!();
        }
    } else {
        // Single job
        display_single_job(data);
    }
    Ok(())
}

fn display_single_job(job: &serde_json::Value) {
    let job_id = job.get("job_id").and_then(|v| v.as_str()).unwrap_or("?");
    let version = job.get("version").and_then(|v| v.as_str()).unwrap_or("?");
    let chain_len = job.get("jobs_in_chain").and_then(|v| v.as_i64()).unwrap_or(1);

    println!("{}", "─".repeat(60).bright_blue());
    println!(
        "  {} {} {}",
        "Job:".bright_white().bold(),
        job_id.bright_white().bold(),
        format!("v{}", version).dimmed()
    );

    if chain_len > 1 {
        println!("  {} {} jobs", "Chain:".bright_white(), chain_len);
    }

    // Schedule
    if let Some(schedule) = job.get("schedule") {
        let min = schedule.get("min_daily").and_then(|v| v.as_i64()).unwrap_or(0);
        let max = schedule.get("max_daily").and_then(|v| v.as_i64()).unwrap_or(0);
        println!("  {} {}-{} executions/day", "Schedule:".bright_white(), min, max);
    }

    // Executions
    let exec_24h = job.get("executions_24h").and_then(|v| v.as_i64()).unwrap_or(0);
    println!("  {} {} in last 24h", "Executions:".bright_white(), exec_24h);

    // Last execution
    if let Some(last) = job.get("last_execution").and_then(|v| v.as_f64()) {
        println!("  {} {}", "Last run:".bright_white(), format_timestamp(last));
    } else {
        println!("  {} {}", "Last run:".bright_white(), "never".dimmed());
    }

    // Next execution
    if let Some(next) = job.get("next_execution").and_then(|v| v.as_f64()) {
        println!("  {} {}", "Next run:".bright_white(), format_timestamp(next));
    }

    // Active execution
    if let Some(active) = job.get("active_execution") {
        if !active.is_null() {
            println!();
            println!("  {} Active Execution:", "→".bright_yellow());
            let exec_id = active.get("execution_id").and_then(|v| v.as_str()).unwrap_or("?");
            let state = active.get("state").and_then(|v| v.as_str()).unwrap_or("?");
            let executor = active.get("executor").and_then(|v| v.as_str()).unwrap_or("?");
            let duration = active.get("duration").and_then(|v| v.as_f64()).unwrap_or(0.0);

            println!("    ID: {}", exec_id.bright_white());
            println!("    State: {}", format_state(state));
            println!("    Executor: {}", executor);
            println!("    Duration: {:.1}s", duration);

            if let Some(timeout_rem) = active.get("timeout_remaining").and_then(|v| v.as_f64()) {
                println!("    Timeout in: {:.0}s", timeout_rem);
            }
        }
    }
}

fn format_timestamp(ts: f64) -> String {
    use chrono::{DateTime, Utc};
    DateTime::<Utc>::from_timestamp(ts as i64, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| "invalid".to_string())
}

fn format_state(state: &str) -> String {
    match state {
        "queued" => "QUEUED".yellow().to_string(),
        "assigned" => "ASSIGNED".blue().to_string(),
        "downloading" => "DOWNLOADING".cyan().to_string(),
        "ready" => "READY".cyan().to_string(),
        "running" => "RUNNING".bright_blue().bold().to_string(),
        "uploading" => "UPLOADING".cyan().to_string(),
        "completed" => "COMPLETED".green().bold().to_string(),
        "failed" => "FAILED".red().bold().to_string(),
        "timeout" => "TIMEOUT".red().bold().to_string(),
        _ => state.to_string(),
    }
}
