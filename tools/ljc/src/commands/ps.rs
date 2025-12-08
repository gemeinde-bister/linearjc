//! List active job executions from coordinator.
//!
//! Usage:
//!   ljc ps                    # Active jobs only
//!   ljc ps --all              # Include completed
//!   ljc ps --executor foo     # Filter by executor
//!   ljc ps --json             # JSON output

use anyhow::{Context, Result};
use colored::Colorize;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

use crate::config::Config;
use crate::mqtt_client::MqttClient;

/// List active job executions
pub fn run(executor: Option<&str>, all: bool, json_output: bool) -> Result<()> {
    println!("{}", "Listing job executions...".bright_blue());
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
    println!("  {} Sending ps request...", "→".bright_blue());
    let request_id = Uuid::new_v4().to_string();

    let request = json!({
        "request_id": request_id,
        "action": "ps",
        "client_id": config.mqtt.client_id,
        "executor": executor,
        "all": all,
    });

    let request_topic = format!("linearjc/dev/ps/request/{}", config.coordinator.id);
    let response_topic = format!("linearjc/dev/ps/response/{}", config.mqtt.client_id);

    // Subscribe BEFORE publish
    mqtt.subscribe(&response_topic)
        .context("Failed to subscribe to response topic")?;

    mqtt.publish_signed(&request_topic, request)
        .context("Failed to publish ps request")?;

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
        display_jobs(&data)?;
    }

    mqtt.disconnect()?;
    Ok(())
}

fn display_jobs(data: &serde_json::Value) -> Result<()> {
    let jobs = data.get("jobs")
        .and_then(|v| v.as_array())
        .map(|a| a.as_slice())
        .unwrap_or(&[]);

    if jobs.is_empty() {
        println!("{}", "No active jobs".dimmed());
        return Ok(());
    }

    // Header
    println!(
        "{:<36} {:<18} {:<10} {:<14} {:>8}",
        "EXECUTION_ID".bright_white().bold(),
        "JOB_ID".bright_white().bold(),
        "STATE".bright_white().bold(),
        "EXECUTOR".bright_white().bold(),
        "DURATION".bright_white().bold()
    );
    println!("{}", "─".repeat(90));

    for job in jobs {
        let exec_id = job.get("execution_id").and_then(|v| v.as_str()).unwrap_or("?");
        let job_id = job.get("job_id").and_then(|v| v.as_str()).unwrap_or("?");
        let state = job.get("state").and_then(|v| v.as_str()).unwrap_or("?");
        let executor = job.get("executor").and_then(|v| v.as_str()).unwrap_or("?");
        let duration = job.get("duration").and_then(|v| v.as_f64()).unwrap_or(0.0);

        println!(
            "{:<36} {:<18} {:<10} {:<14} {:>7.1}s",
            truncate(exec_id, 36),
            truncate(job_id, 18),
            format_state(state),
            truncate(executor, 14),
            duration
        );
    }

    println!();
    println!("  {} {} job(s)", "Total:".bright_white(), jobs.len());

    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() > max {
        format!("{}...", &s[..max.saturating_sub(3)])
    } else {
        s.to_string()
    }
}

fn format_state(state: &str) -> String {
    match state {
        "queued" => "QUEUED".yellow().to_string(),
        "assigned" => "ASSIGNED".blue().to_string(),
        "downloading" => "DOWNLOAD".cyan().to_string(),
        "ready" => "READY".cyan().to_string(),
        "running" => "RUNNING".bright_blue().bold().to_string(),
        "uploading" => "UPLOAD".cyan().to_string(),
        "completed" => "DONE".green().to_string(),
        "failed" => "FAILED".red().bold().to_string(),
        "timeout" => "TIMEOUT".red().bold().to_string(),
        _ => state.to_string(),
    }
}
