//! Show job execution history from coordinator.
//!
//! Usage:
//!   ljc logs <job-id>            # Last 10 executions
//!   ljc logs <job-id> --last 20  # Last 20 executions
//!   ljc logs <job-id> --json     # JSON output

use anyhow::{Context, Result};
use colored::Colorize;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

use crate::config::Config;
use crate::mqtt_client::MqttClient;

/// Show job execution history
pub fn run(job_id: &str, last: u32, _failed: bool, json_output: bool) -> Result<()> {
    println!("{}", "Querying execution history...".bright_blue());
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
    println!("  {} Sending logs request...", "→".bright_blue());
    let request_id = Uuid::new_v4().to_string();

    let request = json!({
        "request_id": request_id,
        "action": "logs",
        "client_id": config.mqtt.client_id,
        "job_id": job_id,
        "last": last,
        "failed": _failed,
    });

    let request_topic = format!("linearjc/dev/logs/request/{}", config.coordinator.id);
    let response_topic = format!("linearjc/dev/logs/response/{}", config.mqtt.client_id);

    // Subscribe BEFORE publish
    mqtt.subscribe(&response_topic)
        .context("Failed to subscribe to response topic")?;

    mqtt.publish_signed(&request_topic, request)
        .context("Failed to publish logs request")?;

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
        display_logs(&data)?;
    }

    mqtt.disconnect()?;
    Ok(())
}

fn display_logs(data: &serde_json::Value) -> Result<()> {
    let job_id = data.get("job_id").and_then(|v| v.as_str()).unwrap_or("?");
    let total_24h = data.get("total_24h").and_then(|v| v.as_i64()).unwrap_or(0);

    println!("{}", "═".repeat(50).bright_blue());
    println!("  {} {}", "Job:".bright_white().bold(), job_id.bright_white());
    println!("  {} {}", "Executions (24h):".bright_white(), total_24h);
    println!("{}", "═".repeat(50).bright_blue());
    println!();

    let executions = data.get("executions")
        .and_then(|v| v.as_array())
        .map(|a| a.as_slice())
        .unwrap_or(&[]);

    if executions.is_empty() {
        println!("{}", "No execution history".dimmed());
        return Ok(());
    }

    // Header
    println!(
        "{:<25} {:>12}",
        "TIMESTAMP".bright_white().bold(),
        "AGE".bright_white().bold()
    );
    println!("{}", "─".repeat(40));

    for exec in executions {
        let timestamp = exec.get("timestamp").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let age_hours = exec.get("age_hours").and_then(|v| v.as_f64()).unwrap_or(0.0);

        println!(
            "{:<25} {:>11.1}h",
            format_timestamp(timestamp),
            age_hours
        );
    }

    println!();
    println!("  {} {} execution(s) shown", "Total:".bright_white(), executions.len());

    Ok(())
}

fn format_timestamp(ts: f64) -> String {
    use chrono::{DateTime, Utc};
    DateTime::<Utc>::from_timestamp(ts as i64, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_else(|| "invalid".to_string())
}
