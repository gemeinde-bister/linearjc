//! Execute job immediately on coordinator (bypass scheduler).
//!
//! This command sends a signed MQTT request to the coordinator to execute
//! a job immediately, bypassing the normal scheduling system.
//!
//! Usage:
//!   ljc exec <job-id> [--follow] [--wait]
//!
//! Options:
//!   --follow  Stream real-time progress updates
//!   --wait    Block until completion, exit with job exit code

use anyhow::{Context, Result};
use colored::Colorize;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

use crate::config::Config;
use crate::mqtt_client::MqttClient;
use crate::progress::{print_streaming_footer, print_streaming_header, stream_progress};

/// Execute a job immediately via coordinator
pub fn run(job_id: &str, follow: bool, wait: bool, timeout_secs: u64) -> Result<()> {
    println!("{}", "Executing job immediately...".bright_blue());
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
    println!(
        "    {} MQTT: {}:{}",
        "✓".green(),
        config.mqtt.broker.bright_white(),
        config.mqtt.port
    );
    println!(
        "    {} Client ID: {}",
        "✓".green(),
        config.mqtt.client_id.bright_white()
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

    // 3. Build and send exec request
    println!("  {} Sending exec request...", "→".bright_blue());
    let request_id = Uuid::new_v4().to_string();

    let exec_request = json!({
        "request_id": request_id,
        "action": "exec_job",
        "client_id": config.mqtt.client_id,
        "job_id": job_id,
        "follow": follow || wait,  // Both modes need progress forwarding
    });

    let request_topic = format!("linearjc/dev/exec/request/{}", config.coordinator.id);
    let response_topic = format!("linearjc/dev/exec/response/{}", config.mqtt.client_id);

    // Subscribe to response topic BEFORE sending request
    mqtt.subscribe(&response_topic)
        .context("Failed to subscribe to response topic")?;

    // If following, also subscribe to progress updates
    let progress_topic = if follow || wait {
        let topic = format!("linearjc/dev/progress/{}", config.mqtt.client_id);
        mqtt.subscribe(&topic)
            .context("Failed to subscribe to progress topic")?;
        Some(topic)
    } else {
        None
    };

    mqtt.publish_signed(&request_topic, exec_request)
        .context("Failed to publish exec request")?;

    println!(
        "    {} Request sent (ID: {})",
        "✓".green(),
        request_id[..8].bright_white()
    );
    println!(
        "    {} Job: {}",
        "✓".green(),
        job_id.bright_white()
    );
    println!();

    // 4. Wait for initial response (accepted/rejected)
    println!("  {} Waiting for coordinator response...", "→".bright_blue());

    let timeout = Duration::from_secs(30);
    let response = mqtt
        .wait_for_response(&response_topic, &request_id, timeout)
        .context("Failed to receive response from coordinator")?;

    let status = response
        .get("status")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    match status {
        "accepted" => {
            let job_execution_id = response
                .get("job_execution_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            println!("    {} Execution accepted", "✓".green());
            println!(
                "    {} Execution ID: {}",
                "→".bright_blue(),
                job_execution_id.bright_white()
            );
            println!();

            // If follow or wait, stream progress updates
            if follow || wait {
                print_streaming_header();

                let result = stream_progress(
                    &mqtt,
                    progress_topic.as_deref().unwrap(),
                    job_execution_id,
                    timeout_secs,
                )?;

                print_streaming_footer();

                // Disconnect
                mqtt.disconnect()?;

                // Exit with job's exit code if waiting
                if wait && result.exit_code != 0 {
                    std::process::exit(result.exit_code);
                }
            } else {
                // Just show that job was started
                println!("{}", "═".repeat(60).bright_blue());
                println!(
                    "  {} {}",
                    "✓".green().bold(),
                    "Job execution started!".green().bold()
                );
                println!();
                println!("  To follow progress:");
                println!(
                    "    ljc tail {}",
                    job_execution_id.bright_white()
                );
                println!("{}", "═".repeat(60).bright_blue());
                println!();

                // Disconnect
                mqtt.disconnect()?;
            }
        }
        "rejected" => {
            let error = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");

            println!();
            println!("{}", "═".repeat(60).bright_blue());
            println!(
                "  {} {}",
                "✗".red().bold(),
                "Execution rejected!".red().bold()
            );
            println!();
            println!("  Error: {}", error.red());
            println!("{}", "═".repeat(60).bright_blue());
            println!();

            // Disconnect
            mqtt.disconnect()?;

            anyhow::bail!("Execution rejected: {}", error);
        }
        "error" => {
            let error = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");

            println!();
            println!("{}", "═".repeat(60).bright_blue());
            println!(
                "  {} {}",
                "✗".red().bold(),
                "Execution failed!".red().bold()
            );
            println!();
            println!("  Error: {}", error.red());
            println!("{}", "═".repeat(60).bright_blue());
            println!();

            // Disconnect
            mqtt.disconnect()?;

            anyhow::bail!("Execution error: {}", error);
        }
        _ => {
            println!("    {} Unknown status: {}", "?".yellow(), status);

            // Disconnect
            mqtt.disconnect()?;

            anyhow::bail!("Unknown response status: {}", status);
        }
    }

    Ok(())
}
