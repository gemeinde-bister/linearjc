//! Follow job execution progress in real-time.
//!
//! Attaches to an existing or active job execution and streams progress
//! updates until the job reaches a terminal state.
//!
//! Usage:
//!   ljc tail <job-id|execution-id> [--timeout <seconds>]
//!
//! Arguments:
//!   <id>  Job ID (attaches to active execution) or execution ID (attaches directly)
//!
//! Options:
//!   --timeout  Maximum time to wait for completion (default: 1 hour)

use anyhow::{Context, Result};
use colored::Colorize;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

use crate::config::Config;
use crate::mqtt_client::MqttClient;
use crate::progress::{format_state, print_streaming_footer, print_streaming_header, stream_progress};

/// Tail (follow) a job execution
pub fn run(id: &str, timeout_secs: u64) -> Result<()> {
    println!("{}", "Tailing job execution...".bright_blue());
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

    // 3. Build and send tail request
    println!("  {} Requesting tail attachment...", "→".bright_blue());
    let request_id = Uuid::new_v4().to_string();

    // Detect if this is an execution ID (contains timestamp pattern) or job ID
    // Execution IDs look like: "job.id-20251118-143022-abc123def"
    // Job IDs look like: "job.id" or "job.id.sub"
    let is_execution_id = id.contains('-') && id.chars().filter(|c| *c == '-').count() >= 3;

    let tail_request = json!({
        "request_id": request_id,
        "action": "tail",
        "client_id": config.mqtt.client_id,
        "job_id": if is_execution_id { None::<&str> } else { Some(id) },
        "execution_id": if is_execution_id { Some(id) } else { None::<&str> },
    });

    let request_topic = format!("linearjc/dev/tail/request/{}", config.coordinator.id);
    let response_topic = format!("linearjc/dev/tail/response/{}", config.mqtt.client_id);
    let progress_topic = format!("linearjc/dev/progress/{}", config.mqtt.client_id);

    // Subscribe to response and progress topics BEFORE sending request
    mqtt.subscribe(&response_topic)
        .context("Failed to subscribe to response topic")?;
    mqtt.subscribe(&progress_topic)
        .context("Failed to subscribe to progress topic")?;

    mqtt.publish_signed(&request_topic, tail_request)
        .context("Failed to publish tail request")?;

    if is_execution_id {
        println!(
            "    {} Execution ID: {}",
            "→".bright_blue(),
            id.bright_white()
        );
    } else {
        println!(
            "    {} Job ID: {} (finding active execution)",
            "→".bright_blue(),
            id.bright_white()
        );
    }
    println!();

    // 4. Wait for response (attached/rejected)
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
        "attached" => {
            let job_execution_id = response
                .get("job_execution_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let job_id = response
                .get("job_id")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let current_state = response
                .get("current_state")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");
            let executor_id = response
                .get("executor_id")
                .and_then(|v| v.as_str());

            println!("    {} Attached to execution", "✓".green());
            println!(
                "    {} Job: {}",
                "→".bright_blue(),
                job_id.bright_white()
            );
            println!(
                "    {} Execution: {}",
                "→".bright_blue(),
                job_execution_id.bright_white()
            );
            if let Some(executor) = executor_id {
                println!(
                    "    {} Executor: {}",
                    "→".bright_blue(),
                    executor.bright_white()
                );
            }
            println!(
                "    {} Current state: {}",
                "→".bright_blue(),
                format_state(current_state)
            );
            println!();

            // Check if job is already in terminal state
            if matches!(current_state, "completed" | "failed" | "timeout") {
                println!("{}", "═".repeat(60).bright_blue());
                println!(
                    "  {} Job already in terminal state: {}",
                    "ℹ".blue(),
                    format_state(current_state)
                );
                println!("{}", "═".repeat(60).bright_blue());
                println!();

                mqtt.disconnect()?;
                return Ok(());
            }

            // Stream progress updates
            print_streaming_header();

            let result = stream_progress(
                &mqtt,
                &progress_topic,
                job_execution_id,
                timeout_secs,
            )?;

            print_streaming_footer();

            // Disconnect
            mqtt.disconnect()?;

            // Exit with job's exit code
            if result.exit_code != 0 {
                std::process::exit(result.exit_code);
            }
        }
        "not_found" => {
            let error = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("No active execution found");

            println!();
            println!("{}", "═".repeat(60).bright_blue());
            println!(
                "  {} {}",
                "✗".red().bold(),
                "No execution found!".red().bold()
            );
            println!();
            println!("  {}", error.yellow());
            println!();
            println!("  Suggestions:");
            println!("    - Use {} to execute the job first", "ljc exec <job-id>".bright_white());
            println!("    - Check if the execution ID is correct");
            println!("{}", "═".repeat(60).bright_blue());
            println!();

            mqtt.disconnect()?;
            anyhow::bail!("No execution found: {}", error);
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
                "Tail failed!".red().bold()
            );
            println!();
            println!("  Error: {}", error.red());
            println!("{}", "═".repeat(60).bright_blue());
            println!();

            mqtt.disconnect()?;
            anyhow::bail!("Tail error: {}", error);
        }
        _ => {
            println!("    {} Unknown status: {}", "?".yellow(), status);

            mqtt.disconnect()?;
            anyhow::bail!("Unknown response status: {}", status);
        }
    }

    Ok(())
}
