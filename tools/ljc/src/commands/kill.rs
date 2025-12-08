//! Cancel a running job execution.
//!
//! Usage:
//!   ljc kill <execution-id>          # Send SIGTERM
//!   ljc kill <execution-id> --force  # Send SIGKILL
//!   ljc kill <execution-id> --wait   # Wait for termination

use anyhow::{Context, Result};
use colored::Colorize;
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

use crate::config::Config;
use crate::mqtt_client::MqttClient;
use crate::progress::{print_streaming_footer, print_streaming_header, stream_progress};

/// Cancel a running job
pub fn run(execution_id: &str, force: bool, wait: bool) -> Result<()> {
    println!("{}", "Sending kill signal...".bright_blue());
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
    println!("  {} Sending kill request...", "→".bright_blue());
    let request_id = Uuid::new_v4().to_string();

    let request = json!({
        "request_id": request_id,
        "action": "kill",
        "client_id": config.mqtt.client_id,
        "execution_id": execution_id,
        "force": force,
    });

    let request_topic = format!("linearjc/dev/kill/request/{}", config.coordinator.id);
    let response_topic = format!("linearjc/dev/kill/response/{}", config.mqtt.client_id);

    // Subscribe to response topic BEFORE publish
    mqtt.subscribe(&response_topic)
        .context("Failed to subscribe to response topic")?;

    // If waiting, also subscribe to progress updates
    let progress_topic = if wait {
        let topic = format!("linearjc/dev/progress/{}", config.mqtt.client_id);
        mqtt.subscribe(&topic)
            .context("Failed to subscribe to progress topic")?;
        Some(topic)
    } else {
        None
    };

    mqtt.publish_signed(&request_topic, request)
        .context("Failed to publish kill request")?;

    println!(
        "    {} Signal: {}",
        "→".bright_blue(),
        if force { "SIGKILL".red().bold() } else { "SIGTERM".yellow() }
    );
    println!("    {} Target: {}", "→".bright_blue(), execution_id.bright_white());
    println!();

    // 4. Wait for response
    println!("  {} Waiting for coordinator response...", "→".bright_blue());
    let timeout = Duration::from_secs(30);
    let response = mqtt
        .wait_for_response(&response_topic, &request_id, timeout)
        .context("Failed to receive response from coordinator")?;

    // 5. Handle response
    let success = response.get("success").and_then(|v| v.as_bool()).unwrap_or(false);

    if !success {
        let error = response.get("error").and_then(|v| v.as_str()).unwrap_or("Unknown error");
        println!();
        println!("{}", "═".repeat(60).bright_blue());
        println!(
            "  {} {}",
            "✗".red().bold(),
            "Kill failed!".red().bold()
        );
        println!();
        println!("  Error: {}", error.red());
        println!("{}", "═".repeat(60).bright_blue());

        mqtt.disconnect()?;
        anyhow::bail!("Kill failed: {}", error);
    }

    let data = response.get("data").cloned().unwrap_or(json!({}));
    let message = data.get("message").and_then(|v| v.as_str()).unwrap_or("Signal sent");

    println!("    {} {}", "✓".green(), message.green());
    println!();

    // 6. Wait for termination if requested
    if wait {
        println!("{}", "═".repeat(60).bright_blue());
        println!(
            "  {} {}",
            "→".bright_blue(),
            "Waiting for job termination...".bright_white()
        );
        println!("{}", "═".repeat(60).bright_blue());
        println!();

        print_streaming_header();

        let result = stream_progress(
            &mqtt,
            progress_topic.as_deref().unwrap(),
            execution_id,
            3600, // 1 hour timeout
        )?;

        print_streaming_footer();

        // Exit with job's exit code
        if result.exit_code != 0 {
            mqtt.disconnect()?;
            std::process::exit(result.exit_code);
        }
    } else {
        println!("{}", "═".repeat(60).bright_blue());
        println!(
            "  {} {}",
            "✓".green().bold(),
            "Kill signal sent successfully!".green().bold()
        );
        println!();
        println!(
            "  To wait for termination: ljc kill {} --wait",
            execution_id.bright_white()
        );
        println!("{}", "═".repeat(60).bright_blue());
    }

    mqtt.disconnect()?;
    Ok(())
}
