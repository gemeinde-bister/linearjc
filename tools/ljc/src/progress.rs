//! Shared progress streaming functionality for ljc commands.
//!
//! This module provides progress streaming utilities used by both:
//! - `ljc exec --follow/--wait` - streaming progress from exec'd jobs
//! - `ljc tail` - attaching to existing job executions
//!
//! The coordinator forwards progress updates to a dev-specific MQTT topic,
//! and this module handles displaying those updates with proper formatting.

use anyhow::{Context, Result};
use colored::Colorize;
use std::time::{Duration, Instant};

use crate::mqtt_client::MqttClient;

/// Result of streaming progress until completion
#[derive(Debug)]
pub struct ProgressResult {
    /// Exit code from the job (0 = success)
    pub exit_code: i32,
    /// Total duration of the streaming session
    pub duration: Duration,
    /// Final state of the job
    pub final_state: String,
}

/// Stream progress updates until job reaches terminal state.
///
/// Listens on the given progress topic and displays state transitions
/// with timestamps. Returns when job completes, fails, or times out.
///
/// # Arguments
/// * `mqtt` - Connected MQTT client
/// * `progress_topic` - Topic to listen for progress updates
/// * `job_execution_id` - Execution ID to filter messages
/// * `timeout_secs` - Maximum time to wait (0 = 1 hour default)
///
/// # Returns
/// `ProgressResult` with exit code, duration, and final state
pub fn stream_progress(
    mqtt: &MqttClient,
    progress_topic: &str,
    job_execution_id: &str,
    timeout_secs: u64,
) -> Result<ProgressResult> {
    let start = Instant::now();
    let timeout = Duration::from_secs(if timeout_secs > 0 { timeout_secs } else { 3600 });
    let mut last_state = String::new();
    let mut exit_code = 0;
    let mut final_state = String::new();

    loop {
        if start.elapsed() > timeout {
            println!();
            println!("  {} Timeout waiting for job completion", "✗".red());
            anyhow::bail!("Timeout waiting for job completion");
        }

        // Try to receive a message (non-blocking with short timeout)
        let result = mqtt.wait_for_progress(progress_topic, Duration::from_millis(500));

        match result {
            Ok(msg) => {
                let state = msg
                    .get("state")
                    .and_then(|v| v.as_str())
                    .unwrap_or("unknown");
                let message = msg
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");
                let exec_id = msg
                    .get("job_execution_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                // Only process messages for our execution
                if !exec_id.is_empty() && exec_id != job_execution_id {
                    continue;
                }

                // Print state transition
                if state != last_state {
                    let state_display = format_state(state);
                    let elapsed = start.elapsed().as_secs();

                    if !message.is_empty() {
                        println!(
                            "  [{:>4}s] {} {}",
                            elapsed,
                            state_display,
                            message.dimmed()
                        );
                    } else {
                        println!("  [{:>4}s] {}", elapsed, state_display);
                    }

                    last_state = state.to_string();
                }

                // Check for terminal states
                match state {
                    "completed" => {
                        println!();
                        let duration = start.elapsed().as_secs_f32();
                        println!(
                            "  {} {} (duration: {:.1}s)",
                            "✓".green().bold(),
                            "Job completed successfully".green().bold(),
                            duration
                        );
                        exit_code = 0;
                        final_state = "completed".to_string();
                        break;
                    }
                    "failed" => {
                        println!();
                        let error = msg
                            .get("error")
                            .and_then(|v| v.as_str())
                            .unwrap_or("Unknown error");
                        let duration = start.elapsed().as_secs_f32();
                        println!(
                            "  {} {} (duration: {:.1}s)",
                            "✗".red().bold(),
                            "Job failed".red().bold(),
                            duration
                        );
                        if !error.is_empty() {
                            println!("  Error: {}", error.red());
                        }
                        exit_code = msg
                            .get("exit_code")
                            .and_then(|v| v.as_i64())
                            .unwrap_or(1) as i32;
                        final_state = "failed".to_string();
                        break;
                    }
                    "timeout" => {
                        println!();
                        let duration = start.elapsed().as_secs_f32();
                        println!(
                            "  {} {} (duration: {:.1}s)",
                            "✗".red().bold(),
                            "Job timed out".red().bold(),
                            duration
                        );
                        exit_code = 124; // Standard timeout exit code
                        final_state = "timeout".to_string();
                        break;
                    }
                    _ => {}
                }
            }
            Err(_) => {
                // No message received within timeout, continue waiting
                continue;
            }
        }
    }

    Ok(ProgressResult {
        exit_code,
        duration: start.elapsed(),
        final_state,
    })
}

/// Format job state for display with colors.
pub fn format_state(state: &str) -> String {
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

/// Print the progress streaming header.
pub fn print_streaming_header() {
    println!("{}", "═".repeat(60).bright_blue());
    println!(
        "  {} {}",
        "→".bright_blue(),
        "Streaming execution progress...".bright_white()
    );
    println!("{}", "═".repeat(60).bright_blue());
    println!();
}

/// Print the progress streaming footer.
pub fn print_streaming_footer() {
    println!("{}", "═".repeat(60).bright_blue());
    println!();
}
