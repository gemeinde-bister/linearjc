use anyhow::{Context, Result};
use colored::Colorize;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

use crate::config::Config;
use crate::mqtt_client::MqttClient;

/// Deploy package to coordinator via MQTT + MinIO
///
/// Workflow:
/// 1. Read package and compute checksum
/// 2. Connect to MQTT
/// 3. Request presigned upload URL from coordinator
/// 4. Upload to MinIO
/// 5. Notify coordinator of completion
/// 6. Wait for install result
pub fn run(package: &Path, _to: &str) -> Result<()> {
    println!("{}", "Deploying package to coordinator...".bright_blue());
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

    // 2. Read package and compute metadata
    println!("  {} Reading package...", "→".bright_blue());
    if !package.exists() {
        anyhow::bail!("Package not found: {}", package.display());
    }

    let package_bytes = fs::read(package)
        .with_context(|| format!("Failed to read package: {}", package.display()))?;

    let package_size = package_bytes.len();

    // Compute SHA-256 checksum
    let mut hasher = Sha256::new();
    hasher.update(&package_bytes);
    let checksum = format!("{:x}", hasher.finalize());

    // Extract job_id from filename (e.g., "backup.pool.ljc" -> "backup.pool")
    let job_id = package
        .file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow::anyhow!("Invalid package filename"))?;

    println!(
        "    {} Package: {}",
        "✓".green(),
        package.file_name().unwrap().to_str().unwrap().bright_white()
    );
    println!(
        "    {} Job ID: {}",
        "✓".green(),
        job_id.bright_white()
    );
    println!(
        "    {} Size: {} bytes",
        "✓".green(),
        package_size.to_string().bright_white()
    );
    println!(
        "    {} Checksum: {}",
        "✓".green(),
        checksum[..16].bright_white()
    );
    println!();

    // 3. Connect to MQTT
    println!("  {} Connecting to MQTT broker...", "→".bright_blue());
    let mqtt = MqttClient::connect(config.clone())
        .context("Failed to connect to MQTT broker")?;

    mqtt.wait_connected(Duration::from_secs(10))
        .context("Failed to establish MQTT connection")?;

    println!("    {} Connected", "✓".green());
    println!();

    // 4. Request presigned upload URL
    println!("  {} Requesting upload URL...", "→".bright_blue());
    let request_id = Uuid::new_v4().to_string();

    let upload_request = json!({
        "request_id": request_id,
        "action": "request_upload_url",
        "client_id": config.mqtt.client_id,
        "job_id": job_id,
        "package_size": package_size,
        "checksum_sha256": checksum,
    });

    let request_topic = format!("linearjc/deploy/request/{}", config.coordinator.id);
    let response_topic = format!("linearjc/deploy/response/{}", config.mqtt.client_id);

    // Subscribe to response topic BEFORE sending request to avoid race condition
    mqtt.subscribe(&response_topic)
        .context("Failed to subscribe to response topic")?;

    mqtt.publish_signed(&request_topic, upload_request)
        .context("Failed to publish upload request")?;

    println!(
        "    {} Request sent (ID: {})",
        "✓".green(),
        request_id[..8].bright_white()
    );

    // 5. Wait for upload URL response
    println!("    {} Waiting for upload URL...", "→".bright_blue());
    let response = mqtt
        .wait_for_response(&response_topic, &request_id, Duration::from_secs(30))
        .context("Failed to receive upload URL from coordinator")?;

    let upload_url = response
        .get("upload_url")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Response missing upload_url field"))?;

    let expires_in = response
        .get("expires_in")
        .and_then(|v| v.as_u64())
        .unwrap_or(600);

    println!(
        "    {} Received upload URL (expires in {}s)",
        "✓".green(),
        expires_in.to_string().bright_white()
    );
    println!();

    // 6. Upload to MinIO
    println!("  {} Uploading to MinIO...", "→".bright_blue());

    let client = reqwest::blocking::Client::new();
    let upload_response = client
        .put(upload_url)
        .header("Content-Type", "application/gzip")
        .header("Content-Length", package_size)
        .body(package_bytes)
        .send()
        .context("Failed to upload package to MinIO")?;

    if !upload_response.status().is_success() {
        anyhow::bail!(
            "MinIO upload failed: HTTP {}",
            upload_response.status()
        );
    }

    println!("    {} Upload complete", "✓".green());
    println!();

    // 7. Notify coordinator of upload completion
    println!("  {} Notifying coordinator...", "→".bright_blue());

    let complete_notification = json!({
        "request_id": request_id,
        "action": "deploy_complete",
        "client_id": config.mqtt.client_id.clone(),
        "job_id": job_id,
        "checksum_sha256": checksum,
    });

    let complete_topic = format!("linearjc/deploy/complete/{}", config.coordinator.id);

    mqtt.publish_signed(&complete_topic, complete_notification)
        .context("Failed to publish completion notification")?;

    println!("    {} Notification sent", "✓".green());

    // 8. Wait for installation result
    println!("    {} Waiting for installation result...", "→".bright_blue());

    let install_result = mqtt
        .wait_for_response(&response_topic, &request_id, Duration::from_secs(60))
        .context("Failed to receive installation result")?;

    let status = install_result
        .get("status")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Response missing status field"))?;

    let installed_job_id = install_result
        .get("job_id")
        .and_then(|v| v.as_str())
        .unwrap_or(job_id);

    let installed_version = install_result
        .get("version")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown");

    println!();
    println!("{}", "═".repeat(60).bright_blue());

    match status {
        "installed" => {
            println!(
                "  {} {}",
                "✓".green().bold(),
                "Deployment successful!".green().bold()
            );
            println!();
            println!("  Job ID:  {}", installed_job_id.bright_white());
            println!("  Version: {}", installed_version.bright_white());
        }
        "failed" => {
            let error_msg = install_result
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("Unknown error");

            println!(
                "  {} {}",
                "✗".red().bold(),
                "Deployment failed!".red().bold()
            );
            println!();
            println!("  Error: {}", error_msg.red());
        }
        _ => {
            println!(
                "  {} {}",
                "?".yellow().bold(),
                "Unknown status".yellow().bold()
            );
            println!();
            println!("  Status: {}", status);
        }
    }

    println!("{}", "═".repeat(60).bright_blue());
    println!();

    // Disconnect
    mqtt.disconnect()?;

    if status != "installed" {
        anyhow::bail!("Deployment failed");
    }

    Ok(())
}
