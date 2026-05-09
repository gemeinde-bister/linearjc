//! Self-update ljc binary from coordinator.
//!
//! Queries the coordinator for the latest tool version and downloads/installs
//! if a newer version is available.
//!
//! Usage:
//!   ljc self-update [--check]
//!
//! Options:
//!   --check   Only check for updates, don't install

use anyhow::{Context, Result, bail};
use colored::Colorize;
use serde_json::json;
use sha2::{Sha256, Digest};
use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

use crate::config::Config;
use crate::mqtt_client::MqttClient;

/// Current version from Cargo.toml
const CURRENT_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Target triple set at build time
#[cfg(all(target_arch = "x86_64", target_os = "linux"))]
const PLATFORM: &str = "x86_64-unknown-linux-musl";

#[cfg(all(target_arch = "aarch64", target_os = "linux"))]
const PLATFORM: &str = "aarch64-unknown-linux-musl";

#[cfg(not(any(
    all(target_arch = "x86_64", target_os = "linux"),
    all(target_arch = "aarch64", target_os = "linux")
)))]
const PLATFORM: &str = "unknown-unknown-unknown";

/// Check for and optionally install ljc updates
pub fn run(check_only: bool) -> Result<()> {
    println!("{}", "Checking for ljc updates...".bright_blue());
    println!();

    // Show current version
    println!(
        "  {} Current version: {}",
        "→".bright_blue(),
        CURRENT_VERSION.bright_white()
    );
    println!(
        "  {} Platform: {}",
        "→".bright_blue(),
        PLATFORM.bright_white()
    );
    println!();

    // Load configuration
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

    // Connect to MQTT
    println!("  {} Connecting to MQTT broker...", "→".bright_blue());
    let mqtt = MqttClient::connect(config.clone())
        .context("Failed to connect to MQTT broker")?;

    mqtt.wait_connected(Duration::from_secs(10))
        .context("Failed to establish MQTT connection")?;

    println!("    {} Connected", "✓".green());
    println!();

    // Build and send version request
    println!("  {} Querying coordinator for latest version...", "→".bright_blue());
    let request_id = Uuid::new_v4().to_string();

    let version_request = json!({
        "request_id": request_id,
        "client_id": config.mqtt.client_id,
        "tool": "ljc",
        "platform": PLATFORM,
        "current_version": CURRENT_VERSION,
    });

    let request_topic = format!("linearjc/tools/version/request/{}", config.coordinator.id);
    let response_topic = format!("linearjc/tools/version/response/{}", config.mqtt.client_id);

    // Subscribe to response topic BEFORE sending request
    mqtt.subscribe(&response_topic)
        .context("Failed to subscribe to response topic")?;

    mqtt.publish_signed(&request_topic, version_request)
        .context("Failed to publish version request")?;

    // Wait for response
    let timeout = Duration::from_secs(30);
    let response = mqtt
        .wait_for_response(&response_topic, &request_id, timeout)
        .context("Failed to receive response from coordinator")?;

    mqtt.disconnect()?;

    // Check for error
    if let Some(error) = response.get("error").and_then(|v| v.as_str()) {
        println!();
        println!("{}", "═".repeat(60).bright_blue());
        println!(
            "  {} {}",
            "✗".red().bold(),
            "Update check failed!".red().bold()
        );
        println!();
        println!("  Error: {}", error.red());
        println!("{}", "═".repeat(60).bright_blue());
        println!();
        bail!("Version check failed: {}", error);
    }

    // Parse response
    let latest_version = response
        .get("version")
        .and_then(|v| v.as_str())
        .context("Response missing version field")?;

    println!(
        "    {} Latest version: {}",
        "✓".green(),
        latest_version.bright_white()
    );
    println!();

    // Compare versions
    match compare_versions(CURRENT_VERSION, latest_version) {
        Ordering::Less => {
            println!(
                "  {} Update available: {} → {}",
                "★".yellow().bold(),
                CURRENT_VERSION.bright_white(),
                latest_version.green().bold()
            );
        }
        Ordering::Equal => {
            println!("{}", "═".repeat(60).bright_blue());
            println!(
                "  {} {}",
                "✓".green().bold(),
                format!("ljc {} is up to date!", CURRENT_VERSION).green().bold()
            );
            println!("{}", "═".repeat(60).bright_blue());
            println!();
            return Ok(());
        }
        Ordering::Greater => {
            println!("{}", "═".repeat(60).bright_blue());
            println!(
                "  {} {}",
                "→".yellow().bold(),
                format!(
                    "ljc {} is newer than coordinator's {} (dev build?)",
                    CURRENT_VERSION, latest_version
                ).yellow()
            );
            println!("{}", "═".repeat(60).bright_blue());
            println!();
            return Ok(());
        }
    }

    if check_only {
        println!();
        println!("{}", "═".repeat(60).bright_blue());
        println!("  Run 'ljc self-update' to install the update");
        println!("{}", "═".repeat(60).bright_blue());
        println!();
        return Ok(());
    }

    println!();

    // Get artifact info
    let artifact = response
        .get("artifact")
        .context("Response missing artifact field")?;

    let download_url = artifact
        .get("uri")
        .and_then(|v| v.as_str())
        .context("Artifact missing uri field")?;

    let expected_checksum = artifact
        .get("checksum_sha256")
        .and_then(|v| v.as_str())
        .context("Artifact missing checksum_sha256 field")?;

    let expected_size = artifact
        .get("size_bytes")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    // Download the binary
    println!(
        "  {} Downloading ljc {}...",
        "→".bright_blue(),
        latest_version.bright_white()
    );

    let temp_path = download_file(download_url, expected_size)?;

    println!("    {} Downloaded", "✓".green());

    // Verify checksum
    println!("  {} Verifying checksum...", "→".bright_blue());

    let actual_checksum = compute_sha256(&temp_path)?;

    if actual_checksum != expected_checksum {
        // Clean up temp file
        let _ = fs::remove_file(&temp_path);
        bail!(
            "Checksum mismatch!\n  Expected: {}\n  Got: {}",
            expected_checksum,
            actual_checksum
        );
    }

    println!("    {} Checksum verified", "✓".green());

    // Make executable (Unix)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temp_path, fs::Permissions::from_mode(0o755))
            .context("Failed to set executable permissions")?;
    }

    // Self-replace
    println!("  {} Installing...", "→".bright_blue());

    self_replace::self_replace(&temp_path)
        .context("Failed to replace binary")?;

    // Clean up temp file (self_replace moves it, but just in case)
    let _ = fs::remove_file(&temp_path);

    println!();
    println!("{}", "═".repeat(60).bright_blue());
    println!(
        "  {} {}",
        "✓".green().bold(),
        format!("Updated ljc {} → {}", CURRENT_VERSION, latest_version).green().bold()
    );
    println!("{}", "═".repeat(60).bright_blue());
    println!();

    Ok(())
}

/// Compare semantic versions (major.minor.patch)
fn compare_versions(current: &str, latest: &str) -> Ordering {
    let parse = |v: &str| -> (u32, u32, u32) {
        let parts: Vec<u32> = v
            .trim_start_matches('v')
            .split('.')
            .filter_map(|s| s.parse().ok())
            .collect();

        (
            parts.first().copied().unwrap_or(0),
            parts.get(1).copied().unwrap_or(0),
            parts.get(2).copied().unwrap_or(0),
        )
    };

    let current = parse(current);
    let latest = parse(latest);

    current.cmp(&latest)
}

/// Download file from URL to temp path
fn download_file(url: &str, expected_size: u64) -> Result<std::path::PathBuf> {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(300))
        .build()
        .context("Failed to create HTTP client")?;

    let response = client
        .get(url)
        .send()
        .context("Failed to download file")?;

    if !response.status().is_success() {
        bail!("Download failed with status: {}", response.status());
    }

    // Create temp file
    let temp_dir = std::env::temp_dir();
    let temp_path = temp_dir.join(format!("ljc-update-{}", uuid::Uuid::new_v4()));

    let bytes = response.bytes().context("Failed to read response body")?;

    // Verify size if expected_size > 0
    if expected_size > 0 && bytes.len() as u64 != expected_size {
        bail!(
            "Size mismatch: expected {} bytes, got {} bytes",
            expected_size,
            bytes.len()
        );
    }

    let mut file = File::create(&temp_path).context("Failed to create temp file")?;
    file.write_all(&bytes).context("Failed to write temp file")?;

    Ok(temp_path)
}

/// Compute SHA256 checksum of file
fn compute_sha256(path: &Path) -> Result<String> {
    let mut file = File::open(path).context("Failed to open file for checksum")?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 8192];

    loop {
        let bytes_read = file.read(&mut buffer).context("Failed to read file")?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(format!("{:x}", hasher.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_versions() {
        assert_eq!(compare_versions("0.1.0", "0.2.0"), Ordering::Less);
        assert_eq!(compare_versions("0.2.0", "0.1.0"), Ordering::Greater);
        assert_eq!(compare_versions("0.1.0", "0.1.0"), Ordering::Equal);
        assert_eq!(compare_versions("1.0.0", "0.9.9"), Ordering::Greater);
        assert_eq!(compare_versions("v0.1.0", "0.1.0"), Ordering::Equal);
    }
}
