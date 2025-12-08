use anyhow::{Context, Result};
use colored::Colorize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::time::Duration;
use uuid::Uuid;

use crate::config::Config;
use crate::mqtt_client::MqttClient;
use crate::utils::{Repository, YamlUtils};

/// Sync registry from coordinator via MQTT
///
/// Workflow:
/// 1. Load local configuration
/// 2. Connect to MQTT broker
/// 3. Request registry from coordinator
/// 4. Compare with local registry
/// 5. Update local registry.yaml
/// 6. Display diff
pub fn run(from: &str) -> Result<()> {
    println!("{}", "Syncing registry from coordinator...".bright_blue());
    println!();

    // 1. Load configuration
    println!("  {} Loading configuration...", "→".bright_blue());
    let mut config = Config::load().context(
        "Failed to load configuration.\n\
         Set LINEARJC_SECRET environment variable and ensure .ljcconfig exists.",
    )?;

    // Override MQTT broker with --from argument if provided
    if !from.is_empty() && from != "default" {
        config.mqtt.broker = from.to_string();
    }

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

    // 2. Find repository and load local registry
    println!("  {} Loading local registry...", "→".bright_blue());
    let repo = Repository::find().context(
        "Not in a LinearJC repository.\n\
         Run 'ljc init' to create one, or cd to a directory with registry.yaml.",
    )?;

    let old_registry = YamlUtils::parse_registry(&repo.registry_file)
        .context("Failed to parse local registry.yaml")?;

    println!(
        "    {} Local entries: {}",
        "✓".green(),
        old_registry.len().to_string().bright_white()
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

    // 4. Request registry from coordinator
    println!("  {} Requesting registry...", "→".bright_blue());
    let request_id = Uuid::new_v4().to_string();

    let sync_request = json!({
        "request_id": request_id,
        "action": "registry_sync",
        "client_id": config.mqtt.client_id,
    });

    let request_topic = format!("linearjc/registry/request/{}", config.coordinator.id);
    let response_topic = format!("linearjc/registry/response/{}", config.mqtt.client_id);

    // Subscribe to response topic BEFORE sending request to avoid race condition
    mqtt.subscribe(&response_topic)
        .context("Failed to subscribe to response topic")?;

    mqtt.publish_signed(&request_topic, sync_request)
        .context("Failed to publish sync request")?;

    println!(
        "    {} Request sent (ID: {})",
        "✓".green(),
        request_id[..8].bright_white()
    );

    // 5. Wait for response
    println!("    {} Waiting for registry data...", "→".bright_blue());
    let response = mqtt
        .wait_for_response(&response_topic, &request_id, Duration::from_secs(30))
        .context("Failed to receive registry from coordinator")?;

    let status = response
        .get("status")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Response missing status field"))?;

    if status != "success" {
        let error_msg = response
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown error");
        anyhow::bail!("Registry sync failed: {}", error_msg);
    }

    let registry_data = response
        .get("registry")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow::anyhow!("Response missing registry field"))?;

    let entry_count = response
        .get("entry_count")
        .and_then(|v| v.as_u64())
        .unwrap_or(registry_data.len() as u64);

    println!(
        "    {} Received {} entries",
        "✓".green(),
        entry_count.to_string().bright_white()
    );
    println!();

    // 6. Convert response to BTreeMap<String, Value> for YAML writing
    let new_registry: BTreeMap<String, serde_yaml::Value> = registry_data
        .iter()
        .map(|(k, v)| {
            let yaml_value = json_to_yaml(v);
            (k.clone(), yaml_value)
        })
        .collect();

    // 7. Compute diff
    let (added, removed, changed) = compute_diff(&old_registry, &new_registry);

    // 8. Update local registry
    println!("  {} Updating local registry...", "→".bright_blue());
    YamlUtils::write_compact(&repo.registry_file, &new_registry)
        .context("Failed to write registry.yaml")?;

    println!(
        "    {} Written to {}",
        "✓".green(),
        repo.registry_file.display()
    );
    println!();

    // 9. Display results
    println!("{}", "═".repeat(60).bright_blue());

    if added.is_empty() && removed.is_empty() && changed.is_empty() {
        println!(
            "  {} {}",
            "✓".green().bold(),
            "Registry is up to date".green()
        );
    } else {
        println!(
            "  {} {}",
            "✓".green().bold(),
            "Registry synced successfully!".green().bold()
        );
        println!();

        if !added.is_empty() {
            println!("  {} {} new entries:", "+".green().bold(), added.len());
            for key in &added {
                println!("    {} {}", "+".green(), key.bright_white());
            }
        }

        if !changed.is_empty() {
            println!("  {} {} changed entries:", "~".yellow().bold(), changed.len());
            for key in &changed {
                println!("    {} {}", "~".yellow(), key.bright_white());
            }
        }

        if !removed.is_empty() {
            println!("  {} {} removed entries:", "-".red().bold(), removed.len());
            for key in &removed {
                println!("    {} {}", "-".red(), key.bright_white());
            }
        }
    }

    println!();
    println!(
        "  Total: {} entries",
        new_registry.len().to_string().bright_white()
    );
    println!("{}", "═".repeat(60).bright_blue());
    println!();

    // Disconnect
    mqtt.disconnect()?;

    Ok(())
}

/// Convert serde_json::Value to serde_yaml::Value
fn json_to_yaml(json: &Value) -> serde_yaml::Value {
    match json {
        Value::Null => serde_yaml::Value::Null,
        Value::Bool(b) => serde_yaml::Value::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_yaml::Value::Number(i.into())
            } else if let Some(f) = n.as_f64() {
                serde_yaml::Value::Number(serde_yaml::Number::from(f))
            } else {
                serde_yaml::Value::Null
            }
        }
        Value::String(s) => serde_yaml::Value::String(s.clone()),
        Value::Array(arr) => {
            let yaml_arr: Vec<serde_yaml::Value> = arr.iter().map(json_to_yaml).collect();
            serde_yaml::Value::Sequence(yaml_arr)
        }
        Value::Object(obj) => {
            let yaml_map: serde_yaml::Mapping = obj
                .iter()
                .map(|(k, v)| (serde_yaml::Value::String(k.clone()), json_to_yaml(v)))
                .collect();
            serde_yaml::Value::Mapping(yaml_map)
        }
    }
}

/// Compute diff between old and new registries
fn compute_diff(
    old: &BTreeMap<String, serde_yaml::Value>,
    new: &BTreeMap<String, serde_yaml::Value>,
) -> (Vec<String>, Vec<String>, Vec<String>) {
    let mut added = Vec::new();
    let mut removed = Vec::new();
    let mut changed = Vec::new();

    // Find added and changed entries
    for key in new.keys() {
        if !old.contains_key(key) {
            added.push(key.clone());
        } else if old.get(key) != new.get(key) {
            changed.push(key.clone());
        }
    }

    // Find removed entries
    for key in old.keys() {
        if !new.contains_key(key) {
            removed.push(key.clone());
        }
    }

    (added, removed, changed)
}
