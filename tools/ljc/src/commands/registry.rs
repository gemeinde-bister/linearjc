use anyhow::{Context, Result};
use colored::Colorize;
use serde_json::json;
use serde_yaml::{Mapping, Value};
use std::collections::BTreeMap;
use std::time::Duration;
use uuid::Uuid;

use crate::config::Config;
use crate::mqtt_client::MqttClient;
use crate::utils::{Repository, YamlUtils};

/// List all registers in the local registry
pub fn run_list(verbose: bool) -> Result<()> {
    let repo = Repository::find()?;
    let registry = YamlUtils::parse_registry(&repo.registry_file)?;

    if registry.is_empty() {
        println!("{}", "Registry is empty".yellow());
        println!();
        println!("Add registers:");
        println!("  ljc registry add <name> --type fs --path /path/to/data --kind file");
        println!();
        println!("Or sync from coordinator:");
        println!("  ljc sync --from <coordinator>");
        return Ok(());
    }

    println!("{}", "Registry".bright_blue());
    println!("{}", "=".repeat(60).dimmed());
    println!();

    if verbose {
        // Verbose mode: show full registry entries
        for (key, value) in &registry {
            println!("  {}", key.bright_white());

            // Try to parse the entry
            if let Some(mapping) = value.as_mapping() {
                for (k, v) in mapping {
                    let key_str = k.as_str().unwrap_or("?");
                    let val_str = format!("{:?}", v).replace('"', "");
                    println!("    {}: {}", key_str.dimmed(), val_str);
                }
            }
            println!();
        }
    } else {
        // Compact mode: show key, type, and path
        for (key, value) in &registry {
            let entry_type = value
                .get("type")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown");

            let path = value
                .get("path")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string())
                .or_else(|| {
                    // For minio, show bucket/key
                    let bucket = value.get("bucket").and_then(|v| v.as_str())?;
                    let prefix = value.get("prefix").and_then(|v| v.as_str()).unwrap_or("");
                    Some(format!("{}/{}", bucket, prefix))
                })
                .unwrap_or_else(|| "?".to_string());

            println!(
                "  {:30} {:12} {}",
                key.bright_white(),
                entry_type.dimmed(),
                path.dimmed()
            );
        }
    }

    println!();
    println!("Total: {} entries", registry.len());
    println!();
    println!("Commands:");
    println!("  ljc registry add <name> ...   {}", "Add new register".dimmed());
    println!("  ljc registry push             {}", "Push to coordinator".dimmed());
    println!("  ljc registry list --verbose   {}", "Show full details".dimmed());

    Ok(())
}

/// Add a new register to the local registry
pub fn run_add(
    name: &str,
    reg_type: &str,
    path: Option<&str>,
    kind: &str,
    bucket: Option<&str>,
    prefix: Option<&str>,
) -> Result<()> {
    let repo = Repository::find()?;
    let mut registry = YamlUtils::parse_registry(&repo.registry_file)?;

    // Check if register already exists
    if registry.contains_key(name) {
        anyhow::bail!(
            "Register '{}' already exists. Registry entries are immutable.",
            name
        );
    }

    // Build the entry based on type
    let mut entry = Mapping::new();

    match reg_type {
        "fs" => {
            let path = path.ok_or_else(|| anyhow::anyhow!("--path is required for type=fs"))?;

            // Validate kind
            if kind != "file" && kind != "dir" {
                anyhow::bail!("--kind must be 'file' or 'dir' for type=fs");
            }

            entry.insert(Value::String("type".to_string()), Value::String("fs".to_string()));
            entry.insert(Value::String("path".to_string()), Value::String(path.to_string()));
            entry.insert(Value::String("kind".to_string()), Value::String(kind.to_string()));
        }
        "minio" => {
            let bucket = bucket.ok_or_else(|| anyhow::anyhow!("--bucket is required for type=minio"))?;
            let prefix = prefix.unwrap_or("");

            entry.insert(Value::String("type".to_string()), Value::String("minio".to_string()));
            entry.insert(Value::String("bucket".to_string()), Value::String(bucket.to_string()));
            entry.insert(Value::String("prefix".to_string()), Value::String(prefix.to_string()));
        }
        _ => {
            anyhow::bail!("Unknown register type '{}'. Must be 'fs' or 'minio'.", reg_type);
        }
    }

    // Add to registry
    registry.insert(name.to_string(), Value::Mapping(entry.clone()));

    // Write back
    YamlUtils::write_compact(&repo.registry_file, &registry)
        .context("Failed to write registry.yaml")?;

    println!("{} Added register '{}'", "✓".green(), name.bright_white());
    println!();

    // Show the entry
    println!("  {}", name.bright_white());
    for (k, v) in &entry {
        let key_str = k.as_str().unwrap_or("?");
        let val_str = v.as_str().unwrap_or("?");
        println!("    {}: {}", key_str.dimmed(), val_str);
    }
    println!();

    println!("Next: {} to sync with coordinator", "ljc registry push".bright_blue());

    Ok(())
}

/// Push local registry to coordinator
pub fn run_push(to: Option<&str>) -> Result<()> {
    println!("{}", "Pushing registry to coordinator...".bright_blue());
    println!();

    // 1. Load configuration
    println!("  {} Loading configuration...", "→".bright_blue());
    let mut config = Config::load().context(
        "Failed to load configuration.\n\
         Set LINEARJC_SECRET environment variable and ensure .ljcconfig exists.",
    )?;

    // Override MQTT broker with --to argument if provided
    if let Some(host) = to {
        config.mqtt.broker = host.to_string();
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
    println!();

    // 2. Load local registry
    println!("  {} Loading local registry...", "→".bright_blue());
    let repo = Repository::find().context(
        "Not in a LinearJC repository.\n\
         Run 'ljc init' to create one, or cd to a directory with registry.yaml.",
    )?;

    let registry = YamlUtils::parse_registry(&repo.registry_file)
        .context("Failed to parse local registry.yaml")?;

    if registry.is_empty() {
        println!("    {} Registry is empty, nothing to push", "!".yellow());
        return Ok(());
    }

    println!(
        "    {} {} entries to push",
        "✓".green(),
        registry.len().to_string().bright_white()
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

    // 4. Convert registry to JSON format
    let registry_json: serde_json::Map<String, serde_json::Value> = registry
        .iter()
        .map(|(k, v)| (k.clone(), yaml_to_json(v)))
        .collect();

    // 5. Send push request
    println!("  {} Pushing registry...", "→".bright_blue());
    let request_id = Uuid::new_v4().to_string();

    let push_request = json!({
        "request_id": request_id,
        "action": "registry_push",
        "client_id": config.mqtt.client_id,
        "registry": registry_json,
        "entry_count": registry.len(),
    });

    let request_topic = format!("linearjc/registry/request/{}", config.coordinator.id);
    let response_topic = format!("linearjc/registry/response/{}", config.mqtt.client_id);

    // Subscribe to response topic BEFORE sending request
    mqtt.subscribe(&response_topic)
        .context("Failed to subscribe to response topic")?;

    mqtt.publish_signed(&request_topic, push_request)
        .context("Failed to publish push request")?;

    println!(
        "    {} Request sent (ID: {})",
        "✓".green(),
        request_id[..8].bright_white()
    );

    // 6. Wait for response
    println!("    {} Waiting for confirmation...", "→".bright_blue());
    let response = mqtt
        .wait_for_response(&response_topic, &request_id, Duration::from_secs(30))
        .context("Failed to receive response from coordinator")?;

    let status = response
        .get("status")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Response missing status field"))?;

    if status != "success" {
        let error_msg = response
            .get("error")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown error");
        anyhow::bail!("Registry push failed: {}", error_msg);
    }

    let added = response
        .get("added")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let updated = response
        .get("updated")
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    println!("    {} Push accepted", "✓".green());
    println!();

    // 7. Display results
    println!("{}", "═".repeat(60).bright_blue());
    println!(
        "  {} {}",
        "✓".green().bold(),
        "Registry pushed successfully!".green().bold()
    );
    println!();

    if added > 0 {
        println!("  {} {} entries added", "+".green(), added);
    }
    if updated > 0 {
        println!("  {} {} entries updated", "~".yellow(), updated);
    }
    if added == 0 && updated == 0 {
        println!("  {} Registry already up to date", "=".dimmed());
    }

    println!();
    println!("  Total on coordinator: {} entries",
        response.get("total").and_then(|v| v.as_u64()).unwrap_or(registry.len() as u64)
    );
    println!("{}", "═".repeat(60).bright_blue());
    println!();

    // Disconnect
    mqtt.disconnect()?;

    Ok(())
}

/// Convert serde_yaml::Value to serde_json::Value
fn yaml_to_json(yaml: &Value) -> serde_json::Value {
    match yaml {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::Value::Number(i.into())
            } else if let Some(f) = n.as_f64() {
                serde_json::json!(f)
            } else {
                serde_json::Value::Null
            }
        }
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::Sequence(seq) => {
            let json_arr: Vec<serde_json::Value> = seq.iter().map(yaml_to_json).collect();
            serde_json::Value::Array(json_arr)
        }
        Value::Mapping(map) => {
            let json_obj: serde_json::Map<String, serde_json::Value> = map
                .iter()
                .filter_map(|(k, v)| {
                    k.as_str().map(|key| (key.to_string(), yaml_to_json(v)))
                })
                .collect();
            serde_json::Value::Object(json_obj)
        }
        Value::Tagged(_) => serde_json::Value::Null,
    }
}

// Keep old function for backwards compatibility (if called directly)
pub fn run(verbose: bool) -> Result<()> {
    run_list(verbose)
}
