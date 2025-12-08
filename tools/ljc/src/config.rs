//! Configuration management for LinearJC developer tool.
//!
//! Loads configuration from:
//! 1. .ljcconfig file (YAML) in repository root or home directory
//! 2. Environment variables (override file config)
//!
//! Configuration structure:
//! ```yaml
//! mqtt:
//!   broker: "192.0.2.1"  # RFC 5737 example - change to your coordinator IP
//!   port: 1883
//!   client_id: "developer-alice"
//!
//! coordinator:
//!   id: "linearjc-coordinator-01"
//!
//! auth:
//!   shared_secret: "${LINEARJC_SECRET}"  # From environment
//! ```

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};

/// MQTT configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct MqttConfig {
    /// MQTT broker hostname or IP
    #[serde(default = "default_broker")]
    pub broker: String,

    /// MQTT broker port
    #[serde(default = "default_port")]
    pub port: u16,

    /// Client ID for this developer
    #[serde(default = "default_client_id")]
    pub client_id: String,

    /// Keepalive interval in seconds
    #[serde(default = "default_keepalive")]
    pub keepalive: u64,
}

fn default_broker() -> String {
    "localhost".to_string()
}

fn default_port() -> u16 {
    1883
}

fn default_client_id() -> String {
    env::var("USER").unwrap_or_else(|_| "developer".to_string())
}

fn default_keepalive() -> u64 {
    60
}

impl Default for MqttConfig {
    fn default() -> Self {
        Self {
            broker: default_broker(),
            port: default_port(),
            client_id: default_client_id(),
            keepalive: default_keepalive(),
        }
    }
}

/// Coordinator configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CoordinatorConfig {
    /// Coordinator ID (for MQTT topics)
    #[serde(default = "default_coordinator_id")]
    pub id: String,
}

fn default_coordinator_id() -> String {
    "linearjc-coordinator".to_string()
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            id: default_coordinator_id(),
        }
    }
}

/// Authentication configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AuthConfig {
    /// Shared secret for HMAC signing (usually from environment)
    pub shared_secret: String,
}

/// Top-level configuration
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub mqtt: MqttConfig,

    #[serde(default)]
    pub coordinator: CoordinatorConfig,

    pub auth: AuthConfig,
}

impl Config {
    /// Load configuration from file and environment variables.
    ///
    /// Search order for .ljcconfig:
    /// 1. Current directory
    /// 2. Repository root (if in a repo)
    /// 3. Home directory (~/.ljcconfig)
    ///
    /// Environment variables override file settings:
    /// - LINEARJC_SECRET (required) - Shared secret for HMAC
    /// - MQTT_BROKER - MQTT broker hostname
    /// - MQTT_PORT - MQTT broker port
    /// - MQTT_CLIENT_ID - Client ID
    /// - COORDINATOR_ID - Coordinator ID
    ///
    /// # Errors
    /// Returns error if LINEARJC_SECRET is not set or config file is invalid
    pub fn load() -> Result<Self> {
        // Try to load from config file
        let config_file = Self::find_config_file();
        let mut config = if let Some(path) = config_file {
            Self::load_from_file(&path)?
        } else {
            // No config file found, use defaults
            Config {
                mqtt: MqttConfig::default(),
                coordinator: CoordinatorConfig::default(),
                auth: AuthConfig {
                    shared_secret: String::new(), // Will be set from env
                },
            }
        };

        // Override from environment variables
        config.apply_env_overrides()?;

        // Validate required fields
        if config.auth.shared_secret.is_empty() {
            anyhow::bail!(
                "LINEARJC_SECRET environment variable is required\n\
                 Set it to the shared secret for authenticating with the coordinator"
            );
        }

        Ok(config)
    }

    /// Find .ljcconfig file in standard locations
    fn find_config_file() -> Option<PathBuf> {
        // 1. Current directory
        let cwd_config = PathBuf::from(".ljcconfig");
        if cwd_config.exists() {
            return Some(cwd_config);
        }

        // 2. Repository root (look for .git or jobs/ directory)
        if let Some(repo_root) = Self::find_repo_root() {
            let repo_config = repo_root.join(".ljcconfig");
            if repo_config.exists() {
                return Some(repo_config);
            }
        }

        // 3. Home directory
        if let Ok(home) = env::var("HOME") {
            let home_config = PathBuf::from(home).join(".ljcconfig");
            if home_config.exists() {
                return Some(home_config);
            }
        }

        None
    }

    /// Find repository root by looking for .git or jobs/ directory
    fn find_repo_root() -> Option<PathBuf> {
        let mut current = env::current_dir().ok()?;

        loop {
            // Check for .git directory
            if current.join(".git").exists() {
                return Some(current);
            }

            // Check for jobs/ directory (LinearJC repo marker)
            if current.join("jobs").exists() {
                return Some(current);
            }

            // Move up one directory
            if !current.pop() {
                break;
            }
        }

        None
    }

    /// Load configuration from YAML file
    fn load_from_file(path: &Path) -> Result<Self> {
        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read config file: {}", path.display()))?;

        serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse config file: {}", path.display()))
    }

    /// Apply environment variable overrides
    fn apply_env_overrides(&mut self) -> Result<()> {
        // LINEARJC_SECRET (required)
        if let Ok(secret) = env::var("LINEARJC_SECRET") {
            self.auth.shared_secret = secret;
        }

        // MQTT_BROKER
        if let Ok(broker) = env::var("MQTT_BROKER") {
            self.mqtt.broker = broker;
        }

        // MQTT_PORT
        if let Ok(port_str) = env::var("MQTT_PORT") {
            self.mqtt.port = port_str
                .parse()
                .context("Invalid MQTT_PORT value (must be a number)")?;
        }

        // MQTT_CLIENT_ID
        if let Ok(client_id) = env::var("MQTT_CLIENT_ID") {
            self.mqtt.client_id = client_id;
        }

        // COORDINATOR_ID
        if let Ok(coordinator_id) = env::var("COORDINATOR_ID") {
            self.coordinator.id = coordinator_id;
        }

        Ok(())
    }

    /// Create a sample .ljcconfig file
    pub fn create_sample(path: &Path) -> Result<()> {
        let sample = Self {
            mqtt: MqttConfig {
                broker: "192.0.2.1".to_string(),  // RFC 5737 - user must change
                port: 1883,
                client_id: default_client_id(),
                keepalive: 60,
            },
            coordinator: CoordinatorConfig {
                id: "linearjc-coordinator-01".to_string(),
            },
            auth: AuthConfig {
                shared_secret: "${LINEARJC_SECRET}".to_string(),
            },
        };

        let yaml = serde_yaml::to_string(&sample)
            .context("Failed to serialize sample config")?;

        // Add comments
        let commented = format!(
            "# LinearJC Developer Tool Configuration\n\
             #\n\
             # This file configures the ljc tool for connecting to the coordinator.\n\
             # Environment variables can override these settings.\n\
             #\n\
             # Required environment variable:\n\
             #   LINEARJC_SECRET - Shared secret for HMAC authentication\n\
             #\n\
             # Optional environment variables:\n\
             #   MQTT_BROKER - Override MQTT broker hostname\n\
             #   MQTT_PORT - Override MQTT port\n\
             #   MQTT_CLIENT_ID - Override client ID\n\
             #   COORDINATOR_ID - Override coordinator ID\n\
             #\n\
             {}\n",
            yaml
        );

        fs::write(path, commented)
            .with_context(|| format!("Failed to write config file: {}", path.display()))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_from_yaml() {
        let yaml = r#"
mqtt:
  broker: "test-broker"
  port: 1234
  client_id: "test-client"

coordinator:
  id: "test-coordinator"

auth:
  shared_secret: "test-secret"
"#;

        let mut temp = NamedTempFile::new().unwrap();
        temp.write_all(yaml.as_bytes()).unwrap();
        temp.flush().unwrap();

        let config = Config::load_from_file(temp.path()).unwrap();

        assert_eq!(config.mqtt.broker, "test-broker");
        assert_eq!(config.mqtt.port, 1234);
        assert_eq!(config.mqtt.client_id, "test-client");
        assert_eq!(config.coordinator.id, "test-coordinator");
        assert_eq!(config.auth.shared_secret, "test-secret");
    }

    #[test]
    fn test_defaults() {
        let config = MqttConfig::default();
        assert_eq!(config.broker, "localhost");
        assert_eq!(config.port, 1883);
        assert_eq!(config.keepalive, 60);

        let coordinator = CoordinatorConfig::default();
        assert_eq!(coordinator.id, "linearjc-coordinator");
    }

    #[test]
    fn test_env_overrides() {
        // SAFETY: Tests run sequentially, no concurrent env access
        unsafe {
            env::set_var("LINEARJC_SECRET", "env-secret");
            env::set_var("MQTT_BROKER", "env-broker");
            env::set_var("MQTT_PORT", "9999");
        }

        let mut config = Config {
            mqtt: MqttConfig::default(),
            coordinator: CoordinatorConfig::default(),
            auth: AuthConfig {
                shared_secret: "file-secret".to_string(),
            },
        };

        config.apply_env_overrides().unwrap();

        assert_eq!(config.auth.shared_secret, "env-secret");
        assert_eq!(config.mqtt.broker, "env-broker");
        assert_eq!(config.mqtt.port, 9999);

        // Clean up
        // SAFETY: Tests run sequentially, no concurrent env access
        unsafe {
            env::remove_var("LINEARJC_SECRET");
            env::remove_var("MQTT_BROKER");
            env::remove_var("MQTT_PORT");
        }
    }

    #[test]
    fn test_create_sample() {
        let temp = NamedTempFile::new().unwrap();
        Config::create_sample(temp.path()).unwrap();

        let content = fs::read_to_string(temp.path()).unwrap();
        assert!(content.contains("LinearJC Developer Tool Configuration"));
        assert!(content.contains("LINEARJC_SECRET"));
        assert!(content.contains("mqtt:"));
        assert!(content.contains("coordinator:"));
    }
}
