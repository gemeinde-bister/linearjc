//! MQTT client for LinearJC developer tool.
//!
//! Handles MQTT communication with the coordinator:
//! - Publishing signed requests
//! - Subscribing to responses
//! - Request/response correlation
//! - Timeout handling
//!
//! Based on the coordinator's MQTT client pattern but simplified for developer use.

use anyhow::{Context, Result};
use rumqttc::{Client, Connection, Event, MqttOptions, Packet, QoS};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use crate::config::Config;
use linearjc_core::signing::{sign_message, verify_message};

/// MQTT client for developer tool
pub struct MqttClient {
    client: Client,
    connection: Arc<Mutex<Connection>>,
    config: Config,
}

/// Response from coordinator
#[derive(Debug)]
pub struct Response {
    pub payload: Value,
    pub received_at: Instant,
}

impl MqttClient {
    /// Create and connect to MQTT broker
    pub fn connect(config: Config) -> Result<Self> {
        let mut mqtt_options = MqttOptions::new(
            config.mqtt.client_id.clone(),
            config.mqtt.broker.clone(),
            config.mqtt.port,
        );

        mqtt_options.set_keep_alive(Duration::from_secs(config.mqtt.keepalive));
        mqtt_options.set_clean_session(true); // Stateless client

        let (client, connection) = Client::new(mqtt_options, 10);

        Ok(Self {
            client,
            connection: Arc::new(Mutex::new(connection)),
            config,
        })
    }

    /// Wait for MQTT connection to be established
    pub fn wait_connected(&self, timeout: Duration) -> Result<()> {
        let start = Instant::now();

        loop {
            if start.elapsed() > timeout {
                anyhow::bail!("Connection timeout after {:?}", timeout);
            }

            let mut conn = self
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            match conn.iter().next() {
                Some(Ok(Event::Incoming(Packet::ConnAck(_)))) => {
                    return Ok(());
                }
                Some(Err(e)) => {
                    anyhow::bail!("Connection failed: {}", e);
                }
                _ => {
                    // Continue waiting
                    std::thread::sleep(Duration::from_millis(100));
                }
            }
        }
    }

    /// Subscribe to a topic
    pub fn subscribe(&self, topic: &str) -> Result<()> {
        self.client
            .subscribe(topic, QoS::AtLeastOnce)
            .context(format!("Failed to subscribe to {}", topic))
    }

    /// Publish a signed message to a topic
    pub fn publish_signed(&self, topic: &str, payload: Value) -> Result<()> {
        // Sign the message
        let envelope = sign_message(payload, &self.config.auth.shared_secret)?;

        // Convert to JSON
        let json = serde_json::to_string(&envelope)
            .context("Failed to serialize signed message")?;

        // Publish
        self.client
            .publish(topic, QoS::AtLeastOnce, false, json.as_bytes())
            .context(format!("Failed to publish to {}", topic))?;

        Ok(())
    }

    /// Wait for a response on a topic with timeout
    ///
    /// This is a blocking call that waits for a message matching the request_id.
    /// Returns the verified payload or error after timeout.
    /// NOTE: Caller must subscribe to the topic before calling this method.
    pub fn wait_for_response(&self, topic: &str, request_id: &str, timeout: Duration) -> Result<Value> {
        let start = Instant::now();

        loop {
            if start.elapsed() > timeout {
                anyhow::bail!("Response timeout after {:?}", timeout);
            }

            let mut conn = self
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            match conn.iter().next() {
                Some(Ok(Event::Incoming(Packet::Publish(msg)))) => {
                    // Check if this is the response we're waiting for
                    if msg.topic != topic {
                        continue;
                    }

                    // Parse envelope
                    let envelope: Value = match serde_json::from_slice(&msg.payload) {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("Failed to parse response JSON: {}", e);
                            continue;
                        }
                    };

                    // Verify signature
                    let payload = match verify_message(&envelope, &self.config.auth.shared_secret, 60) {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("Failed to verify response signature: {}", e);
                            continue;
                        }
                    };

                    // Check if this matches our request_id
                    if let Some(rid) = payload.get("request_id").and_then(|v| v.as_str()) {
                        if rid == request_id {
                            return Ok(payload);
                        }
                    }

                    // Not our response, keep waiting
                }
                Some(Err(e)) => {
                    anyhow::bail!("MQTT error while waiting for response: {}", e);
                }
                _ => {
                    // Other events, keep waiting
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }

    /// Wait for any message on a progress topic with timeout
    ///
    /// Similar to wait_for_response but doesn't filter by request_id.
    /// Returns the verified payload or error after timeout.
    /// NOTE: Caller must subscribe to the topic before calling this method.
    pub fn wait_for_progress(&self, topic: &str, timeout: Duration) -> Result<Value> {
        let start = Instant::now();

        loop {
            if start.elapsed() > timeout {
                anyhow::bail!("Progress timeout after {:?}", timeout);
            }

            let mut conn = self
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            match conn.iter().next() {
                Some(Ok(Event::Incoming(Packet::Publish(msg)))) => {
                    // Check if this is the topic we're waiting for
                    if msg.topic != topic {
                        continue;
                    }

                    // Parse envelope
                    let envelope: Value = match serde_json::from_slice(&msg.payload) {
                        Ok(v) => v,
                        Err(e) => {
                            eprintln!("Failed to parse progress JSON: {}", e);
                            continue;
                        }
                    };

                    // Verify signature
                    let payload = match verify_message(&envelope, &self.config.auth.shared_secret, 60) {
                        Ok(p) => p,
                        Err(e) => {
                            eprintln!("Failed to verify progress signature: {}", e);
                            continue;
                        }
                    };

                    return Ok(payload);
                }
                Some(Err(e)) => {
                    anyhow::bail!("MQTT error while waiting for progress: {}", e);
                }
                _ => {
                    // Other events, keep waiting
                    std::thread::sleep(Duration::from_millis(50));
                }
            }
        }
    }

    /// Process events for a duration (non-blocking event loop)
    pub fn process_events(&self, duration: Duration) -> Result<()> {
        let start = Instant::now();

        while start.elapsed() < duration {
            let mut conn = self
                .connection
                .lock()
                .map_err(|e| anyhow::anyhow!("Lock poisoned: {}", e))?;

            match conn.iter().next() {
                Some(Ok(_)) => {
                    // Event processed
                }
                Some(Err(e)) => {
                    eprintln!("MQTT event error: {}", e);
                }
                None => {
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
        }

        Ok(())
    }

    /// Disconnect from broker
    pub fn disconnect(self) -> Result<()> {
        self.client
            .disconnect()
            .context("Failed to disconnect from MQTT broker")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mqtt_options_configuration() {
        let config = Config {
            mqtt: crate::config::MqttConfig {
                broker: "test-broker".to_string(),
                port: 1883,
                client_id: "test-client".to_string(),
                keepalive: 60,
            },
            coordinator: crate::config::CoordinatorConfig {
                id: "test-coordinator".to_string(),
            },
            auth: crate::config::AuthConfig {
                shared_secret: "test-secret".to_string(),
            },
        };

        // Just verify we can create MQTT options
        // Can't actually connect without a broker running
        let mqtt_options = MqttOptions::new(
            config.mqtt.client_id.clone(),
            config.mqtt.broker.clone(),
            config.mqtt.port,
        );

        assert_eq!(mqtt_options.broker_address().0, "test-broker");
        assert_eq!(mqtt_options.broker_address().1, 1883);
    }
}
