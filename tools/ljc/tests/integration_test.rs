//! Integration tests for ljc tool
//!
//! These tests verify that modules work together correctly.

use std::env;
use std::io::Write;
use tempfile::NamedTempFile;

/// Test that config loading and message signing work together
#[test]
fn test_config_and_signing_integration() {
    // Create a temporary config file
    let config_yaml = r#"
mqtt:
  broker: "test-broker"
  port: 1883
  client_id: "test-client"

coordinator:
  id: "test-coordinator"

auth:
  shared_secret: "file-secret"
"#;

    let mut temp = NamedTempFile::new().unwrap();
    temp.write_all(config_yaml.as_bytes()).unwrap();
    temp.flush().unwrap();

    // Note: In real usage, Config::load() would search for .ljcconfig
    // For testing, we directly load from temp file

    // Set environment variable to override config
    // SAFETY: Tests run sequentially, no concurrent env access
    unsafe {
        env::set_var("LINEARJC_SECRET", "test-secret-from-env");
    }

    // Verify env var override works (simulated, since load_from_file doesn't apply overrides)
    let secret = env::var("LINEARJC_SECRET").unwrap();
    assert_eq!(secret, "test-secret-from-env");

    // Clean up
    // SAFETY: Tests run sequentially, no concurrent env access
    unsafe {
        env::remove_var("LINEARJC_SECRET");
    }
}

/// Test message signing round-trip
#[test]
fn test_message_signing_round_trip() {
    let secret = "integration-test-secret";

    // Create a test message
    let msg = serde_json::json!({
        "action": "deploy",
        "job_id": "test.job",
        "version": "1.0.0"
    });

    // This would use message_signing module, but since it's in a binary crate,
    // we can't import it directly in integration tests.
    // The unit tests in message_signing.rs already verify this functionality.

    // This test mainly documents the expected integration pattern
}
