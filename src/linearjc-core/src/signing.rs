//! Message signing for LinearJC.
//!
//! Implements HMAC-SHA256 message signing with timestamp validation.
//! This module is shared between executor and ljc tool.
//!
//! The coordinator (Python) maintains its own implementation but uses
//! the same algorithm for interoperability.

use anyhow::{Context, Result};
use chrono::Utc;
use hmac::{Hmac, Mac};
use serde_json::Value;
use sha2::Sha256;
use subtle::ConstantTimeEq;

type HmacSha256 = Hmac<Sha256>;

/// Clock skew tolerance (seconds)
/// Allow small time differences between ljc client and coordinator due to NTP sync delays
const CLOCK_SKEW_TOLERANCE: i64 = 30;

/// Sign a message with HMAC-SHA256 using envelope pattern.
///
/// Creates an envelope containing the payload, timestamp, and signature.
/// The signature is computed over payload + timestamp.
///
/// This matches the Python coordinator implementation and Rust executor implementation.
///
/// # Arguments
/// * `msg` - Message payload to sign (will be serialized to JSON)
/// * `secret` - Shared secret for HMAC
///
/// # Returns
/// Envelope JSON with 'payload', 'timestamp', and 'signature'
///
/// # Example
/// ```ignore
/// let msg = serde_json::json!({"job_id": "test.job"});
/// let envelope = sign_message(msg, "secret")?;
/// ```
pub fn sign_message(msg: Value, secret: &str) -> Result<Value> {
    // Serialize payload to canonical JSON (compact, sorted keys)
    // serde_json::to_string() already produces compact JSON
    let payload = serde_json::to_string(&msg)
        .context("Failed to serialize message to JSON")?;

    // Create timestamp (ISO 8601 UTC with 'Z' suffix)
    let timestamp = Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();

    // Compute signature over payload + timestamp
    let to_sign = format!("{}{}", payload, timestamp);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|e| anyhow::anyhow!("Invalid key: {}", e))?;
    mac.update(to_sign.as_bytes());
    let signature = hex::encode(mac.finalize().into_bytes());

    // Return envelope
    Ok(serde_json::json!({
        "payload": payload,
        "timestamp": timestamp,
        "signature": signature
    }))
}

/// Verify a message envelope and return the payload.
///
/// Validates signature and timestamp, then returns the parsed payload.
///
/// # Arguments
/// * `envelope` - Envelope JSON with 'payload', 'timestamp', 'signature'
/// * `secret` - Shared secret for HMAC
/// * `max_age_secs` - Maximum message age in seconds (default: 60)
///
/// # Returns
/// Parsed payload JSON
///
/// # Errors
/// Returns error if:
/// - Envelope is missing required fields
/// - Timestamp is invalid or too old
/// - Signature is invalid
///
/// # Example
/// ```ignore
/// let payload = verify_message(&envelope, "secret", 60)?;
/// ```
pub fn verify_message(envelope: &Value, secret: &str, max_age_secs: i64) -> Result<Value> {
    // Extract required fields
    let payload = envelope
        .get("payload")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Envelope missing 'payload' field"))?;

    let timestamp_str = envelope
        .get("timestamp")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Envelope missing 'timestamp' field"))?;

    let provided_sig = envelope
        .get("signature")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow::anyhow!("Envelope missing 'signature' field"))?;

    // Parse timestamp
    let timestamp = chrono::DateTime::parse_from_rfc3339(&timestamp_str.replace('Z', "+00:00"))
        .context("Invalid timestamp format")?
        .with_timezone(&Utc);

    // Check message age
    let now = Utc::now();
    let age = (now - timestamp).num_seconds();

    // Allow small clock skew for messages slightly in the future
    // This is critical when verifying coordinator responses where coordinator clock may be ahead
    if age < -CLOCK_SKEW_TOLERANCE {
        anyhow::bail!(
            "Message timestamp too far in future: {}s (max skew: {}s)",
            -age,
            CLOCK_SKEW_TOLERANCE
        );
    }

    if age > max_age_secs {
        anyhow::bail!("Message too old: {}s (max: {}s)", age, max_age_secs);
    }

    // Verify signature using constant-time comparison (prevents timing attacks)
    let to_verify = format!("{}{}", payload, timestamp_str);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|e| anyhow::anyhow!("Invalid key: {}", e))?;
    mac.update(to_verify.as_bytes());
    let expected_sig_bytes = mac.finalize().into_bytes();

    // SECURITY CRITICAL: Use constant-time comparison to prevent timing attacks
    // Convert hex strings to bytes for comparison
    let provided_sig_bytes = hex::decode(provided_sig).context("Invalid signature encoding")?;

    // Constant-time comparison using subtle crate
    if expected_sig_bytes.ct_eq(&provided_sig_bytes).into() {
        // Signature valid - return parsed payload
        serde_json::from_str(payload).context("Invalid JSON in payload")
    } else {
        anyhow::bail!("Invalid signature")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sign_and_verify() {
        let secret = "test-secret";
        let msg = serde_json::json!({
            "job_id": "test.job",
            "action": "deploy"
        });

        // Sign message
        let envelope = sign_message(msg.clone(), secret).unwrap();

        // Verify envelope structure
        assert!(envelope.get("payload").is_some());
        assert!(envelope.get("timestamp").is_some());
        assert!(envelope.get("signature").is_some());

        // Verify message (with generous max age for test)
        let payload = verify_message(&envelope, secret, 60).unwrap();

        // Check payload matches original
        assert_eq!(payload.get("job_id").unwrap(), "test.job");
        assert_eq!(payload.get("action").unwrap(), "deploy");
    }

    #[test]
    fn test_wrong_secret_fails() {
        let msg = serde_json::json!({"test": "data"});
        let envelope = sign_message(msg, "correct-secret").unwrap();

        // Try to verify with wrong secret
        let result = verify_message(&envelope, "wrong-secret", 60);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid signature"));
    }

    #[test]
    fn test_missing_fields() {
        let secret = "test-secret";

        // Missing payload
        let envelope = serde_json::json!({
            "timestamp": "2025-01-01T00:00:00Z",
            "signature": "abc123"
        });
        assert!(verify_message(&envelope, secret, 60).is_err());

        // Missing timestamp
        let envelope = serde_json::json!({
            "payload": "{}",
            "signature": "abc123"
        });
        assert!(verify_message(&envelope, secret, 60).is_err());

        // Missing signature
        let envelope = serde_json::json!({
            "payload": "{}",
            "timestamp": "2025-01-01T00:00:00Z"
        });
        assert!(verify_message(&envelope, secret, 60).is_err());
    }

    #[test]
    fn test_message_too_old() {
        let secret = "test-secret";

        // Create envelope with old timestamp
        let old_timestamp = Utc::now() - chrono::Duration::seconds(120);
        let payload = serde_json::to_string(&serde_json::json!({"test": "data"})).unwrap();
        let timestamp_str = old_timestamp.format("%Y-%m-%dT%H:%M:%S%.6fZ").to_string();

        // Compute signature manually
        let to_sign = format!("{}{}", payload, timestamp_str);
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(to_sign.as_bytes());
        let signature = hex::encode(mac.finalize().into_bytes());

        let envelope = serde_json::json!({
            "payload": payload,
            "timestamp": timestamp_str,
            "signature": signature
        });

        // Should fail with max_age_secs=60 (message is 120s old)
        let result = verify_message(&envelope, secret, 60);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too old"));
    }

    #[test]
    fn test_tampered_payload() {
        let secret = "test-secret";
        let msg = serde_json::json!({"amount": 100});

        let mut envelope = sign_message(msg, secret).unwrap();

        // Tamper with payload
        envelope["payload"] = serde_json::Value::String(
            serde_json::to_string(&serde_json::json!({"amount": 999})).unwrap(),
        );

        // Verification should fail
        let result = verify_message(&envelope, secret, 60);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Invalid signature"));
    }
}
