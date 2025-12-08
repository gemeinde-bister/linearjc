//! Job configuration parsing for LinearJC.
//!
//! Provides unified structs for parsing job.yaml files, used by both:
//! - Executor (production job runner)
//! - ljc tool (development CLI)
//!
//! Single source of truth for job configuration format.

use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::{Path, PathBuf};

use crate::isolation::{FilesystemIsolation, IsolationConfig, ResourceLimits};

// ============================================================================
// Job file format (job.yaml)
// ============================================================================

/// Root structure of job.yaml
#[derive(Debug, Clone, Deserialize)]
pub struct JobFile {
    pub job: JobSpec,
}

/// Job specification within job.yaml
#[derive(Debug, Clone, Deserialize)]
pub struct JobSpec {
    /// Unique job identifier (e.g., "backup.pool")
    pub id: String,
    /// Semantic version (e.g., "1.0.0")
    pub version: String,
    /// Registry keys this job reads from
    #[serde(default)]
    pub reads: Vec<String>,
    /// Registry keys this job writes to
    #[serde(default)]
    pub writes: Vec<String>,
    /// Job dependencies (other job IDs that must complete first)
    #[serde(default)]
    pub depends: Vec<String>,
    /// Schedule configuration (cron-like, handled by coordinator)
    #[serde(default)]
    pub schedule: Option<serde_json::Value>,
    /// Runtime configuration
    #[serde(default)]
    pub run: Option<RunSpec>,
}

/// Runtime/execution configuration
#[derive(Debug, Clone, Deserialize)]
pub struct RunSpec {
    /// Unix user to run the job as (default: "nobody")
    #[serde(default = "default_user")]
    pub user: String,
    /// Timeout in seconds (default: 3600)
    #[serde(default = "default_timeout")]
    pub timeout: u64,
    /// Entry script filename (default: "script.sh")
    #[serde(default)]
    pub entry: Option<String>,
    /// Filesystem isolation mode: "strict", "relaxed", or "none" (default: "none")
    #[serde(default = "default_isolation")]
    pub isolation: String,
    /// Network access enabled (default: true)
    #[serde(default = "default_network")]
    pub network: bool,
    /// Extra read-only paths for relaxed isolation mode
    #[serde(default)]
    pub extra_read_paths: Vec<String>,
    /// Resource limits (cgroups v2)
    #[serde(default)]
    pub limits: Option<LimitsSpec>,
}

/// Resource limits for cgroups v2
#[derive(Debug, Clone, Deserialize)]
pub struct LimitsSpec {
    /// CPU limit as percentage of one core (0-100)
    pub cpu_percent: Option<u32>,
    /// Memory limit in MB
    pub memory_mb: Option<u64>,
    /// Maximum number of processes (fork bomb protection)
    pub processes: Option<u64>,
    /// I/O bandwidth limit in MB/s
    pub io_mbps: Option<u64>,
}

// Default value functions for serde
fn default_user() -> String {
    "nobody".to_string()
}

fn default_timeout() -> u64 {
    3600
}

fn default_isolation() -> String {
    "none".to_string()
}

fn default_network() -> bool {
    true
}

// ============================================================================
// Parsing functions
// ============================================================================

/// Parse a job.yaml file from disk.
///
/// # Arguments
/// * `path` - Path to the job.yaml file
///
/// # Returns
/// Parsed JobFile structure
pub fn parse_job_file(path: &Path) -> Result<JobFile> {
    let content = fs::read_to_string(path)
        .context(format!("Failed to read job.yaml: {}", path.display()))?;

    parse_job_yaml(&content)
        .context(format!("Failed to parse job.yaml: {}", path.display()))
}

/// Parse job.yaml content from a string.
///
/// Useful when reading from archives or other sources.
pub fn parse_job_yaml(content: &str) -> Result<JobFile> {
    serde_yaml::from_str(content)
        .context("Invalid job.yaml format")
}

// ============================================================================
// Conversion to IsolationConfig
// ============================================================================

impl RunSpec {
    /// Get the entry script filename (defaults to "script.sh")
    pub fn entry_script(&self) -> &str {
        self.entry.as_deref().unwrap_or("script.sh")
    }

    /// Convert RunSpec to IsolationConfig for execution.
    pub fn to_isolation_config(&self) -> Result<IsolationConfig> {
        let filesystem = FilesystemIsolation::from_str(&self.isolation)?;

        let limits = self.limits.as_ref().map(|l| ResourceLimits {
            cpu_percent: l.cpu_percent,
            memory_mb: l.memory_mb,
            processes: l.processes,
            io_mbps: l.io_mbps,
        });

        Ok(IsolationConfig {
            filesystem,
            network: self.network,
            limits,
            extra_read_paths: self.extra_read_paths
                .iter()
                .map(PathBuf::from)
                .collect(),
        })
    }
}

impl Default for RunSpec {
    fn default() -> Self {
        Self {
            user: default_user(),
            timeout: default_timeout(),
            entry: None,
            isolation: default_isolation(),
            network: default_network(),
            extra_read_paths: Vec::new(),
            limits: None,
        }
    }
}

impl JobSpec {
    /// Get the RunSpec, using defaults if not specified in job.yaml
    pub fn run_config(&self) -> RunSpec {
        self.run.clone().unwrap_or_default()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_minimal_job() {
        let yaml = r#"
job:
  id: test.job
  version: 1.0.0
"#;
        let job = parse_job_yaml(yaml).unwrap();
        assert_eq!(job.job.id, "test.job");
        assert_eq!(job.job.version, "1.0.0");
        assert!(job.job.reads.is_empty());
        assert!(job.job.writes.is_empty());
        assert!(job.job.run.is_none());
    }

    #[test]
    fn test_parse_full_job() {
        let yaml = r#"
job:
  id: backup.pool
  version: 2.1.0
  reads:
    - source_data
    - config
  writes:
    - backup_output
  depends:
    - prepare.job
  run:
    user: backupuser
    timeout: 7200
    entry: backup.sh
    isolation: strict
    network: false
    extra_read_paths:
      - /etc/backup
    limits:
      cpu_percent: 50
      memory_mb: 1024
      processes: 10
"#;
        let job = parse_job_yaml(yaml).unwrap();
        assert_eq!(job.job.id, "backup.pool");
        assert_eq!(job.job.version, "2.1.0");
        assert_eq!(job.job.reads, vec!["source_data", "config"]);
        assert_eq!(job.job.writes, vec!["backup_output"]);
        assert_eq!(job.job.depends, vec!["prepare.job"]);

        let run = job.job.run.unwrap();
        assert_eq!(run.user, "backupuser");
        assert_eq!(run.timeout, 7200);
        assert_eq!(run.entry, Some("backup.sh".to_string()));
        assert_eq!(run.isolation, "strict");
        assert!(!run.network);
        assert_eq!(run.extra_read_paths, vec!["/etc/backup"]);

        let limits = run.limits.unwrap();
        assert_eq!(limits.cpu_percent, Some(50));
        assert_eq!(limits.memory_mb, Some(1024));
        assert_eq!(limits.processes, Some(10));
    }

    #[test]
    fn test_run_spec_defaults() {
        let run = RunSpec::default();
        assert_eq!(run.user, "nobody");
        assert_eq!(run.timeout, 3600);
        assert_eq!(run.entry_script(), "script.sh");
        assert_eq!(run.isolation, "none");
        assert!(run.network);
    }

    #[test]
    fn test_to_isolation_config() {
        let run = RunSpec {
            user: "testuser".to_string(),
            timeout: 60,
            entry: None,
            isolation: "strict".to_string(),
            network: false,
            extra_read_paths: vec!["/opt/libs".to_string()],
            limits: Some(LimitsSpec {
                cpu_percent: Some(25),
                memory_mb: Some(256),
                processes: Some(5),
                io_mbps: None,
            }),
        };

        let config = run.to_isolation_config().unwrap();
        assert_eq!(config.filesystem, FilesystemIsolation::Strict);
        assert!(!config.network);
        assert_eq!(config.extra_read_paths, vec![PathBuf::from("/opt/libs")]);

        let limits = config.limits.unwrap();
        assert_eq!(limits.cpu_percent, Some(25));
        assert_eq!(limits.memory_mb, Some(256));
    }
}
