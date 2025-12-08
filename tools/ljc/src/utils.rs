use anyhow::{bail, Context, Result};
use serde_yaml::Value;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Repository structure
pub struct Repository {
    pub root: PathBuf,
    pub jobs_dir: PathBuf,
    pub dist_dir: PathBuf,
    pub registry_file: PathBuf,
}

impl Repository {
    /// Find repository root from current directory
    pub fn find() -> Result<Self> {
        let mut current = std::env::current_dir()?;

        loop {
            let registry_file = current.join("registry.yaml");
            if registry_file.exists() {
                return Ok(Self {
                    root: current.clone(),
                    jobs_dir: current.join("jobs"),
                    dist_dir: current.join("dist"),
                    registry_file,
                });
            }

            if !current.pop() {
                bail!("Not in a LinearJC repository (no registry.yaml found)");
            }
        }
    }

    /// Create repository structure
    pub fn create(path: &Path) -> Result<Self> {
        let root = path.to_path_buf();
        let jobs_dir = root.join("jobs");
        let dist_dir = root.join("dist");
        let registry_file = root.join("registry.yaml");

        fs::create_dir_all(&jobs_dir)
            .context("Failed to create jobs directory")?;
        fs::create_dir_all(&dist_dir)
            .context("Failed to create dist directory")?;

        Ok(Self {
            root,
            jobs_dir,
            dist_dir,
            registry_file,
        })
    }

    /// List all jobs in repository
    pub fn list_jobs(&self) -> Result<Vec<String>> {
        let mut jobs = Vec::new();

        if !self.jobs_dir.exists() {
            return Ok(jobs);
        }

        for entry in fs::read_dir(&self.jobs_dir)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    // Check if it has job.yaml
                    if path.join("job.yaml").exists() {
                        jobs.push(name.to_string());
                    }
                }
            }
        }

        jobs.sort();
        Ok(jobs)
    }

    /// Get job directory path
    pub fn job_path(&self, job_id: &str) -> PathBuf {
        self.jobs_dir.join(job_id)
    }
}

/// YAML utilities for compact format
pub struct YamlUtils;

impl YamlUtils {
    /// Write YAML in compact one-line format (flow style)
    pub fn write_compact(path: &Path, data: &BTreeMap<String, Value>) -> Result<()> {
        let mut output = String::from("registry:\n");

        for (key, value) in data {
            // Convert value to compact flow style
            let compact = Self::to_compact_flow(value)?;
            output.push_str(&format!("  {}: {}\n", key, compact));
        }

        fs::write(path, output)
            .with_context(|| format!("Failed to write {}", path.display()))?;

        Ok(())
    }

    /// Convert YAML value to compact flow style string
    fn to_compact_flow(value: &Value) -> Result<String> {
        match value {
            Value::Mapping(map) => {
                let mut items = Vec::new();
                for (k, v) in map {
                    let key_str = k.as_str().unwrap_or("?");
                    let val_str = match v {
                        Value::String(s) => s.clone(),
                        Value::Bool(b) => b.to_string(),
                        Value::Number(n) => n.to_string(),
                        _ => serde_yaml::to_string(v)?.trim().to_string(),
                    };
                    items.push(format!("{}: {}", key_str, val_str));
                }
                Ok(format!("{{{}}}", items.join(", ")))
            }
            _ => Ok(serde_yaml::to_string(value)?.trim().to_string()),
        }
    }

    /// Parse compact YAML registry
    pub fn parse_registry(path: &Path) -> Result<BTreeMap<String, Value>> {
        if !path.exists() {
            return Ok(BTreeMap::new());
        }

        let content = fs::read_to_string(path)
            .with_context(|| format!("Failed to read {}", path.display()))?;

        let data: serde_yaml::Value = serde_yaml::from_str(&content)
            .with_context(|| format!("Failed to parse {}", path.display()))?;

        // Get registry section - return empty if not found or null
        let registry = match data.get("registry") {
            Some(Value::Null) | None => return Ok(BTreeMap::new()),
            Some(v) => v.as_mapping().context("'registry' must be a mapping")?,
        };

        let mut result = BTreeMap::new();
        for (k, v) in registry {
            if let Some(key_str) = k.as_str() {
                result.insert(key_str.to_string(), v.clone());
            }
        }

        Ok(result)
    }
}

/// Version comparison utilities
pub struct VersionUtils;

impl VersionUtils {
    /// Compare semantic versions
    /// Returns: 1 if v1 > v2, 0 if equal, -1 if v1 < v2
    pub fn compare(v1: &str, v2: &str) -> i32 {
        let parts1: Vec<u32> = Self::parse_version(v1);
        let parts2: Vec<u32> = Self::parse_version(v2);

        for i in 0..3 {
            let p1 = parts1.get(i).unwrap_or(&0);
            let p2 = parts2.get(i).unwrap_or(&0);

            if p1 > p2 {
                return 1;
            } else if p1 < p2 {
                return -1;
            }
        }

        0
    }

    fn parse_version(version: &str) -> Vec<u32> {
        version
            .split('.')
            .filter_map(|s| s.parse::<u32>().ok())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_compare() {
        assert_eq!(VersionUtils::compare("2.0.0", "1.0.0"), 1);
        assert_eq!(VersionUtils::compare("1.0.0", "1.0.0"), 0);
        assert_eq!(VersionUtils::compare("1.0.0", "2.0.0"), -1);
        assert_eq!(VersionUtils::compare("1.2.3", "1.2.2"), 1);
        assert_eq!(VersionUtils::compare("1.0.1", "1.0.10"), -1);
    }
}
