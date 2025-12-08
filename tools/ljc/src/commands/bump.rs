use anyhow::{bail, Context, Result};
use colored::Colorize;
use serde_yaml::Value;
use std::fs;
use std::path::Path;

use crate::utils::Repository;

#[derive(Debug, Clone, Copy)]
pub enum BumpType {
    Major,
    Minor,
    Patch,
}

impl BumpType {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "major" => Ok(Self::Major),
            "minor" => Ok(Self::Minor),
            "patch" => Ok(Self::Patch),
            _ => bail!("Invalid bump type: {}. Use 'major', 'minor', or 'patch'", s),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Version {
    major: u32,
    minor: u32,
    patch: u32,
}

impl Version {
    fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self { major, minor, patch }
    }

    fn parse(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s.split('.').collect();
        if parts.len() != 3 {
            bail!("Invalid version format: '{}'. Expected 'major.minor.patch'", s);
        }

        let major = parts[0]
            .parse::<u32>()
            .with_context(|| format!("Invalid major version: '{}'", parts[0]))?;
        let minor = parts[1]
            .parse::<u32>()
            .with_context(|| format!("Invalid minor version: '{}'", parts[1]))?;
        let patch = parts[2]
            .parse::<u32>()
            .with_context(|| format!("Invalid patch version: '{}'", parts[2]))?;

        Ok(Self { major, minor, patch })
    }

    fn bump(&self, bump_type: BumpType) -> Self {
        match bump_type {
            BumpType::Major => Self::new(self.major + 1, 0, 0),
            BumpType::Minor => Self::new(self.major, self.minor + 1, 0),
            BumpType::Patch => Self::new(self.major, self.minor, self.patch + 1),
        }
    }

    fn to_string(&self) -> String {
        format!("{}.{}.{}", self.major, self.minor, self.patch)
    }
}

pub fn run(bump_type: &str, job_id: &str, dry_run: bool) -> Result<()> {
    let bump = BumpType::from_str(bump_type)?;
    let repo = Repository::find()?;
    let job_path = repo.job_path(job_id);

    if !job_path.exists() {
        bail!("Job not found: {}", job_id);
    }

    println!(
        "{}",
        format!("Bumping version: {} ({})", job_id, bump_type).bright_blue()
    );
    println!("{}", "=".repeat(60).dimmed());
    println!();

    // Check required files exist
    let job_yaml = job_path.join("job.yaml");

    if !job_yaml.exists() {
        bail!("Missing job.yaml in {}", job_path.display());
    }

    // Read and parse job.yaml
    println!("{}", "Reading job.yaml...".dimmed());
    let job_content = fs::read_to_string(&job_yaml)
        .with_context(|| format!("Failed to read {}", job_yaml.display()))?;

    let mut job_data: Value = serde_yaml::from_str(&job_content)
        .with_context(|| format!("Failed to parse {}", job_yaml.display()))?;

    // Get current version from job.yaml (or default to 0.1.0)
    let current_version = job_data
        .get("job")
        .and_then(|j| j.get("version"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let current_ver = match current_version {
        Some(v) => {
            println!("  {} Current version: {}", "→".dimmed(), v.bright_white());
            Version::parse(&v).with_context(|| {
                format!(
                    "Invalid version in job.yaml: '{}'. Expected format: 'major.minor.patch'",
                    v
                )
            })?
        }
        None => {
            println!(
                "  {} No version found, defaulting to {}",
                "!".yellow(),
                "0.1.0".bright_white()
            );
            Version::new(0, 1, 0)
        }
    };

    // Bump version
    let new_ver = current_ver.bump(bump);

    println!();
    println!("  {} {}", "Old version:".dimmed(), current_ver.to_string().bright_white());
    println!("  {} {}", "New version:".bright_green(), new_ver.to_string().bright_white());
    println!();

    if dry_run {
        println!("{}", "✓ Dry run - no files modified".yellow());
        return Ok(());
    }

    // Update job.yaml
    println!("{}", "Updating job.yaml...".dimmed());
    if let Some(job) = job_data.get_mut("job") {
        if let Some(obj) = job.as_mapping_mut() {
            obj.insert(
                Value::String("version".to_string()),
                Value::String(new_ver.to_string()),
            );
        }
    } else {
        bail!("job.yaml missing 'job' section");
    }

    fs::write(
        &job_yaml,
        serde_yaml::to_string(&job_data)
            .context("Failed to serialize job.yaml")?,
    )
    .with_context(|| format!("Failed to write {}", job_yaml.display()))?;

    println!("  {} job.yaml updated", "✓".green());
    println!();
    println!("{}", format!("✓ Version bumped: {} → {}", current_ver.to_string(), new_ver.to_string()).green());
    println!();
    println!("Next steps:");
    println!("  ljc validate {}    {}", job_id, "# Validate changes".dimmed());
    println!("  ljc build {}       {}", job_id, "# Build package".dimmed());

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version_parse() {
        let v = Version::parse("1.2.3").unwrap();
        assert_eq!(v.major, 1);
        assert_eq!(v.minor, 2);
        assert_eq!(v.patch, 3);
    }

    #[test]
    fn test_version_parse_invalid() {
        assert!(Version::parse("1.2").is_err());
        assert!(Version::parse("1.2.3.4").is_err());
        assert!(Version::parse("a.b.c").is_err());
    }

    #[test]
    fn test_version_bump() {
        let v = Version::new(1, 2, 3);

        let major = v.bump(BumpType::Major);
        assert_eq!(major, Version::new(2, 0, 0));

        let minor = v.bump(BumpType::Minor);
        assert_eq!(minor, Version::new(1, 3, 0));

        let patch = v.bump(BumpType::Patch);
        assert_eq!(patch, Version::new(1, 2, 4));
    }

    #[test]
    fn test_version_to_string() {
        let v = Version::new(1, 2, 3);
        assert_eq!(v.to_string(), "1.2.3");
    }

    #[test]
    fn test_bump_type_from_str() {
        assert!(matches!(BumpType::from_str("major").unwrap(), BumpType::Major));
        assert!(matches!(BumpType::from_str("MINOR").unwrap(), BumpType::Minor));
        assert!(matches!(BumpType::from_str("patch").unwrap(), BumpType::Patch));
        assert!(BumpType::from_str("invalid").is_err());
    }
}
