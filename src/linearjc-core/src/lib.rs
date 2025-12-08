//! LinearJC Core Library
//!
//! Shared security-critical code for LinearJC executor and ljc tool.
//!
//! This crate provides:
//! - **job**: Job configuration parsing (job.yaml format)
//! - **package**: Package handling (.ljc format, extraction, versioning)
//! - **isolation**: Landlock filesystem, cgroups v2, network namespace isolation
//! - **workdir**: Work directory setup (in/out/tmp structure)
//! - **execution**: Process execution (fork, setuid, exec)
//! - **archive**: Secure tar.gz handling with symlink/traversal protection
//! - **signing**: HMAC-SHA256 message signing and verification
//!
//! # Design Principle
//!
//! This crate is shared ONLY between Rust components:
//! - Executor (job runner on worker nodes)
//! - ljc tool (developer CLI)
//!
//! The coordinator (Python) maintains its own implementations to avoid
//! FFI complexity and keep each component self-contained.
//!
//! # Security
//!
//! All code in this crate is security-critical and should be audited
//! carefully before changes. Dependencies are pinned to exact versions
//! to prevent supply chain attacks.

pub mod archive;
pub mod execution;
pub mod isolation;
pub mod job;
pub mod package;
pub mod signing;
pub mod workdir;

// Re-export commonly used types at crate root
pub use isolation::{FilesystemIsolation, IsolationConfig, ResourceLimits};
pub use signing::{sign_message, verify_message};
pub use workdir::WorkDir;

// Re-export job types
pub use job::{JobFile, JobSpec, RunSpec, LimitsSpec, parse_job_file, parse_job_yaml};

// Re-export package types and functions
pub use package::{
    PackageMetadata,
    read_package_metadata, get_package_version, extract_package_to_workdir,
    compare_versions,
};

// Re-export execution types and functions
pub use execution::{ExecutionResult, validate_job_script, execute_simple};
#[cfg(unix)]
pub use execution::{
    SpawnedJob, spawn_isolated, poll_job, wait_for_job, kill_job, cleanup_job,
    execute_isolated, validate_and_get_uid,
};
