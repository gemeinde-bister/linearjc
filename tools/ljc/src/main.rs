use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod commands;
mod config;
mod mqtt_client;
mod progress;
mod registry;
mod utils;
mod validator;

// message_signing is now in linearjc_core::signing

/// LinearJC job development and deployment tool
#[derive(Parser)]
#[command(name = "ljc")]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new LinearJC job repository
    Init {
        /// Path to create repository
        path: PathBuf,
    },

    /// Create a new job from template
    New {
        /// Job ID (e.g., process.daily)
        job_id: String,
    },

    /// Show job information
    Info {
        /// Job ID
        job_id: String,
    },

    /// List all jobs in repository
    List,

    /// Validate job(s)
    Validate {
        /// Job ID to validate (or --all for all jobs)
        job_id: Option<String>,

        /// Validate all jobs
        #[arg(long)]
        all: bool,
    },

    /// Build job package (.ljc)
    Build {
        /// Job ID to build
        job_id: String,

        /// Output path (default: dist/<job-id>.ljc)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Bump job version (major, minor, or patch)
    Bump {
        /// Bump type: major, minor, or patch
        bump_type: String,

        /// Job ID
        job_id: String,

        /// Dry run - show changes without modifying files
        #[arg(long)]
        dry_run: bool,
    },

    /// Extract package to local directory with executor-like structure
    Extract {
        /// Package path (.ljc file)
        package: PathBuf,

        /// Output directory (default: .extract)
        #[arg(short, long)]
        output: Option<PathBuf>,
    },

    /// Test a job locally with optional isolation
    Test {
        /// Job ID to test
        job_id: String,

        /// Skip isolation (faster, no root required)
        #[arg(long)]
        no_isolation: bool,

        /// Override isolation mode (strict, relaxed, none)
        #[arg(long)]
        isolation: Option<String>,

        /// Timeout in seconds (default: from job.yaml or 300)
        #[arg(long, default_value = "0")]
        timeout: u64,

        /// Show script stdout/stderr
        #[arg(long, short)]
        verbose: bool,

        /// Keep workdir after execution (for debugging)
        #[arg(long)]
        keep: bool,

        /// Override network access (true/false)
        #[arg(long)]
        network: Option<bool>,
    },

    /// Sync registry from coordinator
    Sync {
        /// Coordinator hostname
        #[arg(long)]
        from: String,
    },

    /// Registry management commands
    #[command(subcommand)]
    Registry(RegistryCommands),

    /// Deploy package to coordinator
    Deploy {
        /// Package path (.ljc file)
        package: PathBuf,

        /// Coordinator hostname
        #[arg(long)]
        to: String,
    },

    /// Execute a job immediately (bypass scheduler)
    Exec {
        /// Job ID to execute
        job_id: String,

        /// Stream real-time progress updates
        #[arg(long, short)]
        follow: bool,

        /// Block until completion, exit with job exit code
        #[arg(long, short)]
        wait: bool,

        /// Timeout in seconds for waiting (default: 3600)
        #[arg(long, default_value = "3600")]
        timeout: u64,
    },

    /// Follow job execution progress in real-time
    Tail {
        /// Job ID (attaches to active execution) or execution ID
        id: String,

        /// Timeout in seconds for waiting (default: 3600)
        #[arg(long, default_value = "3600")]
        timeout: u64,
    },

    /// Show job scheduling status
    Status {
        /// Job ID to query (required unless --all)
        job_id: Option<String>,

        /// Show all jobs
        #[arg(long)]
        all: bool,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// List active job executions
    Ps {
        /// Filter by executor ID
        #[arg(long)]
        executor: Option<String>,

        /// Show all jobs (including completed)
        #[arg(long, short)]
        all: bool,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Show job execution history
    Logs {
        /// Job ID to query
        job_id: String,

        /// Number of executions to show
        #[arg(long, default_value = "10")]
        last: u32,

        /// Show only failed executions
        #[arg(long)]
        failed: bool,

        /// Output as JSON
        #[arg(long)]
        json: bool,
    },

    /// Cancel a running job
    Kill {
        /// Execution ID to kill
        execution_id: String,

        /// Force kill (SIGKILL instead of SIGTERM)
        #[arg(long)]
        force: bool,

        /// Wait for job to terminate
        #[arg(long)]
        wait: bool,
    },
}

#[derive(Subcommand)]
enum RegistryCommands {
    /// List all registers
    List {
        /// Show detailed registry information
        #[arg(long)]
        verbose: bool,
    },

    /// Add a new register
    Add {
        /// Register name
        name: String,

        /// Register type (fs or minio)
        #[arg(long, default_value = "fs")]
        r#type: String,

        /// Filesystem path (for type=fs)
        #[arg(long)]
        path: Option<String>,

        /// Kind: file or dir (for type=fs)
        #[arg(long, default_value = "file")]
        kind: String,

        /// MinIO bucket (for type=minio)
        #[arg(long)]
        bucket: Option<String>,

        /// MinIO prefix (for type=minio)
        #[arg(long)]
        prefix: Option<String>,
    },

    /// Push registry to coordinator
    Push {
        /// Coordinator hostname (optional, uses config default)
        #[arg(long)]
        to: Option<String>,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { path } => {
            commands::init::run(&path)?;
        }
        Commands::New { job_id } => {
            commands::new::run(&job_id)?;
        }
        Commands::Info { job_id } => {
            commands::info::run(&job_id)?;
        }
        Commands::List => {
            commands::list::run()?;
        }
        Commands::Validate { job_id, all } => {
            commands::validate::run(job_id.as_deref(), all)?;
        }
        Commands::Build { job_id, output } => {
            commands::build::run(&job_id, output.as_deref())?;
        }
        Commands::Bump { bump_type, job_id, dry_run } => {
            commands::bump::run(&bump_type, &job_id, dry_run)?;
        }
        Commands::Extract { package, output } => {
            commands::extract::run(&package, output.as_deref())?;
        }
        Commands::Test { job_id, no_isolation, isolation, timeout, verbose, keep, network } => {
            commands::test::run(&job_id, no_isolation, isolation.as_deref(), timeout, verbose, keep, network)?;
        }
        Commands::Sync { from } => {
            commands::sync::run(&from)?;
        }
        Commands::Registry(subcmd) => match subcmd {
            RegistryCommands::List { verbose } => {
                commands::registry::run_list(verbose)?;
            }
            RegistryCommands::Add { name, r#type, path, kind, bucket, prefix } => {
                commands::registry::run_add(&name, &r#type, path.as_deref(), &kind, bucket.as_deref(), prefix.as_deref())?;
            }
            RegistryCommands::Push { to } => {
                commands::registry::run_push(to.as_deref())?;
            }
        }
        Commands::Deploy { package, to } => {
            commands::deploy::run(&package, &to)?;
        }
        Commands::Exec { job_id, follow, wait, timeout } => {
            commands::exec::run(&job_id, follow, wait, timeout)?;
        }
        Commands::Tail { id, timeout } => {
            commands::tail::run(&id, timeout)?;
        }
        Commands::Status { job_id, all, json } => {
            commands::status::run(job_id.as_deref(), all, json)?;
        }
        Commands::Ps { executor, all, json } => {
            commands::ps::run(executor.as_deref(), all, json)?;
        }
        Commands::Logs { job_id, last, failed, json } => {
            commands::logs::run(&job_id, last, failed, json)?;
        }
        Commands::Kill { execution_id, force, wait } => {
            commands::kill::run(&execution_id, force, wait)?;
        }
    }

    Ok(())
}
