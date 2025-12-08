use anyhow::{bail, Result};
use colored::Colorize;
use std::fs;
use std::path::Path;

use crate::utils::Repository;

pub fn run(path: &Path) -> Result<()> {
    if path.exists() {
        bail!("Directory already exists: {}", path.display());
    }

    println!("{}", "Initializing LinearJC job repository...".bright_blue());
    println!("Path: {}\n", path.display());

    // Create repository structure
    let repo = Repository::create(path)?;

    // Create empty registry.yaml
    let registry_content = r#"# LinearJC Registry
# Define data registers for jobs to read from and write to
#
# Format:
#   register_name: {type: fs, path: /path/to/file, kind: file}
#   register_name: {type: fs, path: /path/to/dir, kind: dir}
#   register_name: {type: minio, bucket: mybucket, prefix: path/}

registry:
  # Example registers:
  # my_input:  {type: fs, path: /var/share/data/input.txt, kind: file}
  # my_output: {type: fs, path: /var/share/data/output/, kind: dir}
"#;

    fs::write(&repo.registry_file, registry_content)?;

    // Create README.md
    let readme_content = r#"# LinearJC Job Repository

## Development Workflow

1. **Define registers**: Edit `registry.yaml` to define data locations
2. **Create job**: `ljc new process.daily --reads input_reg --writes output_reg`
3. **Implement**: Edit `jobs/process.daily/script.sh`
4. **Validate**: `ljc validate process.daily`
5. **Build**: `ljc build process.daily`
6. **Deploy**: `ljc deploy dist/process.daily.ljc --to coordinator`

## Directory Structure

- `jobs/` - Job definitions (one directory per job)
- `dist/` - Built .ljc packages
- `registry.yaml` - Register definitions (data locations)

## Commands

```bash
ljc init <path>                    # Initialize repository
ljc new <job-id>                   # Create new job
ljc info <job-id>                  # Show job information
ljc list                           # List all jobs
ljc validate [job-id|--all]        # Validate job(s)
ljc build <job-id>                 # Build package
ljc registry                       # Show registers
ljc deploy <package> --to <host>   # Deploy package
```

## Next Steps

1. Define your registers in `registry.yaml`

2. Create your first job:
   ```bash
   ljc new process.daily
   ```
"#;

    fs::write(repo.root.join("README.md"), readme_content.replace("{}", &path.display().to_string()))?;

    println!("{}", "✓ Repository initialized".green());
    println!();
    println!("Directory structure:");
    println!("  {} - Job definitions", "jobs/".bright_blue());
    println!("  {} - Built packages", "dist/".bright_blue());
    println!("  {} - Register definitions", "registry.yaml".bright_blue());
    println!("  {} - Documentation", "README.md".bright_blue());
    println!();
    println!("Next steps:");
    println!("  cd {}", path.display());
    println!("  vim registry.yaml      {}", "# Define your data registers".dimmed());
    println!("  ljc new process.daily  {}", "# Create your first job".dimmed());

    Ok(())
}
