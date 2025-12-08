ljc(1)

# NAME

ljc - LinearJC job development and deployment tool

# SYNOPSIS

*ljc* <command> [options]

# DESCRIPTION

ljc is a command-line tool for developing, testing, and deploying LinearJC
jobs. It handles the full lifecycle from creating new jobs to monitoring
their execution on remote coordinators.

# COMMANDS

## Development

*init*
	Initialize a new job repository in the current directory.

*new* <job-id>
	Create a new job from template.

*info* [job-id]
	Show job information.

*list*
	List all jobs in repository.

*validate* [job-id]
	Validate job configuration.

*build* [job-id]
	Build job package (.ljc archive).

*bump* <job-id> <major|minor|patch>
	Bump job version.

*test* <job-id>
	Test job locally with optional isolation.

## Deployment

*sync* --from <broker>
	Sync registry from coordinator.

*registry*
	Show local registry map.

*deploy* <package.ljc> --to <broker>
	Deploy package to coordinator.

## Runtime

*exec* <job-id> [--follow|--wait]
	Execute job immediately, bypassing scheduler.

*tail* <job-id|execution-id>
	Follow job execution progress in real-time.

*status* <job-id>|--all
	Show job scheduling status.

*ps* [--all] [--executor <id>]
	List active job executions.

*logs* <job-id> [--last <n>]
	Show job execution history.

*kill* <execution-id> [--force]
	Cancel a running job.

# ENVIRONMENT

*LINEARJC_SECRET*
	Shared secret for coordinator authentication. Required for remote
	operations.

*MQTT_BROKER*
	MQTT broker hostname (default: from .ljcconfig).

*MQTT_PORT*
	MQTT broker port (default: 1883).

# FILES

*.ljcconfig*
	Repository configuration file created by *ljc init*.

*registry.yaml*
	Local data registry definitions.

*jobs/*
	Job source directories.

*dist/*
	Built packages (.ljc files).

# EXIT STATUS

*0*
	Success.

*1*
	General error.

*>1*
	Job exit code (when using *exec --wait*).

# SEE ALSO

*linearjc-coordinator*(1), *linearjc-executor*(1)

# AUTHORS

Kevin Bortis
