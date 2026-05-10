## LinearJC

A small, mainframe-style batch controller for register-driven workflows. Jobs declare the named data registers they read and write; LinearJC validates ownership, protects shared data with locks, and runs batch work in a predictable order with clear auditability.

The current implementation supports linear chains and fan-out. The project direction is to keep the user model register-driven, including future multi-input barrier jobs, instead of becoming a general DAG authoring platform.

### What it solves
- Deterministic batch execution for easy auditing and debugging.
- Explicit data contracts: jobs read and write named registers, not ad hoc paths.
- Single-writer ownership for every managed data register.
- Min/max executions per 24 hours using a sliding window (not cron).
- Stateless executors with signed messaging and pre‑signed artifact transfers.
- Simple single‑server or modest distributed setups without a heavy control plane.
- Isolation where executors run inside KVM and can execute workloads in Docker inside those VMs.

This is intentionally simpler than general DAG platforms (Airflow, Prefect, Dagu). If you need complex branching, UI-driven workflow design, dynamic branching, or broad backfill machinery, those tools may be a better fit. If you want predictable batch flows with strong data ownership and low operational overhead, LinearJC is appropriate.

### Mainframe mental model

| Mainframe concept | LinearJC concept |
|-------------------|------------------|
| Cataloged dataset | Register |
| Temporary dataset | Temp register |
| Program or job step | Job module |
| Job net / application | Batch group |
| GDG generation | Batch generation |
| ENQ / DEQ | Register lock |
| JES spool | Execution archive |

The important rule is data readiness, not graph drawing: a job is eligible when all input registers for the current batch generation are ready and all output registers can be exclusively locked.

### When to use it
- Batch pipelines where outputs are named, owned, and audited.
- Linear chains where each step must follow the previous.
- Data consolidation flows where future barrier jobs should wait for multiple module outputs from the same batch generation.
- Environments with guaranteed headroom to meet N..M runs per day without fine‑grained parallelism.
- Teams prioritizing observability, security, and straightforward ops over maximum scheduling flexibility.

### When not to use it
- Arbitrary graph authoring, dynamic branching, or high fan-out workflow design.
- Broad historical backfill machinery across many independent paths.
- Full multi‑tenant workflows with interactive UIs and pluggable schedulers.

## Multi-input barrier model (planned, not yet implemented)

Some batch work needs to generate data from the results of multiple other modules. The LinearJC direction is to express this through registers, not duplicated task dependencies. The current implementation supports linear chains and fan-out only; the barrier model below is the planned evolution.

```yaml
# Conceptual example - this multi-input pattern is not yet accepted by the coordinator.
# Currently, each job can depend on at most one other job (linear chains only).

registry:
  source_sales:       {type: fs, path: /data/source/sales.csv, kind: file, protect: true}
  source_inventory:   {type: fs, path: /data/source/inventory.csv, kind: file, protect: true}
  sales_extract:      {type: temp, kind: file}
  inventory_extract:  {type: temp, kind: file}
  daily_report:       {type: fs, path: /data/reports/daily.csv, kind: file}

jobs:
  - id: extract.sales
    reads: [source_sales]
    writes: [sales_extract]

  - id: extract.inventory
    reads: [source_inventory]
    writes: [inventory_extract]

  - id: build.report
    reads: [sales_extract, inventory_extract]
    writes: [daily_report]
```

In this model, `build.report` would be a barrier job. It runs only after `sales_extract` and `inventory_extract` are both ready for the same batch generation. The coordinator would infer dependencies from register ownership rather than requiring explicit `depends:` links. See [ROADMAP.md](ROADMAP.md) for implementation priorities.

## Migrating from cron

For many cron‑driven jobs, migration is straightforward:
- Represent your cron script as a LinearJC job script.
- Set `min_daily` and `max_daily` to match frequency. Examples:
  - Once per day (`0 0 * * *`) → `min_daily: 1`, `max_daily: 1`
  - Every 30 minutes (`*/30 * * * *`) → `min_daily: 48`, `max_daily: 48`
- If your pipeline is multiple steps, place them in order as a linear chain.

Notes and caveats:
- The scheduler uses a sliding 24h window and aims to satisfy min/max counts; it does not guarantee an exact wall‑clock time. If strict time‑of‑day is required, use a small cron wrapper to publish a job request at that time, or add an external trigger.
- Executors pass environment and paths to scripts via `LINEARJC_*` variables and input/output directories; most cron scripts need little or no change.

## Architecture overview

- Coordinator (Python)
  - Discovers jobs, validates configurations, builds linear trees.
  - Schedules runs to satisfy min/max per‑day constraints.
  - Prepares inputs/outputs via MinIO pre‑signed URLs and publishes job requests over MQTT.
  - Structured logging with correlation IDs for end‑to‑end tracing.

- Executor (Rust)
  - Stateless worker that subscribes to signed job requests over MQTT.
  - Validates inputs strictly (paths, URLs, users, timeouts).
  - Runs job scripts with privilege separation (fork + setuid).
  - Optional process isolation: Landlock filesystem sandboxing, cgroups v2 resource limits, network namespace isolation.
  - Moves data using pre‑signed URLs; archives/extracts with safe libraries.
  - Designed to run inside KVM; workloads can run inside Docker within those VMs.

Transport and data
- Messaging: MQTT with HMAC‑SHA256 envelope signing and timestamp checks.
- Artifacts: MinIO via pre‑signed GET/PUT URLs; no long‑lived credentials on the wire.

## Security model (summary)
- Message integrity: HMAC‑SHA256 envelopes with max‑age enforcement.
- Principle of least privilege: executor user switching; secure work directory permissions.
- Process isolation (opt-in per job):
  - Landlock filesystem sandboxing (strict/relaxed/none modes).
  - cgroups v2 resource limits (CPU, memory, process count).
  - Network namespace isolation (optional network deny).
- Safe archives: library‑based tar.gz handling with path traversal and symlink checks.
- Defensive validation: strict checks for job IDs, registry keys, paths, URLs, users, and timeouts.
- Logging: correlation IDs and metrics; avoids logging secrets or pre‑signed URLs.

TLS for MQTT and MinIO is recommended for production. Example configs use development defaults; replace them before deployment.

## Status and scope
- Current scope: linear chains and fan-out with explicit `depends:` links. No general DAG semantics.
- Archive format: tar.gz.
- State: coordinator keeps scheduling state in memory; executor is stateless.
- This repository includes examples and scripts for local evaluation and tests.

### Current limitations
- **Fan-out duplicates root execution**: Each consuming tree runs its own copy of the shared root job. For expensive jobs (e.g., a 6-hour simulation), compute cost scales linearly with the number of consumers.
- **No generation scoping**: No guarantee that downstream jobs consume data from the same batch run. Partial failure + retry can mix fresh and stale data.
- **No atomic deployment**: `ljc deploy` pushes one package at a time. Coordinated updates across multiple jobs have a window of inconsistency.

The planned direction is register-driven barrier execution: the coordinator infers dependencies from register ownership (`reads:`/`writes:`), each job runs once per batch generation, and multi-input barriers emerge naturally. See [ROADMAP.md](ROADMAP.md) for priorities and motivation.

## Quick start (development)

Prerequisites
- Python 3.10+ (coordinator)
- Rust 1.70+ (executor)
- Mosquitto MQTT broker
- MinIO object storage (for data movement tests)

Install dependencies
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

Start services (development)
```bash
# Mosquitto (example - distro package)
mosquitto -v

# MinIO (example - download binary)
wget https://dl.min.io/server/minio/release/linux-amd64/minio -O ./minio
chmod +x ./minio
MINIO_ROOT_USER=minioadmin MINIO_ROOT_PASSWORD=minioadmin \
  ./minio server /tmp/minio-data --console-address ":9001"
```

Build and run executor (replace secret for production)
```bash
cd src/executor
cargo build --release
EXECUTOR_ID=executor-01 \
JOBS_DIR=$PWD/../../examples/executor-jobs \
WORK_DIR=/tmp/linearjc-executor \
MQTT_BROKER=localhost MQTT_PORT=1883 MQTT_SHARED_SECRET=dev-secret-change-in-production \
sudo ../../target/release/linearjc-executor
```

Run coordinator with example configuration (copy and edit .sample files)
```bash
# Copy example configs to /etc and adjust (secrets, paths)
sudo mkdir -p /etc/linearjc /etc/linearjc/executor /var/log/linearjc
sudo cp examples/config.yaml.sample /etc/linearjc/config.yaml
sudo cp examples/data_registry.yaml.sample /etc/linearjc/data_registry.yaml
sudo cp -r examples/jobs /etc/linearjc/

# Run coordinator
python src/coordinator_v2/main.py --config /etc/linearjc/config.yaml run
```

Notes
- Example configs include development defaults (`minioadmin`, a sample shared secret). Replace before production and enable TLS.

## Developer CLI (ljc)

The `ljc` tool manages job development and deployment:
- `ljc new <job>` - Create new job from template
- `ljc validate <job>` - Validate job configuration
- `ljc build <job>` - Package job for deployment
- `ljc deploy <package> --to <coordinator>` - Deploy to coordinator
- `ljc exec <job>` - Trigger immediate execution
- `ljc ps` - List active jobs
- `ljc logs <job>` - View execution history
- `ljc status <job>` - Check scheduling state
- `ljc tail <job>` - Follow job output

See `ljc --help` for full documentation.

## Documentation

- [Job Design Guide](docs/job-design-guide.md) - How to write job scripts and definitions

## Configuration

Coordinator configuration (`examples/config.yaml`) defines:
- `mqtt`: broker connection
- `minio`: endpoint and temp bucket for artifacts
- `jobs_dir`: directory of job YAMLs
- `data_registry`: logical names mapped to filesystem or MinIO locations
- `scheduling`: loop interval and message max age
- `signing`: shared secret for HMAC envelopes
- `logging`: level, optional JSON, and optional log file path
- `security`: allowed data roots; option to validate secrets (enable in production)
- `archive`: archive format (`tar.gz`)

Executors read environment variables:
- `MQTT_BROKER`, `MQTT_PORT`, `MQTT_SHARED_SECRET` - MQTT connection
- `MINIO_ENDPOINT`, `MINIO_SECURE` - MinIO connection (optional)
- `EXECUTOR_ID`, `JOBS_DIR`, `WORK_DIR` - executor identity and paths
- `CAPABILITIES` - comma-separated capability types (e.g., `pool`)

## Limitations
- Current execution model is chain-oriented; multi-input barrier jobs are a planned register-driven extension.
- No built‑in UI.
- MQTT and MinIO are external dependencies.
- Production deployments should enable TLS for MQTT and MinIO and provide strong secrets.

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the full prioritized roadmap with motivation.

## Project structure

```
linearjc/
├── src/
│   ├── coordinator/           # Python coordinator
│   ├── executor/              # Rust executor
│   └── linearjc-core/         # Rust shared library (signing, isolation, archive)
├── tools/
│   └── ljc/                   # Developer CLI (deploy, status, logs, exec)
├── tests/                     # Unit, integration, and E2E tests
└── examples/                  # Example configs and jobs
```

## License

MIT. See `LICENSE`.
