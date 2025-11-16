## LinearJC

A small, mainframe‑style job controller for linear workflows. It runs jobs in strict order, with clear auditability and simple operations. It is designed for environments that have enough capacity to meet daily targets without parallelizing every step.

### What it solves
- Deterministic, in‑order execution for easy auditing and debugging.
- Min/max executions per 24 hours using a sliding window (not cron).
- Stateless executors with signed messaging and pre‑signed artifact transfers.
- Simple single‑server or modest distributed setups without a heavy control plane.
- Isolation where executors run inside KVM and can execute workloads in Docker inside those VMs.

This is intentionally simpler than general DAG platforms (Airflow, Prefect, Dagu). If you need complex branching, backfills across multiple paths, or a rich UI, those tools may be a better fit. If you want predictable linear chains with low operational overhead, LinearJC is appropriate.

### When to use it
- Linear pipelines where each step must follow the previous.
- Environments with guaranteed headroom to meet N..M runs per day without fine‑grained parallelism.
- Teams prioritizing observability, security, and straightforward ops over maximum scheduling flexibility.

### When not to use it
- Complex DAGs, backfills, or high fan‑out pipelines.
- Full multi‑tenant workflows with interactive UIs and pluggable schedulers.

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
  - Moves data using pre‑signed URLs; archives/extracts with safe libraries.
  - Designed to run inside KVM; workloads can run inside Docker within those VMs.

Transport and data
- Messaging: MQTT with HMAC‑SHA256 envelope signing and timestamp checks.
- Artifacts: MinIO via pre‑signed GET/PUT URLs; no long‑lived credentials on the wire.

## Security model (summary)
- Message integrity: HMAC‑SHA256 envelopes with max‑age enforcement.
- Principle of least privilege: executor user switching; secure work directory permissions.
- Safe archives: library‑based tar.gz handling with path traversal checks.
- Defensive validation: strict checks for job IDs, registry keys, paths, URLs, users, and timeouts.
- Logging: correlation IDs and metrics; avoids logging secrets or pre‑signed URLs.

TLS for MQTT and MinIO is recommended for production. Example configs use development defaults; replace them before deployment.

## Status and scope
- Scope: linear chains only. No general DAG semantics.
- Archive format: tar.gz.
- State: coordinator keeps scheduling state in memory; executor is stateless.
- This repository includes examples and scripts for local evaluation and tests.

## Quick start (development)

Prerequisites
- Python 3.10+ (coordinator)
- Rust 1.70+ (executor)
- Mosquitto MQTT broker
- MinIO object storage (for data movement tests)

Install dependencies
```bash
cd .
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
python src/coordinator/main.py --config /etc/linearjc/config.yaml
```

Notes
- Example configs include development defaults (`minioadmin`, a sample shared secret). Replace before production and enable TLS.

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

Executors read environment variables for `MQTT_BROKER`, `MQTT_PORT`, and `MQTT_SHARED_SECRET`, and paths for `JOBS_DIR` and `WORK_DIR`.

## Limitations
- Linear execution only; a single path per pipeline.
- No built‑in UI.
- MQTT and MinIO are external dependencies.
- Production deployments should enable TLS for MQTT and MinIO and provide strong secrets.

## Roadmap (non‑binding)
- Optional retries with backoff for artifact transfers.
- TLS configuration examples for MQTT and MinIO.
- Systemd unit examples and log rotation guidance.

## Project structure

```
linearjc/
├── src/
│   ├── coordinator/           # Python coordinator
│   └── executor/              # Rust executor
└── examples/                  # Example configs and jobs
```

## License

MIT. See `LICENSE`.
