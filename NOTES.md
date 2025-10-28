# Jetstreamer IQ Session Pipeline Notes

## Overview
- Added a dedicated `iq-plugin` crate implementing `IQSessionPlugin` to stream chunked Pinocchio sessions from Old Faithful.
- Runner binary (`iq_runner`) loads ClickHouse/JETSTREAMER env, fetches slot ranges (via RPC signatures or PDA list), streams target slots, reconstructs payload, POSTs to backend ingest endpoint.
- ClickHouse interactions now point to an external helper service to avoid per-run startup cost.

## Environment Expectations
- `.env` in repo root (`iq-plugin/.env`) exports IQ-specific settings:
  - `IQ_PROGRAM_ID` (single or comma-separated), `IQ_SESSION_PDA_LIST` (when set, only these sessions are processed), `IQ_READER_MODE`
  - Reader backend: `IQ_READER_BASE_URL`, `IQ_READER_ENDPOINT_PATH`
  - ClickHouse mode: `JETSTREAMER_CLICKHOUSE_MODE=remote`, `JETSTREAMER_CLICKHOUSE_DSN=http://ipaddress:8123`
  - Runner log control can be set via `RUST_LOG` in this env.
- `scripts/iq-env.sh` sources the env + sets toolchain vars; use `source scripts/iq-env.sh` before running commands.

## Running Jetstreamer for a session
- `scripts/run-session.sh <PDA>[,<PDA>...] [--slot-padding N --max-signature-pages M]`
  - Prepares env (via `iq-env.sh`) and runs `cargo run -p iq-plugin --bin iq_runner --release`.
  - Automatically sets `--rpc-url https://api.mainnet-beta.solana.com` unless overridden.
- Useful options:
  - Increase `--slot-padding` (e.g. 200) to ensure the full chunk + finalize window is streamed.
  - Raise `--max-signature-pages` when the PDA has many chunks (each page fetches 1000 signatures).
  - Enable debug logging: `export RUST_LOG=iq_plugin=debug` in your shell or `.env`.
- Successful run: logs `uploaded session ... (chunks, bytes) to http://secret:8080/api/v2/inscribe/ingest`.

## Backend ingest endpoint
- `production-api/iqlabs-core-api` exposes `POST /api/v2/inscribe/ingest`:
  - Accepts payload from the plugin (`session_pubkey`, `payload_base64`, chunk counts, slot metadata).
  - Writes reconstruct payload to `IQ_INGEST_OUTPUT_DIR` (default `backend/ingest_cache`).
  - Updates any matching `ManagedUpload` / `ManagedUploadRegistry` rows (status -> `COMPLETED`).
- Ensure `backend/.env` includes `IQ_INGEST_OUTPUT_DIR` so files land under the desired directory.

## ClickHouse helper service
- New `clickhouse-service/` microservice replicates the embedded helper but keeps the server always running:
  - Binaries: drop the Jetstreamer build output into `clickhouse-service/bin/clickhouse`.
  - Config: `clickhouse-service/config/clickhouse-local-config.xml` (HTTP 8123 / TCP 9000).
  - Data/log directories: `clickhouse-service/data/`, `clickhouse-service/logs/` (`.gitkeep` present).
  - Runner: `python3 clickhouse-service/clickhouse_runner.py` (or add to PM2: `pm2 start ./clickhouse-service/clickhouse_runner.py --name iq-clickhouse`).
- Jetstreamer env: `JETSTREAMER_CLICKHOUSE_MODE=remote`, `JETSTREAMER_CLICKHOUSE_DSN=http://secret:8123`.
- If logs are needed, update the ClickHouse config `<logger>` section to set `<log>`/`<errorlog>` paths.

## Tips & Troubleshooting
- If plugin logs `finalized with ... expected chunks (currently have X)` and never POSTs, increase padding/pages or verify that the missing chunk instructions exist (session PDA must be in `IQ_SESSION_PDA_LIST`).
- To enable debug logging permanently, add `RUST_LOG=iq_plugin=debug` to `iq-plugin/.env`.
- Ensure the ClickHouse service is running before Jetstreamer (curl redacted/ping -> `Ok.`).
- The `scripts/run-session.sh` wrapper appends `--session-pda`, so make sure additional flags go after the PDA argument.

## Key Commands Recap
- Source env: `source scripts/iq-env.sh`
- Run session: `scripts/run-session.sh <PDA> --slot-padding 200 --max-signature-pages 100`
- Enable debug logs (once): `export RUST_LOG=iq_plugin=debug`
- Start ClickHouse helper: `python3 clickhouse-service/clickhouse_runner.py`
- Stop helper: `pkill -f clickhouse_runner.py` and `pkill -f clickhouse-service/bin/clickhouse`
- Ingest backend directory: check files under `backend/ingest_cache` (or custom `IQ_INGEST_OUTPUT_DIR`).
