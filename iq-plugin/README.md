# IQ Session Plugin

The `iq-plugin` crate extends Jetstreamer with an IQ Labs session collector. It watches
transactions flowing through Project Yellowstone's firehose, reconstructs hybrid session
uploads (Pinocchio or legacy), and forwards the fully reassembled payloads to the IQ Labs
reader API.

## Environment

Set the following variables before starting the runner:

- `IQ_PROGRAM_ID` – Base58 program id(s) for the IQ protocol. Accepts a single id or a
  comma-separated list. Optional when you only care about a fixed set of session PDAs.  
- `IQ_SESSION_PDA_LIST` – Comma-separated list of session PDA addresses to focus on. When
  this list is non-empty, the plugin ignores sessions outside the list even if the program
  id matches. Optional.
- `IQ_READER_MODE` – `pinocchio`, `legacy`, or `passthrough`. The default is
  `pinocchio`; `passthrough` attempts both parsers.
- `IQ_READER_BASE_URL` – Base URL for the reader backend (default:
  `https://api.iqlabs.xyz`).
- `IQ_READER_ENDPOINT_PATH` – Relative path that receives reconstructed payloads (default:
  `/api/v2/inscribe/ingest`).

The runner automatically loads variables from a local `.env` file when present, so you can
switch configurations by editing that file instead of exporting values manually.

To prep your shell with both the `.env` values and Homebrew paths for OpenSSL/LLVM, run:

```bash
./scripts/iq-env.sh
```

This script exports the required toolchain variables and then sources `iq-plugin/.env`.

## Running the collector

Replay an explicit slot range from Old Faithful (release build recommended):

```bash
JETSTREAMER_THREADS=8 \
IQ_PROGRAM_ID=<program> \
IQ_READER_BASE_URL=https://api.iqlabs.xyz \
IQ_READER_ENDPOINT_PATH=/api/v2/inscribe/ingest \
cargo run -p iq-plugin --bin iq_runner --release -- 358560000:367631999
```

To stream continuously from the most recent finalized slot, pass `-- tail` after the slot
range arguments or rely on Jetstreamer’s default CLI behaviour. Jetstreamer CLI flags still
apply; see `cargo run -- --help` in the workspace root for details.

### Discovering slot ranges from session PDAs

The runner can derive the slot window automatically by querying the Solana RPC for a session
PDA’s signature history:

```bash
source scripts/iq-env.sh
cargo run -p iq-plugin --bin iq_runner --release -- \
  --session-pda F6wEXAMPLEPDAXXXXXXXXXXXXXXXX
```

For a one-liner that handles the env bootstrap automatically:

```bash
scripts/run-session.sh F6wEXAMPLEPDAXXXXXXXXXXXXXXXX --slot-padding 5
```

You can pass multiple PDAs (comma-delimited), provide a custom endpoint via `--rpc-url` (or
`IQ_RPC_URL`), and extend the replay window with `--slot-padding <slots>`. The command fetches
`getSignaturesForAddress` pages (1,000 signatures per request) up to `--max-signature-pages`
before handing the minimal covering `[start, end)` range to Jetstreamer.

## Parser modes

- **Pinocchio** – Interprets single-byte discriminators from the Pinocchio program. Chunk
  payloads marked with method `0` are base64-decoded before reassembly.
- **Legacy** – Handles the 8-byte discriminator layout used by the original hybrid V2
  contract (Borsh string chunks with optional method byte).
- **Passthrough** – Attempts Pinocchio parsing first, falling back to the legacy decoder.

Only fully reconstructed sessions (all chunk indexes present and a finalize event observed)
are sent to the backend. Each upload is base64-encoded in the HTTP payload and includes the
session PDA, optional session id hex, chunk counts, compression method, and merkle root when
available.

## Backend expectations

The plugin never writes credentials or hostnames to disk. Backend URLs must come from the
environment. Failed HTTP requests are logged but do not terminate Jetstreamer; fix the
configuration and rerun if you need to backfill missed uploads.
