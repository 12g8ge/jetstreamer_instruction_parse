use std::{env, str::FromStr};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use iq_plugin::iq_session_plugin::IQSessionPlugin;
use jetstreamer::JetstreamerRunner;
use serde::Deserialize;
use serde_json::json;
use solana_pubkey::Pubkey;
use tokio::time::{sleep, Duration};

const SIGNATURE_PAGE_LIMIT: usize = 1_000;
const DEFAULT_MAX_SIGNATURE_PAGES: usize = 25;
const PAGE_DELAY_MS: u64 = 50;

#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "IQ Labs Jetstreamer runner",
    disable_help_subcommand = true
)]
struct CliArgs {
    /// Slot range or epoch specification (e.g. 123:456 or 890)
    #[arg(value_name = "SLOT_SPEC", conflicts_with = "session_pda")]
    slot_spec: Option<String>,

    /// Session PDA(s) to inspect; derive slot range via RPC
    #[arg(long = "session-pda", value_delimiter = ',', value_name = "PDA")]
    session_pda: Vec<String>,

    /// Solana RPC endpoint used for PDA signature lookup
    #[arg(long, value_name = "URL")]
    rpc_url: Option<String>,

    /// Slots of padding added before/after the discovered PDA range
    #[arg(long, default_value_t = 0, value_name = "SLOTS")]
    slot_padding: u64,

    /// Maximum number of requests per PDA when fetching signatures (each fetch pulls 1,000 signatures)
    #[arg(long, default_value_t = DEFAULT_MAX_SIGNATURE_PAGES, value_name = "REQUESTS")]
    max_signature_pages: usize,

    /// Firehose worker threads override (defaults to JETSTREAMER_THREADS or auto sizing)
    #[arg(long, value_name = "THREADS")]
    threads: Option<usize>,

    /// Log level passed to Jetstreamer (default info)
    #[arg(long, default_value = "info", value_name = "LEVEL")]
    log_level: String,
}

fn main() -> Result<()> {
    let args = CliArgs::parse();

    // Initialize logging before we perform any signature discovery so that
    // `log!` invocations inside helper utilities are visible even if Jetstreamer
    // reconfigures the logger later on.
    solana_logger::setup_with_default(&args.log_level);

    let plugin = IQSessionPlugin::new().context("failed to configure IQ session plugin")?;

    let slot_range = if !args.session_pda.is_empty() {
        let rpc_url = args
            .rpc_url
            .or_else(|| env::var("IQ_RPC_URL").ok())
            .or_else(|| env::var("SOLANA_RPC_URL").ok())
            .unwrap_or_else(|| "https://api.mainnet-beta.solana.com".to_string());

        let pdas = parse_pdas(&args.session_pda)?;
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("failed to create tokio runtime for slot discovery")?;

        let range = runtime.block_on(discover_slot_range(
            &rpc_url,
            &pdas,
            args.max_signature_pages,
        ))?;
        drop(runtime);

        let (min_slot, max_slot) = range.context("no transactions found for session PDA(s)")?;
        let padded_start = min_slot.saturating_sub(args.slot_padding);
        let padded_end_inclusive = max_slot.saturating_add(args.slot_padding);
        (padded_start, padded_end_inclusive.saturating_add(1))
    } else if let Some(spec) = args.slot_spec {
        parse_slot_spec(&spec)?
    } else {
        return Err(anyhow!(
            "provide either a slot/epoch spec or at least one --session-pda"
        ));
    };

    let threads = args.threads.unwrap_or_else(resolve_thread_count);

    let runner = JetstreamerRunner::new()
        .with_log_level(&args.log_level)
        .with_threads(threads)
        .with_slot_range_bounds(slot_range.0, slot_range.1)
        .with_plugin(Box::new(plugin));

    runner.run().map_err(anyhow::Error::from)?;

    Ok(())
}

fn resolve_thread_count() -> usize {
    env::var("JETSTREAMER_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or_else(jetstreamer_firehose::system::optimal_firehose_thread_count)
}

fn parse_slot_spec(spec: &str) -> Result<(u64, u64)> {
    if let Some((start, end)) = spec.split_once(':') {
        let start_slot: u64 = start.parse().context("failed to parse slot range start")?;
        let end_slot: u64 = end.parse().context("failed to parse slot range end")?;
        if start_slot > end_slot {
            return Err(anyhow!("slot range start must be <= end"));
        }
        Ok((start_slot, end_slot.saturating_add(1)))
    } else {
        let epoch: u64 = spec.parse().context("failed to parse epoch value")?;
        let (start_slot, end_slot_inclusive) =
            jetstreamer_firehose::epochs::epoch_to_slot_range(epoch);
        Ok((start_slot, end_slot_inclusive.saturating_add(1)))
    }
}

fn parse_pdas(raw: &[String]) -> Result<Vec<Pubkey>> {
    raw.iter()
        .map(|value| {
            Pubkey::from_str(value).with_context(|| format!("invalid session PDA '{}'", value))
        })
        .collect()
}

#[derive(Deserialize)]
struct SignatureRecord {
    signature: String,
    slot: u64,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

async fn discover_slot_range(
    rpc_url: &str,
    pdas: &[Pubkey],
    max_pages: usize,
) -> Result<Option<(u64, u64)>> {
    let client = reqwest::Client::builder().build()?;
    let mut overall_min: Option<u64> = None;
    let mut overall_max: Option<u64> = None;

    for pda in pdas {
        let (min_slot, max_slot) = fetch_bounds_for_pda(&client, rpc_url, pda, max_pages).await?;
        if let Some(min) = min_slot {
            overall_min = Some(match overall_min {
                Some(current) => current.min(min),
                None => min,
            });
        }
        if let Some(max) = max_slot {
            overall_max = Some(match overall_max {
                Some(current) => current.max(max),
                None => max,
            });
        }
    }

    match (overall_min, overall_max) {
        (Some(min), Some(max)) => Ok(Some((min, max))),
        _ => Ok(None),
    }
}

async fn fetch_bounds_for_pda(
    client: &reqwest::Client,
    rpc_url: &str,
    pda: &Pubkey,
    max_pages: usize,
) -> Result<(Option<u64>, Option<u64>)> {
    let mut before: Option<String> = None;
    let mut min_slot: Option<u64> = None;
    let mut max_slot: Option<u64> = None;
    let mut pages_fetched: usize = 0;
    let mut total_signatures: usize = 0;

    loop {
        if pages_fetched >= max_pages {
            log::warn!(
                "PDA {} signature history truncated at {} pages (oldest slot seen: {:?})",
                pda,
                max_pages,
                min_slot
            );
            break;
        }

        let mut params = json!([
            pda.to_string(),
            {
                "limit": SIGNATURE_PAGE_LIMIT,
                "commitment": "confirmed"
            }
        ]);
        if let Some(ref before_sig) = before {
            params[1]["before"] = json!(before_sig);
        }

        let response = client
            .post(rpc_url)
            .json(&json!({
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getSignaturesForAddress",
                "params": params,
            }))
            .send()
            .await
            .with_context(|| format!("failed to fetch signatures for {}", pda))?;

        let parsed: RpcResponse<Vec<SignatureRecord>> = response
            .error_for_status()?
            .json()
            .await
            .context("failed to decode RPC response")?;

        if let Some(error) = parsed.error {
            return Err(anyhow!(
                "RPC error {} for {}: {}",
                error.code,
                pda,
                error.message
            ));
        }

        let entries = parsed.result.unwrap_or_default();
        if entries.is_empty() {
            break;
        }
        pages_fetched += 1;
        total_signatures += entries.len();

        let newest_slot = entries.first().map(|r| r.slot).unwrap_or_default();
        let oldest_slot = entries.last().map(|r| r.slot).unwrap_or_default();
        log::debug!(
            "PDA {} page {} fetched {} signatures (slots {}..{})",
            pda,
            pages_fetched,
            entries.len(),
            oldest_slot,
            newest_slot
        );

        for record in &entries {
            min_slot = Some(min_slot.map(|m| m.min(record.slot)).unwrap_or(record.slot));
            max_slot = Some(max_slot.map(|m| m.max(record.slot)).unwrap_or(record.slot));
        }

        before = entries.last().map(|r| r.signature.clone());

        if entries.len() < SIGNATURE_PAGE_LIMIT {
            break;
        }

        // Avoid hammering public RPC endpoints; a short pause keeps us under burst limits.
        sleep(Duration::from_millis(PAGE_DELAY_MS)).await;
    }

    log::info!(
        "PDA {} signature sweep fetched {} page(s) / {} signatures (slots {:?}..{:?})",
        pda,
        pages_fetched,
        total_signatures,
        min_slot,
        max_slot
    );

    Ok((min_slot, max_slot))
}
