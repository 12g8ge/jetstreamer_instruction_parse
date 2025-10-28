use std::{
    collections::{BTreeMap, HashSet},
    env,
    fs::OpenOptions,
    io::{BufRead, BufReader, ErrorKind, Write},
    str::FromStr,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use anyhow::{anyhow, Context, Result};
use base64::prelude::{Engine as _, BASE64_STANDARD};
use clickhouse::Client as ClickhouseClient;
use dashmap::DashMap;
use dotenvy::dotenv;
use jetstreamer_firehose::firehose::TransactionData;
use jetstreamer_plugin::{Plugin, PluginFuture};
use log::{debug, error, info, warn};
use reqwest::Client as HttpClient;
use serde::Serialize;
use serde_json::json;
use solana_message::{compiled_instruction::CompiledInstruction, VersionedMessage};
use solana_pubkey::Pubkey;

const PINOCCHIO_CHUNK_DISCRIMINATOR: u8 = 0x04;
const PINOCCHIO_FINALIZE_DISCRIMINATOR: u8 = 0x05;
const LEGACY_CHUNK_DISCRIMINATORS: [[u8; 8]; 3] = [
    [0xac, 0xc6, 0x2c, 0x43, 0xcd, 0xf6, 0x46, 0x0c],
    [0x04, 0xf7, 0x8d, 0x73, 0x20, 0xc3, 0x37, 0x48],
    [0x04, 0x4b, 0xaf, 0xd7, 0xc2, 0x5b, 0xed, 0x4f],
];
const LEGACY_FINALIZE_HYBRID: [u8; 8] = [0x18, 0xda, 0xbe, 0xb1, 0xe4, 0x53, 0x7e, 0x7b];
const LEGACY_FINALIZE_BUNDLE: [u8; 8] = [0x23, 0xa3, 0xd3, 0xb1, 0x3f, 0x5d, 0xdb, 0x8a];
const BACKFILL_HINTS_FILE: &str = "missing_sessions.log";

#[derive(Clone)]
struct PluginConfig {
    program_ids: HashSet<Pubkey>,
    session_pdas: HashSet<Pubkey>,
    reader_mode: ReaderMode,
    endpoint_url: String,
}

#[derive(Default)]
struct PluginStats {
    uploaded_sessions: AtomicUsize,
    uploaded_chunks: AtomicU64,
    uploaded_bytes: AtomicU64,
    failed_uploads: AtomicUsize,
}

impl PluginConfig {
    fn from_env() -> Result<Self> {
        let program_ids = env::var("IQ_PROGRAM_ID")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .filter_map(|entry| {
                        let trimmed = entry.trim();
                        if trimmed.is_empty() {
                            return None;
                        }
                        match Pubkey::from_str(trimmed) {
                            Ok(pk) => Some(pk),
                            Err(err) => {
                                warn!(
                                    "ignored malformed program id '{}' (IQ_PROGRAM_ID): {}",
                                    trimmed, err
                                );
                                None
                            }
                        }
                    })
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        let reader_mode = ReaderMode::from_env(env::var("IQ_READER_MODE").ok());

        let session_pdas = env::var("IQ_SESSION_PDA_LIST")
            .ok()
            .map(|raw| {
                raw.split(',')
                    .filter_map(|entry| {
                        let trimmed = entry.trim();
                        if trimmed.is_empty() {
                            return None;
                        }
                        match Pubkey::from_str(trimmed) {
                            Ok(pk) => Some(pk),
                            Err(err) => {
                                warn!(
                                    "ignored malformed session PDA '{}' ({}): {}",
                                    trimmed, "IQ_SESSION_PDA_LIST", err
                                );
                                None
                            }
                        }
                    })
                    .collect::<HashSet<_>>()
            })
            .unwrap_or_default();

        let base_url =
            env::var("IQ_READER_BASE_URL").unwrap_or_else(|_| "https://api.iqlabs.xyz".to_string());
        if base_url.trim().is_empty() {
            return Err(anyhow!("IQ_READER_BASE_URL cannot be empty"));
        }

        let endpoint_path = env::var("IQ_READER_ENDPOINT_PATH")
            .unwrap_or_else(|_| "/api/v2/inscribe/ingest".to_string());
        if endpoint_path.trim().is_empty() {
            return Err(anyhow!("IQ_READER_ENDPOINT_PATH cannot be empty"));
        }

        let endpoint_url = format!(
            "{}/{}",
            base_url.trim_end_matches('/'),
            endpoint_path.trim_start_matches('/')
        );

        Ok(Self {
            program_ids,
            session_pdas,
            reader_mode,
            endpoint_url,
        })
    }
}

#[derive(Clone, Copy, Debug)]
enum ReaderMode {
    Pinocchio,
    Legacy,
    Passthrough,
}

impl ReaderMode {
    fn from_env(raw: Option<String>) -> Self {
        match raw.as_deref() {
            Some(mode) if mode.eq_ignore_ascii_case("legacy") => Self::Legacy,
            Some(mode) if mode.eq_ignore_ascii_case("passthrough") => Self::Passthrough,
            _ => Self::Pinocchio,
        }
    }

    fn as_str(&self) -> &'static str {
        match self {
            ReaderMode::Pinocchio => "pinocchio",
            ReaderMode::Legacy => "legacy",
            ReaderMode::Passthrough => "passthrough",
        }
    }

    fn parse_chunk(&self, raw: &[u8]) -> Option<ParsedChunk> {
        match self {
            ReaderMode::Pinocchio => parse_pinocchio_chunk(raw),
            ReaderMode::Legacy => parse_legacy_chunk(raw),
            ReaderMode::Passthrough => {
                parse_pinocchio_chunk(raw).or_else(|| parse_legacy_chunk(raw))
            }
        }
    }

    fn parse_finalize(&self, raw: &[u8]) -> Option<FinalizeData> {
        match self {
            ReaderMode::Pinocchio => parse_pinocchio_finalize(raw),
            ReaderMode::Legacy => parse_legacy_finalize(raw),
            ReaderMode::Passthrough => {
                parse_pinocchio_finalize(raw).or_else(|| parse_legacy_finalize(raw))
            }
        }
    }
}

#[derive(Debug, Default)]
struct SessionAccumulator {
    session_id: Option<[u8; 16]>,
    total_chunks: Option<u32>,
    merkle_root: Option<[u8; 32]>,
    chunks: BTreeMap<u32, Vec<u8>>,
    method: Option<u8>,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    finalize_slot: Option<u64>,
    finalize_signature: Option<String>,
    min_chunk_seen: Option<u32>,
    max_chunk_seen: Option<u32>,
}

impl SessionAccumulator {
    fn reset(&mut self) {
        self.session_id = None;
        self.total_chunks = None;
        self.merkle_root = None;
        self.chunks.clear();
        self.method = None;
        self.first_slot = None;
        self.last_slot = None;
        self.finalize_slot = None;
        self.finalize_signature = None;
        self.min_chunk_seen = None;
        self.max_chunk_seen = None;
    }

    fn attempt_assemble(&self) -> Option<AssembledSession> {
        let total = self.total_chunks?;
        if self.chunks.len() != total as usize {
            return None;
        }
        let mut payload = Vec::new();
        for index in 0..total {
            let chunk = self.chunks.get(&index)?;
            payload.extend_from_slice(chunk);
        }
        Some(AssembledSession {
            session_id: self.session_id,
            total_chunks: total,
            merkle_root: self.merkle_root,
            payload,
            method: self.method,
            first_slot: self.first_slot,
            last_slot: self.last_slot,
            finalize_slot: self.finalize_slot,
            finalize_signature: self.finalize_signature.clone(),
            chunk_count: self.chunks.len(),
        })
    }
}

#[derive(Debug)]
struct ParsedChunk {
    session_id: Option<[u8; 16]>,
    chunk_index: u32,
    payload: Vec<u8>,
    method: Option<u8>,
}

#[derive(Debug)]
struct FinalizeData {
    session_id: Option<[u8; 16]>,
    total_chunks: u32,
    merkle_root: Option<[u8; 32]>,
}

struct AssembledSession {
    session_id: Option<[u8; 16]>,
    total_chunks: u32,
    merkle_root: Option<[u8; 32]>,
    payload: Vec<u8>,
    method: Option<u8>,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    finalize_slot: Option<u64>,
    finalize_signature: Option<String>,
    chunk_count: usize,
}

#[derive(Serialize)]
struct UploadRequest {
    session_pubkey: String,
    session_id: Option<String>,
    total_chunks: u32,
    chunk_count: usize,
    payload_base64: String,
    merkle_root: Option<String>,
    reader_mode: &'static str,
    first_slot: Option<u64>,
    last_slot: Option<u64>,
    finalize_slot: Option<u64>,
    finalize_signature: Option<String>,
    compression_method: Option<u8>,
}

/// Streams Solana transactions and reconstructs IQ Labs session uploads.
pub struct IQSessionPlugin {
    config: Arc<PluginConfig>,
    client: HttpClient,
    sessions: Arc<DashMap<Pubkey, SessionAccumulator>>,
    stats: Arc<PluginStats>,
    recorded_backfills: Arc<Mutex<HashSet<(Pubkey, Vec<u32>)>>>,
}

impl IQSessionPlugin {
    /// Builds a new plugin instance using environment configuration.
    pub fn new() -> Result<Self> {
        if let Err(err) = dotenv() {
            if !matches!(err, dotenvy::Error::Io(ref io) if io.kind() == ErrorKind::NotFound) {
                warn!("failed to load .env file: {}", err);
            }
        }
        let config = Arc::new(PluginConfig::from_env()?);
        let client = HttpClient::builder().build()?;
        let sessions = Arc::new(DashMap::new());
        let stats = Arc::new(PluginStats::default());

        // Load existing backfill hints to avoid duplicates
        let recorded_backfills = Arc::new(Mutex::new(load_existing_backfills()));

        Ok(Self {
            config,
            client,
            sessions,
            stats,
            recorded_backfills,
        })
    }
}

impl Plugin for IQSessionPlugin {
    fn name(&self) -> &'static str {
        "iq-session"
    }

    fn on_load(&self, _db: Option<Arc<ClickhouseClient>>) -> PluginFuture<'_> {
        let config = self.config.clone();
        Box::pin(async move {
            let program_summary = if config.program_ids.is_empty() {
                "<any>".to_string()
            } else {
                config
                    .program_ids
                    .iter()
                    .map(|pk| pk.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            };
            info!(
                "IQ Session Plugin loaded | mode={} endpoint={} program_ids={} session_filters={}",
                config.reader_mode.as_str(),
                config.endpoint_url,
                program_summary,
                config.session_pdas.len()
            );
            Ok(())
        })
    }

    fn on_transaction<'a>(
        &'a self,
        thread_id: usize,
        _db: Option<Arc<ClickhouseClient>>,
        transaction: &'a TransactionData,
    ) -> PluginFuture<'a> {
        let config = self.config.clone();
        let client = self.client.clone();
        let sessions = self.sessions.clone();
        let stats = self.stats.clone();
        let recorded_backfills = self.recorded_backfills.clone();
        Box::pin(async move {
            if let Err(err) =
                process_transaction(thread_id, &config, &client, &sessions, &stats, &recorded_backfills, transaction)
                    .await
            {
                error!(
                    "thread {} failed to process transaction {} in slot {}: {}",
                    thread_id, transaction.signature, transaction.slot, err
                );
            }
            Ok(())
        })
    }

    fn on_exit(&self, _db: Option<Arc<ClickhouseClient>>) -> PluginFuture<'_> {
        let sessions = self.sessions.clone();
        let stats = self.stats.clone();
        let recorded_backfills = self.recorded_backfills.clone();
        Box::pin(async move {
            let uploaded_sessions = stats.uploaded_sessions.load(Ordering::Relaxed);
            let uploaded_chunks = stats.uploaded_chunks.load(Ordering::Relaxed);
            let uploaded_bytes = stats.uploaded_bytes.load(Ordering::Relaxed);
            let failed_uploads = stats.failed_uploads.load(Ordering::Relaxed);
            info!(
                "IQ Session Plugin summary | uploaded_sessions={} uploaded_chunks={} uploaded_bytes={} failed_uploads={}",
                uploaded_sessions, uploaded_chunks, uploaded_bytes, failed_uploads
            );
            if !sessions.is_empty() {
                warn!("{} sessions still buffered at shutdown", sessions.len());
                for entry in sessions.iter() {
                    let session = entry.key();
                    let acc = entry.value();
                    if let Some(total) = acc.total_chunks {
                        let missing = missing_chunk_indexes(&acc, total);
                        warn!(
                            "buffered session {} | chunks_observed={} total_expected={} first_slot={:?} last_slot={:?} finalize_slot={:?} min_chunk_seen={:?} max_chunk_seen={:?} missing_indexes_preview={}",
                            session,
                            acc.chunks.len(),
                            total,
                            acc.first_slot,
                            acc.last_slot,
                            acc.finalize_slot,
                            acc.min_chunk_seen,
                            acc.max_chunk_seen,
                            format_missing_preview(&missing)
                        );
                        if !missing.is_empty() {
                            info!(
                            "session {} missing chunk details: total_missing={} missing_list={:?}",
                            session,
                            missing.len(),
                            missing
                        );
                            record_backfill_hint(entry.key(), acc, Some(total), &missing, &recorded_backfills);
                        }
                    } else {
                        warn!(
                        "buffered session {} | chunks_observed={} total_expected=<unknown> first_slot={:?} last_slot={:?} finalize_slot={:?} min_chunk_seen={:?} max_chunk_seen={:?}",
                        session,
                            acc.chunks.len(),
                            acc.first_slot,
                            acc.last_slot,
                            acc.finalize_slot,
                            acc.min_chunk_seen,
                            acc.max_chunk_seen
                        );
                    }
                }
            }
            Ok(())
        })
    }
}

async fn process_transaction(
    thread_id: usize,
    config: &PluginConfig,
    client: &HttpClient,
    sessions: &Arc<DashMap<Pubkey, SessionAccumulator>>,
    stats: &Arc<PluginStats>,
    recorded_backfills: &Arc<Mutex<HashSet<(Pubkey, Vec<u32>)>>>,
    transaction: &TransactionData,
) -> Result<()> {
    let (account_keys, instructions): (Vec<Pubkey>, &Vec<CompiledInstruction>) = match &transaction
        .transaction
        .message
    {
        VersionedMessage::Legacy(message) => (message.account_keys.clone(), &message.instructions),
        VersionedMessage::V0(message) => {
            let mut keys = message.account_keys.clone();
            keys.extend(
                transaction
                    .transaction_status_meta
                    .loaded_addresses
                    .writable
                    .iter()
                    .copied(),
            );
            keys.extend(
                transaction
                    .transaction_status_meta
                    .loaded_addresses
                    .readonly
                    .iter()
                    .copied(),
            );
            (keys, &message.instructions)
        }
    };

    let mut processed = false;

    for instruction in instructions {
        let program_key = account_keys
            .get(instruction.program_id_index as usize)
            .copied();
        let program_matches = config.program_ids.is_empty()
            || program_key
                .map(|key| config.program_ids.contains(&key))
                .unwrap_or(false);

        if !program_matches {
            continue;
        }

        let session_matches = if config.session_pdas.is_empty() {
            false
        } else {
            instruction.accounts.iter().any(|idx| {
                account_keys
                    .get(*idx as usize)
                    .map(|key| config.session_pdas.contains(key))
                    .unwrap_or(false)
            })
        };

        if !config.session_pdas.is_empty() && !session_matches {
            continue;
        }

        let data = instruction.data.as_slice();

        let mut handled = false;

        if let Some(chunk) = config.reader_mode.parse_chunk(data) {
            processed = true;
            handle_chunk(
                config,
                client,
                sessions,
                stats,
                &account_keys,
                instruction,
                transaction,
                chunk,
            )
            .await?;
            handled = true;
        } else if let Some(finalize) = config.reader_mode.parse_finalize(data) {
            processed = true;
            handle_finalize(
                config,
                client,
                sessions,
                stats,
                recorded_backfills,
                &account_keys,
                instruction,
                transaction,
                finalize,
            )
            .await?;
            handled = true;
        }

        if !handled {
            if let Some((session_bytes, chunk_index, method)) = inspect_pinocchio_metadata(data) {
                info!(
                    "ignored pin chunk in slot {} | chunk={} method={} session={} program_match={} session_match={} data_len={}",
                    transaction.slot,
                    chunk_index,
                    method,
                    hex::encode(session_bytes),
                    program_matches,
                    session_matches,
                    data.len()
                );
                if let Err(err) = append_ignored_chunk_log(
                    transaction.slot,
                    chunk_index,
                    method,
                    &session_bytes,
                    program_key,
                    data,
                ) {
                    debug!("failed to persist ignored chunk log: {}", err);
                }
            } else {
                let prefix = data
                    .iter()
                    .take(16)
                    .map(|byte| format!("{:02x}", byte))
                    .collect::<Vec<_>>()
                    .join("");
                debug!(
                    "ignored instruction in slot {} for program {:?} | data_len={} prefix={} accounts={:?}",
                    transaction.slot,
                    program_key,
                    data.len(),
                    prefix,
                    instruction.accounts
                );
            }
        }
    }

    if processed {
        debug!(
            "thread {} processed transaction {} in slot {}",
            thread_id, transaction.signature, transaction.slot
        );
    }

    Ok(())
}

async fn handle_chunk(
    config: &PluginConfig,
    client: &HttpClient,
    sessions: &Arc<DashMap<Pubkey, SessionAccumulator>>,
    stats: &Arc<PluginStats>,
    account_keys: &[Pubkey],
    instruction: &CompiledInstruction,
    transaction: &TransactionData,
    chunk: ParsedChunk,
) -> Result<()> {
    let Some(session_account) =
        extract_session_account(account_keys, instruction, &config.session_pdas)
    else {
        debug!(
            "skipped chunk in slot {} (unable to identify session account)",
            transaction.slot
        );
        return Ok(());
    };

    if !config.session_pdas.is_empty() && !config.session_pdas.contains(&session_account) {
        debug!(
            "skipping chunk for untracked session {} in slot {}",
            session_account, transaction.slot
        );
        return Ok(());
    }

    let assembled = {
        let mut entry = sessions
            .entry(session_account)
            .or_insert_with(SessionAccumulator::default);

        if let Some(incoming_id) = chunk.session_id {
            if let Some(existing_id) = entry.session_id {
                if existing_id != incoming_id {
                    warn!(
                        "session {} saw new session id {}, resetting previous buffer",
                        session_account,
                        hex::encode(incoming_id)
                    );
                    entry.reset();
                }
            }
            entry.session_id = Some(incoming_id);
        }

        if entry.first_slot.is_none() {
            entry.first_slot = Some(transaction.slot);
        }
        entry.last_slot = Some(transaction.slot);

        if let Some(method) = chunk.method {
            match entry.method {
                None => entry.method = Some(method),
                Some(existing) if existing != method => {
                    warn!(
                        "session {} changed compression method from {} to {}",
                        session_account, existing, method
                    );
                    entry.method = Some(method);
                }
                _ => {}
            }
        }

        if entry.chunks.contains_key(&chunk.chunk_index) {
            debug!(
                "session {} replacing chunk {} (duplicate) in slot {}",
                session_account, chunk.chunk_index, transaction.slot
            );
        }
        entry.chunks.insert(chunk.chunk_index, chunk.payload);

        if chunk.chunk_index < 64 {
            debug!(
                "session {} stored early chunk {} in slot {}",
                session_account, chunk.chunk_index, transaction.slot
            );
        }

        if (2500..2600).contains(&chunk.chunk_index) {
            info!(
                "session {} stored mid-range chunk {} in slot {}",
                session_account, chunk.chunk_index, transaction.slot
            );
        }

        entry.min_chunk_seen = Some(
            entry
                .min_chunk_seen
                .map(|current| current.min(chunk.chunk_index))
                .unwrap_or(chunk.chunk_index),
        );
        entry.max_chunk_seen = Some(
            entry
                .max_chunk_seen
                .map(|current| current.max(chunk.chunk_index))
                .unwrap_or(chunk.chunk_index),
        );

        if entry.total_chunks.is_none() {
            debug!(
                "session {} chunk {} stored ({} total so far)",
                session_account,
                chunk.chunk_index,
                entry.chunks.len()
            );
        }

        entry.attempt_assemble()
    };

    if let Some(mut assembled) = assembled {
        if assembled.finalize_slot.is_none() {
            assembled.finalize_slot = Some(transaction.slot);
        }
        if assembled.finalize_signature.is_none() {
            assembled.finalize_signature = Some(transaction.signature.to_string());
        }
        sessions.remove(&session_account);
        if let Err(err) = send_upload(client, config, stats, session_account, assembled).await {
            error!(
                "failed to send reconstructed session {} to backend: {}",
                session_account, err
            );
            stats.failed_uploads.fetch_add(1, Ordering::Relaxed);
        }
    }

    Ok(())
}

async fn handle_finalize(
    config: &PluginConfig,
    client: &HttpClient,
    sessions: &Arc<DashMap<Pubkey, SessionAccumulator>>,
    stats: &Arc<PluginStats>,
    recorded_backfills: &Arc<Mutex<HashSet<(Pubkey, Vec<u32>)>>>,
    account_keys: &[Pubkey],
    instruction: &CompiledInstruction,
    transaction: &TransactionData,
    finalize: FinalizeData,
) -> Result<()> {
    let Some(session_account) =
        extract_session_account(account_keys, instruction, &config.session_pdas)
    else {
        warn!(
            "finalize instruction missing identifiable session account in slot {}",
            transaction.slot
        );
        return Ok(());
    };

    if !config.session_pdas.is_empty() && !config.session_pdas.contains(&session_account) {
        debug!(
            "skipping finalize for untracked session {} in slot {}",
            session_account, transaction.slot
        );
        return Ok(());
    }

    let assembled = {
        let mut entry = sessions
            .entry(session_account)
            .or_insert_with(SessionAccumulator::default);

        if let Some(incoming_id) = finalize.session_id {
            if let Some(existing_id) = entry.session_id {
                if existing_id != incoming_id {
                    warn!(
                        "session {} finalize revealed new session id {}, resetting previous buffer",
                        session_account,
                        hex::encode(incoming_id)
                    );
                    entry.reset();
                }
            }
            entry.session_id = Some(incoming_id);
        }

        entry.total_chunks = Some(finalize.total_chunks);
        if let Some(root) = finalize.merkle_root {
            entry.merkle_root = Some(root);
        }
        entry.finalize_slot = Some(transaction.slot);
        entry.finalize_signature = Some(transaction.signature.to_string());

        entry.attempt_assemble()
    };

    if let Some(assembled) = assembled {
        sessions.remove(&session_account);
        if let Err(err) = send_upload(client, config, stats, session_account, assembled).await {
            error!(
                "failed to send reconstructed session {} after finalize: {}",
                session_account, err
            );
            stats.failed_uploads.fetch_add(1, Ordering::Relaxed);
        }
    } else {
        let observed = sessions
            .get(&session_account)
            .map(|entry| {
                let acc = entry.value();
                let missing = missing_chunk_indexes(acc, finalize.total_chunks);
                if !missing.is_empty() {
                    warn!(
                        "session {} finalized with {} expected chunks (currently have {}) | missing indexes: {} | min_chunk_seen={:?} max_chunk_seen={:?}",
                        session_account,
                        finalize.total_chunks,
                        acc.chunks.len(),
                        format_missing_preview(&missing),
                        acc.min_chunk_seen,
                        acc.max_chunk_seen
                    );
                    record_backfill_hint(
                        &session_account,
                        acc,
                        Some(finalize.total_chunks),
                        &missing,
                        recorded_backfills,
                    );
                } else {
                    debug!(
                        "session {} finalized with {} expected chunks (currently have {})",
                        session_account,
                        finalize.total_chunks,
                        acc.chunks.len()
                    );
                }
                acc.chunks.len()
            })
            .unwrap_or_default();
        if observed != finalize.total_chunks as usize {
            debug!(
                "session {} finalize still waiting on {} chunk(s)",
                session_account,
                finalize.total_chunks as usize - observed
            );
        }
    }

    Ok(())
}

fn extract_session_account(
    account_keys: &[Pubkey],
    instruction: &CompiledInstruction,
    known_sessions: &HashSet<Pubkey>,
) -> Option<Pubkey> {
    if !known_sessions.is_empty() {
        for idx in &instruction.accounts {
            if let Some(key) = account_keys.get(*idx as usize) {
                if known_sessions.contains(key) {
                    return Some(*key);
                }
            }
        }
        return None;
    }
    instruction
        .accounts
        .get(1)
        .and_then(|idx| account_keys.get(*idx as usize))
        .copied()
}

fn missing_chunk_indexes(entry: &SessionAccumulator, total_chunks: u32) -> Vec<u32> {
    let mut missing = Vec::new();
    for index in 0..total_chunks {
        if !entry.chunks.contains_key(&index) {
            missing.push(index);
        }
        if missing.len() >= 16 {
            break;
        }
    }
    missing
}

fn format_missing_preview(missing: &[u32]) -> String {
    if missing.is_empty() {
        return String::new();
    }
    let mut parts: Vec<String> = missing.iter().map(|idx| idx.to_string()).collect();
    if missing.len() >= 16 {
        parts.push("…".into());
    }
    parts.join(",")
}

async fn send_upload(
    client: &HttpClient,
    config: &PluginConfig,
    stats: &Arc<PluginStats>,
    session_account: Pubkey,
    assembled: AssembledSession,
) -> Result<()> {
    let payload_len = assembled.payload.len();
    let payload_base64 = BASE64_STANDARD.encode(&assembled.payload);
    let session_id_hex = assembled.session_id.map(hex::encode);
    let merkle_hex = assembled.merkle_root.map(hex::encode);
    let finalize_slot = assembled.finalize_slot.or(assembled.last_slot);
    let finalize_signature = assembled.finalize_signature.clone();

    let request = UploadRequest {
        session_pubkey: session_account.to_string(),
        session_id: session_id_hex,
        total_chunks: assembled.total_chunks,
        chunk_count: assembled.chunk_count,
        payload_base64,
        merkle_root: merkle_hex,
        reader_mode: config.reader_mode.as_str(),
        first_slot: assembled.first_slot,
        last_slot: assembled.last_slot,
        finalize_slot,
        finalize_signature,
        compression_method: assembled.method,
    };

    let response = client
        .post(&config.endpoint_url)
        .json(&request)
        .send()
        .await
        .context("ingest request failed")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        warn!(
            "backend {} returned {} for session {}: {}",
            config.endpoint_url, status, session_account, body
        );
    } else {
        info!(
            "uploaded session {} ({} chunks, {} bytes) to {}",
            session_account, assembled.total_chunks, payload_len, config.endpoint_url
        );
        stats.uploaded_sessions.fetch_add(1, Ordering::Relaxed);
        stats
            .uploaded_chunks
            .fetch_add(assembled.chunk_count as u64, Ordering::Relaxed);
        stats
            .uploaded_bytes
            .fetch_add(payload_len as u64, Ordering::Relaxed);
    }

    Ok(())
}

fn parse_pinocchio_chunk(raw: &[u8]) -> Option<ParsedChunk> {
    if raw.first().copied()? != PINOCCHIO_CHUNK_DISCRIMINATOR {
        return None;
    }
    if raw.len() < 1 + 16 + 4 + 1 {
        return None;
    }
    let mut offset = 1;
    let session_id = raw.get(offset..offset + 16)?.try_into().ok();
    offset += 16;
    let chunk_index = u32::from_le_bytes(raw.get(offset..offset + 4)?.try_into().ok()?);
    offset += 4;
    let method = raw.get(offset).copied();
    offset += 1;
    let payload_bytes = raw.get(offset..).unwrap_or_default();

    if (2400..=2700).contains(&chunk_index) {
        let method_display = method
            .map(|value| value.to_string())
            .unwrap_or_else(|| "<none>".to_string());
        let payload_len = payload_bytes.len();
        let payload_prefix = payload_bytes
            .iter()
            .take(8)
            .map(|byte| format!("{:02x}", byte))
            .collect::<Vec<_>>()
            .join("");
        debug!(
            "parse_pinocchio_chunk metadata | discriminator=0x{:02x} chunk_index={} method={} raw_len={} payload_len={} payload_prefix={}",
            raw[0],
            chunk_index,
            method_display,
            raw.len(),
            payload_len,
            payload_prefix
        );
    }

    if payload_bytes.is_empty() {
        return None;
    }
    let decoded = if method == Some(0) {
        BASE64_STANDARD
            .decode(payload_bytes)
            .unwrap_or_else(|_| payload_bytes.to_vec())
    } else {
        payload_bytes.to_vec()
    };

    Some(ParsedChunk {
        session_id,
        chunk_index,
        payload: decoded,
        method,
    })
}

fn parse_legacy_chunk(raw: &[u8]) -> Option<ParsedChunk> {
    if raw.len() < 8 + 16 + 4 + 4 + 1 {
        return None;
    }
    let discriminator: [u8; 8] = raw.get(0..8)?.try_into().ok()?;
    if !LEGACY_CHUNK_DISCRIMINATORS.contains(&discriminator) {
        return None;
    }
    let mut offset = 8;
    let session_id = raw.get(offset..offset + 16)?.try_into().ok();
    offset += 16;
    let chunk_index = u32::from_le_bytes(raw.get(offset..offset + 4)?.try_into().ok()?);
    offset += 4;
    let string_len = u32::from_le_bytes(raw.get(offset..offset + 4)?.try_into().ok()?);
    offset += 4;
    let string_len = usize::try_from(string_len).ok()?;
    if raw.len() < offset.saturating_add(string_len) {
        return None;
    }
    let chunk_slice = &raw[offset..offset + string_len];
    offset += string_len;

    let method = raw.get(offset).copied();

    let decoded = BASE64_STANDARD
        .decode(chunk_slice)
        .unwrap_or_else(|_| chunk_slice.to_vec());

    Some(ParsedChunk {
        session_id,
        chunk_index,
        payload: decoded,
        method,
    })
}

fn inspect_pinocchio_metadata(raw: &[u8]) -> Option<([u8; 16], u32, u8)> {
    if raw.len() < 1 + 16 + 4 + 1 {
        return None;
    }
    if raw[0] != PINOCCHIO_CHUNK_DISCRIMINATOR {
        return None;
    }
    let mut session_id = [0u8; 16];
    session_id.copy_from_slice(&raw[1..17]);
    let chunk_index = u32::from_le_bytes(raw[17..21].try_into().ok()?);
    let method = raw[21];
    Some((session_id, chunk_index, method))
}

fn append_ignored_chunk_log(
    slot: u64,
    chunk_index: u32,
    method: u8,
    session_bytes: &[u8; 16],
    program: Option<Pubkey>,
    data: &[u8],
) -> std::io::Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("ignored_pin_chunks.log")?;

    writeln!(
        file,
        "slot={} chunk={} method={} session={} program={} len={} prefix={}",
        slot,
        chunk_index,
        method,
        hex::encode(session_bytes),
        program
            .map(|p| p.to_string())
            .unwrap_or_else(|| "<unknown>".to_string()),
        data.len(),
        data.iter()
            .take(32)
            .map(|byte| format!("{:02x}", byte))
            .collect::<Vec<_>>()
            .join("")
    )
}

fn parse_pinocchio_finalize(raw: &[u8]) -> Option<FinalizeData> {
    if raw.first().copied()? != PINOCCHIO_FINALIZE_DISCRIMINATOR {
        return None;
    }
    if raw.len() < 1 + 16 + 4 {
        return None;
    }
    let mut offset = 1;
    let session_id = Some(raw.get(offset..offset + 16)?.try_into().ok()?);
    offset += 16;
    let total_chunks = u32::from_le_bytes(raw.get(offset..offset + 4)?.try_into().ok()?);
    offset += 4;
    let merkle_root = if raw.len() >= offset + 32 {
        Some(raw.get(offset..offset + 32)?.try_into().ok()?)
    } else {
        None
    };

    Some(FinalizeData {
        session_id,
        total_chunks,
        merkle_root,
    })
}

fn record_backfill_hint(
    session: &Pubkey,
    acc: &SessionAccumulator,
    total_chunks: Option<u32>,
    missing_preview: &[u32],
    recorded_backfills: &Arc<Mutex<HashSet<(Pubkey, Vec<u32>)>>>,
) {
    if missing_preview.is_empty() {
        return;
    }

    // Check if we've already recorded this session + missing chunks combination
    let cache_key = (*session, missing_preview.to_vec());
    {
        let mut cache = recorded_backfills.lock().unwrap();
        if cache.contains(&cache_key) {
            debug!(
                "skipping duplicate backfill hint for session {} (already recorded)",
                session
            );
            return;
        }
        // Add to cache before writing to prevent duplicates from concurrent threads
        cache.insert(cache_key);
    }

    // Use a much tighter window: only ±1 slot padding around the actual observed range
    let slot_start = acc.first_slot.or(acc.last_slot).or(acc.finalize_slot);
    let slot_end = acc.finalize_slot.or(acc.last_slot).or(slot_start);
    let range_start = slot_start.unwrap_or(0).saturating_sub(1);
    let range_end = slot_end.unwrap_or(range_start).saturating_add(2);

    let total_missing = total_chunks.map(|total| {
        (0..total)
            .filter(|idx| !acc.chunks.contains_key(idx))
            .count()
    });

    let recommended_command = slot_end.map(|_| {
        format!(
            "cargo run -p iq-plugin --bin iq_runner --release -- {}:{} --log-level debug",
            range_start,
            range_end,
        )
    });

    let record = json!({
        "session": session.to_string(),
        "observed_chunks": acc.chunks.len(),
        "expected_chunks": total_chunks,
        "total_missing": total_missing,
        "missing_preview": missing_preview,
        "first_slot": acc.first_slot,
        "last_slot": acc.last_slot,
        "finalize_slot": acc.finalize_slot,
        "finalize_signature": acc.finalize_signature,
        "recommended_slot_range": {
            "start": range_start,
            "end_inclusive": range_end.saturating_sub(1),
        },
        "recommended_command": recommended_command
    });

    if let Ok(json_line) = serde_json::to_string(&record) {
        append_line(BACKFILL_HINTS_FILE, &json_line);
    }
}

fn append_line(path: &str, line: &str) {
    match OpenOptions::new().create(true).append(true).open(path) {
        Ok(mut file) => {
            if let Err(err) = writeln!(file, "{line}") {
                debug!("failed to append to {}: {}", path, err);
            }
        }
        Err(err) => debug!("failed to open {} for append: {}", path, err),
    }
}

fn load_existing_backfills() -> HashSet<(Pubkey, Vec<u32>)> {
    let mut seen = HashSet::new();
    let file = match std::fs::File::open(BACKFILL_HINTS_FILE) {
        Ok(f) => f,
        Err(_) => return seen,
    };

    let reader = BufReader::new(file);
    for line in reader.lines().flatten() {
        if let Ok(record) = serde_json::from_str::<serde_json::Value>(&line) {
            if let (Some(session_str), Some(missing_arr)) =
                (record.get("session"), record.get("missing_preview"))
            {
                if let (Some(session_str), Some(missing_arr)) =
                    (session_str.as_str(), missing_arr.as_array())
                {
                    if let Ok(pubkey) = Pubkey::from_str(session_str) {
                        let missing: Vec<u32> = missing_arr
                            .iter()
                            .filter_map(|v| v.as_u64().map(|n| n as u32))
                            .collect();
                        seen.insert((pubkey, missing));
                    }
                }
            }
        }
    }

    if !seen.is_empty() {
        info!(
            "loaded {} existing backfill hints to avoid duplicates",
            seen.len()
        );
    }

    seen
}

fn parse_legacy_finalize(raw: &[u8]) -> Option<FinalizeData> {
    if raw.len() < 8 + 4 {
        return None;
    }
    let discriminator: [u8; 8] = raw.get(0..8)?.try_into().ok()?;
    let mut offset = 8;
    if discriminator == LEGACY_FINALIZE_HYBRID {
        if raw.len() < offset + 16 + 4 {
            return None;
        }
        let session_id = Some(raw.get(offset..offset + 16)?.try_into().ok()?);
        offset += 16;
        let total_chunks = u32::from_le_bytes(raw.get(offset..offset + 4)?.try_into().ok()?);
        offset += 4;
        let merkle_root = if raw.len() >= offset + 32 {
            Some(raw.get(offset..offset + 32)?.try_into().ok()?)
        } else {
            None
        };
        return Some(FinalizeData {
            session_id,
            total_chunks,
            merkle_root,
        });
    }

    if discriminator == LEGACY_FINALIZE_BUNDLE {
        if raw.len() < offset + 4 {
            return None;
        }
        let total_chunks = u32::from_le_bytes(raw.get(offset..offset + 4)?.try_into().ok()?);
        offset += 4;
        let merkle_root = if raw.len() >= offset + 32 {
            Some(raw.get(offset..offset + 32)?.try_into().ok()?)
        } else {
            None
        };
        return Some(FinalizeData {
            session_id: None,
            total_chunks,
            merkle_root,
        });
    }

    None
}
