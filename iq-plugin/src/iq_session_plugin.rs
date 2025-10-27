use std::{
    collections::{BTreeMap, HashSet},
    env,
    io::ErrorKind,
    str::FromStr,
    sync::Arc,
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

#[derive(Clone)]
struct PluginConfig {
    program_id: Option<Pubkey>,
    session_pdas: HashSet<Pubkey>,
    reader_mode: ReaderMode,
    endpoint_url: String,
}

impl PluginConfig {
    fn from_env() -> Result<Self> {
        let program_id = match env::var("IQ_PROGRAM_ID") {
            Ok(value) if !value.trim().is_empty() => {
                Some(Pubkey::from_str(value.trim()).context("invalid IQ_PROGRAM_ID")?)
            }
            _ => None,
        };

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
            program_id,
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
        Ok(Self {
            config,
            client,
            sessions,
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
            info!(
                "IQ Session Plugin loaded | mode={} endpoint={} program_id={} session_filters={}",
                config.reader_mode.as_str(),
                config.endpoint_url,
                config
                    .program_id
                    .map(|pk| pk.to_string())
                    .unwrap_or_else(|| "<any>".to_string()),
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
        Box::pin(async move {
            if let Err(err) =
                process_transaction(thread_id, &config, &client, &sessions, transaction).await
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
        Box::pin(async move {
            if !sessions.is_empty() {
                warn!("{} sessions still buffered at shutdown", sessions.len());
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
        let program_matches = config
            .program_id
            .map(|expected| Some(expected) == program_key)
            .unwrap_or(true);
        let session_match = instruction.accounts.iter().any(|idx| {
            account_keys
                .get(*idx as usize)
                .map(|key| config.session_pdas.contains(key))
                .unwrap_or(false)
        });

        if !program_matches && !session_match {
            continue;
        }

        let data = instruction.data.as_slice();

        if let Some(chunk) = config.reader_mode.parse_chunk(data) {
            processed = true;
            handle_chunk(
                config,
                client,
                sessions,
                &account_keys,
                instruction,
                transaction,
                chunk,
            )
            .await?;
            continue;
        }

        if let Some(finalize) = config.reader_mode.parse_finalize(data) {
            processed = true;
            handle_finalize(
                config,
                client,
                sessions,
                &account_keys,
                instruction,
                transaction,
                finalize,
            )
            .await?;
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
        if let Err(err) = send_upload(client, config, session_account, assembled).await {
            error!(
                "failed to send reconstructed session {} to backend: {}",
                session_account, err
            );
        }
    }

    Ok(())
}

async fn handle_finalize(
    config: &PluginConfig,
    client: &HttpClient,
    sessions: &Arc<DashMap<Pubkey, SessionAccumulator>>,
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
        if let Err(err) = send_upload(client, config, session_account, assembled).await {
            error!(
                "failed to send reconstructed session {} after finalize: {}",
                session_account, err
            );
        }
    } else {
        debug!(
            "session {} finalized with {} expected chunks (currently have {})",
            session_account,
            finalize.total_chunks,
            sessions
                .get(&session_account)
                .map(|entry| entry.chunks.len())
                .unwrap_or_default()
        );
    }

    Ok(())
}

fn extract_session_account(
    account_keys: &[Pubkey],
    instruction: &CompiledInstruction,
    known_sessions: &HashSet<Pubkey>,
) -> Option<Pubkey> {
    for idx in &instruction.accounts {
        if let Some(key) = account_keys.get(*idx as usize) {
            if known_sessions.contains(key) {
                return Some(*key);
            }
        }
    }
    instruction
        .accounts
        .get(1)
        .and_then(|idx| account_keys.get(*idx as usize))
        .copied()
}

async fn send_upload(
    client: &HttpClient,
    config: &PluginConfig,
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
