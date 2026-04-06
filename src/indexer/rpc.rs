use anyhow::{Context, Result};
use solana_client::{
    rpc_client::{GetConfirmedSignaturesForAddress2Config, RpcClient},
    rpc_config::{RpcBlockConfig, RpcTransactionConfig},
    rpc_response::{RpcConfirmedTransactionStatusWithSignature, transaction::VersionedMessage},
};
use solana_commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    EncodedConfirmedTransactionWithStatusMeta, EncodedTransactionWithStatusMeta,
    TransactionDetails, UiTransactionEncoding,
};
use std::str::FromStr;
use std::time::Duration;
use tokio_retry::{strategy::ExponentialBackoff, Retry};
use tracing::{debug, info, warn};

pub struct RpcClientWithRetry {
    pub client: RpcClient,
    max_retries: u32,
}

impl RpcClientWithRetry {
    pub fn new(url: &str, max_retries: u32) -> Self {
        let client = RpcClient::new_with_timeout(url.to_string(), Duration::from_secs(60));
        Self { client, max_retries }
    }

    fn retry_strategy(&self) -> impl Iterator<Item = Duration> {
        ExponentialBackoff::from_millis(500)
            .factor(2)
            .max_delay(Duration::from_secs(30))
            .take(self.max_retries as usize)
    }

    pub async fn get_signatures_page(
        &self,
        program_id: &str,
        before: Option<&str>,
        until: Option<&str>,
        limit: usize,
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        let pid = Pubkey::from_str(program_id)?;
        let before_sig: Option<solana_sdk::signature::Signature> =
            before.and_then(|s| s.parse().ok());
        let until_sig: Option<solana_sdk::signature::Signature> =
            until.and_then(|s| s.parse().ok());

        let strategy = self.retry_strategy();
        let client = &self.client;
        Retry::spawn(strategy, || async {
            let cfg = GetConfirmedSignaturesForAddress2Config {
                before: before_sig,
                until: until_sig,
                limit: Some(limit),
                commitment: Some(CommitmentConfig::confirmed()),
            };
            client
                .get_signatures_for_address_with_config(&pid, cfg)
                .map_err(|e| { warn!("get_signatures failed: {e}"); e })
        })
        .await
        .context("get_signatures_for_address after retries")
    }

    pub async fn get_transaction(
        &self,
        signature: &str,
    ) -> Result<EncodedConfirmedTransactionWithStatusMeta> {
        let sig = signature
            .parse()
            .with_context(|| format!("invalid signature: {signature}"))?;
        let cfg = RpcTransactionConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(0),
        };
        let strategy = self.retry_strategy();
        let client = &self.client;
        Retry::spawn(strategy, || async {
            client
                .get_transaction_with_config(&sig, cfg.clone())
                .map_err(|e| { warn!(sig = %signature, "get_transaction failed: {e}"); e })
        })
        .await
        .with_context(|| format!("get_transaction({signature}) after retries"))
    }

    /// Scan a slot range via getBlock — directly reads each block and filters
    /// transactions that involve the target program. Runs in a blocking thread
    /// to avoid starving the async runtime on synchronous RPC calls.
    pub async fn get_signatures_for_slot_range(
        &self,
        program_id: &str,
        start_slot: u64,
        end_slot: u64,
        _batch_size: usize,
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        info!(start_slot, end_slot, program_id, "Scanning slot range via getBlock");

        let total_slots = end_slot.saturating_sub(start_slot) + 1;
        info!(total_slots, "Slots to scan");

        // get_block_with_config is synchronous — must run in spawn_blocking
        // to avoid blocking the Tokio async runtime.
        let rpc_url   = self.client.url();
        let pid_str   = program_id.to_string();
        let max_retry = self.max_retries;

        let all = tokio::task::spawn_blocking(move || {
            use std::time::Duration;

            let client = RpcClient::new_with_timeout(rpc_url, Duration::from_secs(30));
            let mut all: Vec<RpcConfirmedTransactionStatusWithSignature> = Vec::new();
            let log_every = (total_slots / 10).max(1);

            for (i, slot) in (start_slot..=end_slot).enumerate() {
                // Log progress every 10%
                if i as u64 % log_every == 0 {
                    info!(
                        slot,
                        progress = format!("{}/{}", i, total_slots),
                        found = all.len(),
                        "Scanning slots…"
                    );
                }

                // Retry each slot up to max_retry times
                let mut last_err = None;
                let block = 'retry: {
                    for attempt in 0..max_retry.max(1) {
                        match client.get_block_with_config(
                            slot,
                            RpcBlockConfig {
                                encoding: Some(UiTransactionEncoding::Json),
                                transaction_details: Some(TransactionDetails::Full),
                                rewards: Some(false),
                                commitment: None,
                                max_supported_transaction_version: Some(0),
                            },
                        ) {
                            Ok(b) => break 'retry Some(b),
                            Err(e) => {
                                let msg = e.to_string();
                                // SlotSkipped / BlockNotAvailable — slot is empty, skip immediately
                                if msg.contains("SlotSkipped")
                                    || msg.contains("Block not available")
                                    || msg.contains("was skipped")
                                {
                                    debug!(slot, "Slot skipped or not available");
                                    break 'retry None;
                                }
                                last_err = Some(msg);
                                if attempt + 1 < max_retry.max(1) {
                                    std::thread::sleep(Duration::from_millis(
                                        500 * 2u64.pow(attempt),
                                    ));
                                }
                            }
                        }
                    }
                    // All retries exhausted
                    if let Some(e) = &last_err {
                        debug!(slot, error = %e, "Failed to fetch block after retries, skipping");
                    }
                    None
                };

                let block = match block {
                    Some(b) => b,
                    None => continue,
                };

                let block_time = block.block_time;
                if let Some(txs) = block.transactions {
                    let tx_count = txs.len();
                    let mut matched = 0usize;
                    for tx in txs {
                        if Self::tx_uses_program(&tx, &pid_str) {
                            matched += 1;
                            if let Some(sig) = Self::tx_first_signature(&tx) {
                                all.push(RpcConfirmedTransactionStatusWithSignature {
                                    signature: sig,
                                    slot,
                                    err: tx.meta.as_ref().and_then(|m| m.err.clone()),
                                    memo: None,
                                    block_time,
                                    confirmation_status: None,
                                });
                            }
                        }
                    }
                    if matched > 0 {
                        debug!(slot, tx_count, matched, "Found program txs in block");
                    }
                }
            }

            all
        })
        .await
        .context("slot range scan task panicked")?;

        info!(total = all.len(), start_slot, end_slot, "Slot range scan complete");
        Ok(all)
    }

    /// Check if a transaction involves the target program.
    ///
    /// When blocks are fetched with Json encoding, transactions come as
    /// EncodedTransaction::Json(UiTransaction) — decode() returns None in that case.
    /// We check account_keys directly from the UiRawMessage instead.
    fn tx_uses_program(tx: &EncodedTransactionWithStatusMeta, program_id: &str) -> bool {
        use solana_transaction_status::{EncodedTransaction, UiMessage};

        match &tx.transaction {
            // Json encoding — read account_keys from UiRawMessage directly
            EncodedTransaction::Json(ui_tx) => {
                match &ui_tx.message {
                    UiMessage::Raw(raw) => {
                        raw.account_keys.iter().any(|k| k == program_id)
                    }
                    UiMessage::Parsed(parsed) => {
                        parsed.account_keys.iter().any(|k| k.pubkey == program_id)
                    }
                }
            }
            // Binary/Base64 encoding — decode and check account keys
            _ => {
                let pid = match Pubkey::from_str(program_id) {
                    Ok(p) => p,
                    Err(_) => return false,
                };
                if let Some(decoded) = tx.transaction.decode() {
                    let keys: &[Pubkey] = match &decoded.message {
                        VersionedMessage::Legacy(msg) => &msg.account_keys,
                        VersionedMessage::V0(msg)     => &msg.account_keys,
                    };
                    return keys.iter().any(|k| k == &pid);
                }
                false
            }
        }
    }

    /// Extract the first signature from a transaction regardless of encoding.
    fn tx_first_signature(tx: &EncodedTransactionWithStatusMeta) -> Option<String> {
        use solana_transaction_status::EncodedTransaction;

        match &tx.transaction {
            EncodedTransaction::Json(ui_tx) => {
                ui_tx.signatures.first().cloned()
            }
            _ => {
                tx.transaction.decode()
                    .and_then(|d| d.signatures.first().map(|s| s.to_string()))
            }
        }
    }

    /// Fetch all signatures newer than `since_sig` (for cold-start backfill).
    /// Uses getSignaturesForAddress with `until` parameter — efficient because
    /// it stops as soon as it hits the known checkpoint.
    pub async fn get_signatures_since(
        &self,
        program_id: &str,
        since_sig: &str,
        batch_size: usize,
    ) -> Result<Vec<RpcConfirmedTransactionStatusWithSignature>> {
        let mut all = Vec::new();
        let mut before: Option<String> = None;
        loop {
            let page = self
                .get_signatures_page(program_id, before.as_deref(), Some(since_sig), batch_size)
                .await?;
            if page.is_empty() { break; }
            before = page.last().map(|s| s.signature.clone());
            all.extend(page);
        }
        // RPC returns newest-first; reverse to process oldest first
        all.reverse();
        Ok(all)
    }

    /// Returns (lamports, data) per pubkey.
    /// Extracts fields immediately to avoid cross-version solana-account type conflict.
    pub async fn get_accounts_data(
        &self,
        pubkeys: &[Pubkey],
    ) -> Result<Vec<Option<(u64, Vec<u8>)>>> {
        if pubkeys.is_empty() {
            return Ok(vec![]);
        }
        let strategy = self.retry_strategy();
        let client = &self.client;
        let result: Vec<Option<(u64, Vec<u8>)>> = Retry::spawn(strategy, || async {
            client
                .get_multiple_accounts(pubkeys)
                .map(|accounts| {
                    accounts
                        .into_iter()
                        .map(|opt| opt.map(|a| (a.lamports, a.data.clone())))
                        .collect()
                })
                .map_err(|e| { warn!("get_multiple_accounts failed: {e}"); e })
        })
        .await
        .context("get_multiple_accounts after retries")?;
        Ok(result)
    }
}
