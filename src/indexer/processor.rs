use anyhow::{Context, Result};
use serde_json::{json, Value};
use solana_sdk::pubkey::Pubkey;
use solana_transaction_status::{
    EncodedTransaction, UiMessage, UiTransaction, UiTransactionStatusMeta,
};
use sqlx::PgPool;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::{
    db::queries::{
        upsert_account_typed, upsert_checkpoint, upsert_ix_typed, upsert_transaction,
        LAST_SIG_KEY, LAST_SLOT_KEY,
    },
    idl::{
        decoder::{AccountDecoder, IxDecoder},
        schema::IdlSchema,
        Idl,
    },
    indexer::rpc::RpcClientWithRetry,
};

// Checkpoint keys are defined in db::queries (LAST_SIG_KEY, LAST_SLOT_KEY)

struct NormalisedTx {
    signature: String,
    slot: u64,
    block_time: Option<i64>,
    err: Option<Value>,
    accounts: Vec<String>,
    num_required_signers: usize,
    num_readonly_signed: usize,
    num_readonly_unsigned: usize,
    instructions: Vec<(u8, Vec<u8>, Vec<u8>)>,
}

pub struct Processor {
    pub rpc: Arc<RpcClientWithRetry>,
    pub pool: PgPool,
    pub program_id: String,
    pub ix_decoder: Arc<IxDecoder>,
    pub account_decoder: Arc<AccountDecoder>,
    pub ix_table_map: HashMap<String, String>,
    pub acc_table_map: HashMap<String, String>,
    pub batch_size: usize,
    pub shutdown: watch::Receiver<bool>,
}

impl Processor {
    pub fn new(
        rpc: Arc<RpcClientWithRetry>,
        pool: PgPool,
        program_id: String,
        idl: &Idl,
        schema: &IdlSchema,
        batch_size: usize,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        let ix_table_map = schema.instructions.iter()
            .map(|ix| (ix.name.clone(), ix.table_name.clone()))
            .collect();
        let acc_table_map = schema.accounts.iter()
            .map(|acc| (acc.name.clone(), acc.table_name.clone()))
            .collect();

        Self {
            rpc,
            pool,
            program_id,
            ix_decoder: Arc::new(IxDecoder::new(idl)),
            account_decoder: Arc::new(AccountDecoder::new(idl)),
            ix_table_map,
            acc_table_map,
            batch_size,
            shutdown,
        }
    }

    pub fn is_shutting_down(&self) -> bool { *self.shutdown.borrow() }

    pub async fn process_signatures(&self, signatures: &[String]) -> Result<()> {
        let total = signatures.len();
        let mut processed = 0usize;
        for chunk in signatures.chunks(self.batch_size) {
            if self.is_shutting_down() {
                info!("Shutdown requested, stopping processing");
                break;
            }
            self.process_and_commit(chunk).await?;
            processed += chunk.len();
            info!(processed, total, "batch committed");
        }
        Ok(())
    }

    pub async fn process_and_commit(&self, signatures: &[String]) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        let mut last_sig: Option<&str> = None;
        let mut last_slot: Option<u64> = None;
        let mut account_addrs_to_fetch: Vec<(String, u64)> = Vec::new();

        info!(count = signatures.len(), "Processing batch");

        for sig in signatures {
            let encoded = match self.rpc.get_transaction(sig).await {
                Ok(t) => t,
                Err(e) => { warn!(sig = %sig, "Failed to fetch tx: {e}"); continue; }
            };
            let norm = match normalise_tx(&encoded, sig) {
                Some(n) => n,
                None => { warn!(sig = %sig, "Failed to normalise tx"); continue; }
            };

            debug!(sig = %sig, slot = norm.slot, ix_count = norm.instructions.len(), "Tx normalised");

            upsert_transaction(
                &mut tx, &norm.signature, norm.slot as i64,
                norm.block_time, norm.err.as_ref(),
            ).await?;

            for (prog_idx, acc_indices, data_bytes) in &norm.instructions {
                let prog_pubkey = match norm.accounts.get(*prog_idx as usize) {
                    Some(p) => p.clone(),
                    None => continue,
                };

                debug!(sig = %sig, prog = %prog_pubkey, data_len = data_bytes.len(), "Checking instruction");

                if prog_pubkey != self.program_id {
                    debug!(sig = %sig, prog = %prog_pubkey, expected = %self.program_id, "Skipping: different program");
                    continue;
                }

                let (ix_name, decoded_args) = match self.ix_decoder.decode(data_bytes) {
                    Some(d) => d,
                    None => {
                        let disc = if data_bytes.len() >= 8 { hex::encode(&data_bytes[..8]) } else { hex::encode(data_bytes) };
                        warn!(sig = %sig, discriminator = %disc, "Unknown discriminator, skipping");
                        continue;
                    }
                };

                info!(sig = %sig, ix = %ix_name, "Decoded instruction");

                for idx in acc_indices {
                    if let Some(addr) = norm.accounts.get(*idx as usize) {
                        account_addrs_to_fetch.push((addr.clone(), norm.slot));
                    }
                }

                if let Some(table_name) = self.ix_table_map.get(&ix_name) {
                    upsert_ix_typed(
                        &mut tx, table_name, &norm.signature,
                        norm.slot as i64, norm.block_time, &decoded_args,
                    ).await.unwrap_or_else(|e| warn!("ix typed insert failed for {table_name}: {e}"));
                } else {
                    warn!(ix = %ix_name, "No table mapping for instruction — check IDL");
                }
            }

            last_sig = Some(sig.as_str());
            last_slot = Some(norm.slot);
        }

        if self.account_decoder.has_accounts() && !account_addrs_to_fetch.is_empty() {
            self.fetch_and_store_accounts(&mut tx, &account_addrs_to_fetch)
                .await
                .unwrap_or_else(|e| warn!("account state fetch failed: {e}"));
        }

        if let Some(sig) = last_sig {
            upsert_checkpoint(&mut tx, LAST_SIG_KEY, sig).await?;
        }
        if let Some(slot) = last_slot {
            upsert_checkpoint(&mut tx, LAST_SLOT_KEY, &slot.to_string()).await?;
        }

        tx.commit().await.context("commit batch")?;
        Ok(())
    }

    async fn fetch_and_store_accounts(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        addr_slots: &[(String, u64)],
    ) -> Result<()> {
        let mut unique: HashMap<String, u64> = HashMap::new();
        for (addr, slot) in addr_slots {
            unique.entry(addr.clone()).or_insert(*slot);
        }
        let pubkeys: Vec<Pubkey> = unique.keys()
            .filter_map(|a| Pubkey::from_str(a).ok())
            .collect();
        if pubkeys.is_empty() { return Ok(()); }

        let accounts = self.rpc.get_accounts_data(&pubkeys).await?;
        for (pk, acc_opt) in pubkeys.iter().zip(accounts.iter()) {
            if let Some((lamports, data)) = acc_opt {
                let addr = pk.to_string();
                let slot = *unique.get(&addr).unwrap_or(&0) as i64;
                if let Some((acc_name, decoded)) = self.account_decoder.decode(data) {
                    if let Some(table_name) = self.acc_table_map.get(&acc_name) {
                        upsert_account_typed(
                            tx, table_name, &addr, slot,
                            Some(*lamports as i64), &decoded,
                        ).await?;
                        debug!(addr = %addr, table = %table_name, "account state stored");
                    }
                }
            }
        }
        Ok(())
    }
}

fn normalise_tx(
    encoded: &solana_transaction_status::EncodedConfirmedTransactionWithStatusMeta,
    signature: &str,
) -> Option<NormalisedTx> {
    let slot = encoded.slot;
    let block_time = encoded.block_time;
    let meta: &UiTransactionStatusMeta = encoded.transaction.meta.as_ref()?;
    let err = meta.err.as_ref().map(|e| json!(e.to_string()));

    match &encoded.transaction.transaction {
        EncodedTransaction::Json(ui_tx) =>
            normalise_json_tx(ui_tx, signature, slot, block_time, err),
        EncodedTransaction::Binary(b64, _enc) => {
            let bytes = base64::Engine::decode(
                &base64::engine::general_purpose::STANDARD, b64,
            ).ok()?;
            normalise_binary_tx(&bytes, signature, slot, block_time, err, meta)
        }
        _ => None,
    }
}

fn normalise_json_tx(
    ui_tx: &UiTransaction, signature: &str, slot: u64,
    block_time: Option<i64>, err: Option<Value>,
) -> Option<NormalisedTx> {
    let message = match &ui_tx.message {
        UiMessage::Parsed(_) => return None,
        UiMessage::Raw(r) => r,
    };
    let accounts: Vec<String> = message.account_keys.clone();
    let num_required_signers  = message.header.num_required_signatures as usize;
    let num_readonly_signed   = message.header.num_readonly_signed_accounts as usize;
    let num_readonly_unsigned = message.header.num_readonly_unsigned_accounts as usize;
    let instructions = message.instructions.iter().map(|ix| {
        let data_bytes = bs58::decode(&ix.data).into_vec().unwrap_or_default();
        (ix.program_id_index, ix.accounts.clone(), data_bytes)
    }).collect();
    Some(NormalisedTx {
        signature: signature.to_string(), slot, block_time, err, accounts,
        num_required_signers, num_readonly_signed, num_readonly_unsigned, instructions,
    })
}

fn normalise_binary_tx(
    bytes: &[u8], signature: &str, slot: u64,
    block_time: Option<i64>, err: Option<Value>,
    meta: &UiTransactionStatusMeta,
) -> Option<NormalisedTx> {
    use solana_sdk::transaction::VersionedTransaction;
    use solana_transaction_status::option_serializer::OptionSerializer;

    let vtx: VersionedTransaction = bincode::deserialize(bytes).ok()?;
    let message = &vtx.message;
    let static_keys: Vec<String> = message.static_account_keys()
        .iter().map(|k| k.to_string()).collect();
    let mut accounts = static_keys;
    if let OptionSerializer::Some(loaded) = &meta.loaded_addresses {
        for addr in &loaded.writable { accounts.push(addr.clone()); }
        for addr in &loaded.readonly { accounts.push(addr.clone()); }
    }
    let header = message.header();
    let num_required_signers  = header.num_required_signatures as usize;
    let num_readonly_signed   = header.num_readonly_signed_accounts as usize;
    let num_readonly_unsigned = header.num_readonly_unsigned_accounts as usize;
    let instructions: Vec<(u8, Vec<u8>, Vec<u8>)> = message.instructions().iter()
        .map(|ix| (ix.program_id_index, ix.accounts.clone(), ix.data.clone()))
        .collect();
    Some(NormalisedTx {
        signature: signature.to_string(), slot, block_time, err, accounts,
        num_required_signers, num_readonly_signed, num_readonly_unsigned, instructions,
    })
}

fn _build_accounts_json(
    acc_indices: &[u8], all_accounts: &[String],
    num_required_signers: usize, num_readonly_signed: usize, num_readonly_unsigned: usize,
) -> Value {
    let total = all_accounts.len();
    let num_signed = num_required_signers;
    let num_writable_signed = num_signed.saturating_sub(num_readonly_signed);
    let num_unsigned = total.saturating_sub(num_signed);
    let num_writable_unsigned = num_unsigned.saturating_sub(num_readonly_unsigned);
    let accounts: Vec<Value> = acc_indices.iter().filter_map(|&idx| {
        let pubkey = all_accounts.get(idx as usize)?.clone();
        let i = idx as usize;
        let is_signer = i < num_signed;
        let is_writable = if is_signer {
            i < num_writable_signed
        } else {
            i.saturating_sub(num_signed) < num_writable_unsigned
        };
        Some(json!({ "pubkey": pubkey, "is_signer": is_signer, "is_writable": is_writable }))
    }).collect();
    Value::Array(accounts)
}
