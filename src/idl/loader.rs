use anyhow::{anyhow, Context, Result};
use flate2::read::ZlibDecoder;
use serde::{Deserialize, Serialize};
use solana_client::rpc_client::RpcClient;
use solana_sdk::pubkey::Pubkey;
use std::io::Read;
use std::str::FromStr;
use tracing::info;

use crate::config::IdlSource;

/// Universal IDL struct — works with Anchor <0.30 and >=0.30.
/// All collection fields are Vec<Value> to avoid breaking on different IDL formats.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Idl {
    pub version: Option<String>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub instructions: Vec<serde_json::Value>,
    /// Account type definitions (for state decoding)
    #[serde(default)]
    pub accounts: Vec<serde_json::Value>,
    #[serde(default)]
    pub types: Vec<serde_json::Value>,
    #[serde(default)]
    pub events: Vec<serde_json::Value>,
    #[serde(default)]
    pub errors: Vec<serde_json::Value>,
    #[serde(default)]
    pub address: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

pub fn load_idl(source: &IdlSource, rpc: &RpcClient, program_id: &str) -> Result<Idl> {
    match source {
        IdlSource::File(path) => load_from_file(path, program_id),
        IdlSource::OnChain => load_from_chain(rpc, program_id),
    }
}

fn load_from_file(path: &str, expected_program_id: &str) -> Result<Idl> {
    info!("Loading IDL from file: {}", path);
    let raw =
        std::fs::read_to_string(path).with_context(|| format!("failed to read IDL file: {path}"))?;
    let idl: Idl = serde_json::from_str(&raw).context("failed to parse IDL JSON from file")?;

    // Verify the IDL matches PROGRAM_ID
    let idl_address = idl
        .address
        .as_deref()
        .or_else(|| {
            idl.metadata
                .as_ref()
                .and_then(|m| m.get("address"))
                .and_then(|v| v.as_str())
        });

    if let Some(addr) = idl_address {
        if addr != expected_program_id {
            return Err(anyhow!(
                "IDL address ({addr}) does not match PROGRAM_ID ({expected_program_id})"
            ));
        }
    }

    info!(
        name = idl.name.as_deref().unwrap_or("unknown"),
        instructions = idl.instructions.len(),
        accounts = idl.accounts.len(),
        "IDL loaded from file"
    );
    Ok(idl)
}

fn load_from_chain(rpc: &RpcClient, program_id: &str) -> Result<Idl> {
    info!("Loading IDL from on-chain for program: {}", program_id);
    let pid =
        Pubkey::from_str(program_id).with_context(|| format!("invalid program id: {program_id}"))?;

    // Anchor IDL PDA: seeds=[], createWithSeed(base, "anchor:idl", pid)
    let (base, _) = Pubkey::find_program_address(&[], &pid);
    let idl_address = Pubkey::create_with_seed(&base, "anchor:idl", &pid)
        .context("failed to derive IDL address")?;

    let account = rpc
        .get_account(&idl_address)
        .with_context(|| format!("IDL account not found at {idl_address}"))?;

    // Layout: [8 discriminator][32 authority][4 data_len][N payload]
    if account.data.len() < 44 {
        return Err(anyhow!(
            "IDL account data too short: {} bytes",
            account.data.len()
        ));
    }
    let data_len = u32::from_le_bytes(account.data[40..44].try_into().unwrap()) as usize;
    if account.data.len() < 44 + data_len {
        return Err(anyhow!("IDL account data truncated"));
    }
    let payload = &account.data[44..44 + data_len];

    // Magic byte 0x78 = zlib (Anchor <0.30), otherwise raw JSON
    let json = if payload.first() == Some(&0x78) {
        let mut decoder = ZlibDecoder::new(payload);
        let mut s = String::new();
        decoder.read_to_string(&mut s).context("zlib decode failed")?;
        s
    } else {
        String::from_utf8(payload.to_vec()).context("IDL payload is not valid UTF-8")?
    };

    let idl: Idl = serde_json::from_str(&json).context("failed to parse on-chain IDL JSON")?;
    info!(
        name = idl.name.as_deref().unwrap_or("unknown"),
        instructions = idl.instructions.len(),
        accounts = idl.accounts.len(),
        "IDL loaded from chain"
    );
    Ok(idl)
}
