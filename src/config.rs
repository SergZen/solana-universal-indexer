use anyhow::{Context, Result};
use std::time::Duration;

#[derive(Debug, Clone)]
pub enum IdlSource {
    File(String),
    OnChain,
}

#[derive(Debug, Clone, PartialEq)]
pub enum IndexerMode {
    Batch,
    Realtime,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub rpc_url: String,
    pub ws_url: String,
    pub program_id: String,
    pub idl_source: IdlSource,
    pub database_url: String,
    pub mode: IndexerMode,
    pub start_slot: Option<u64>,
    pub end_slot: Option<u64>,
    pub signatures: Vec<String>,
    pub batch_size: usize,
    pub poll_interval_secs: u64,
    pub rpc_max_retries: u32,
    pub api_host: String,
    pub api_port: u16,
}

impl Config {
    pub fn from_env() -> Result<Self> {
        dotenvy::dotenv().ok();

        let rpc_url = required("RPC_URL")?;
        let program_id = required("PROGRAM_ID")?;
        let database_url = required("DATABASE_URL")?;

        let ws_url = std::env::var("WS_URL").unwrap_or_else(|_| {
            rpc_url
                .replacen("https://", "wss://", 1)
                .replacen("http://", "ws://", 1)
        });

        let idl_source = match std::env::var("IDL_SOURCE")
            .unwrap_or_else(|_| "onchain".into())
            .to_lowercase()
            .as_str()
        {
            "file" => {
                let path =
                    required("IDL_PATH").context("IDL_PATH required when IDL_SOURCE=file")?;
                IdlSource::File(path)
            }
            _ => IdlSource::OnChain,
        };

        let mode = match std::env::var("INDEXER_MODE")
            .unwrap_or_else(|_| "realtime".into())
            .to_lowercase()
            .as_str()
        {
            "batch" => IndexerMode::Batch,
            _ => IndexerMode::Realtime,
        };

        let signatures = std::env::var("SIGNATURES")
            .ok()
            .map(|s| s.split(',').map(|x| x.trim().to_string()).filter(|x| !x.is_empty()).collect())
            .unwrap_or_default();

        let start_slot = std::env::var("START_SLOT").ok().and_then(|v| v.parse().ok());
        let end_slot = std::env::var("END_SLOT").ok().and_then(|v| v.parse().ok());
        let batch_size = std::env::var("BATCH_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);
        let poll_interval_secs = std::env::var("POLL_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);
        let rpc_max_retries = std::env::var("RPC_MAX_RETRIES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(5);
        let api_host = std::env::var("API_HOST").unwrap_or_else(|_| "0.0.0.0".into());
        let api_port = std::env::var("API_PORT")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(3000);

        Ok(Self {
            rpc_url,
            ws_url,
            program_id,
            database_url,
            idl_source,
            mode,
            start_slot,
            end_slot,
            signatures,
            batch_size,
            poll_interval_secs,
            rpc_max_retries,
            api_host,
            api_port,
        })
    }

    pub fn poll_interval(&self) -> Duration {
        Duration::from_secs(self.poll_interval_secs)
    }
}

fn required(key: &str) -> Result<String> {
    std::env::var(key).with_context(|| format!("missing required env var: {key}"))
}
