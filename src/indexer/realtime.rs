use anyhow::Result;
use solana_client::{
    pubsub_client::PubsubClient,
    rpc_config::{RpcTransactionLogsConfig, RpcTransactionLogsFilter},
};
use solana_commitment_config::CommitmentConfig;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::{
    db::queries::{get_checkpoint, LAST_SIG_KEY, LAST_SLOT_KEY},
    indexer::{
        processor::Processor,
        rpc::RpcClientWithRetry,
    },
};

pub async fn run_realtime(
    processor: Arc<Processor>,
    rpc: Arc<RpcClientWithRetry>,
    ws_url: String,
    program_id: String,
    batch_size: usize,
    poll_interval: Duration,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    // -----------------------------------------------------------------------
    // Phase 1: Cold-start backfill from last checkpoint
    // -----------------------------------------------------------------------
    let last_sig   = get_checkpoint(&processor.pool, LAST_SIG_KEY).await?;
    let last_slot  = get_checkpoint(&processor.pool, LAST_SLOT_KEY).await?
        .and_then(|s| s.parse::<u64>().ok());

    match (&last_sig, last_slot) {
        (Some(sig), Some(slot)) => {
            info!(last_sig = %sig, last_slot = slot, "Cold start: backfilling from checkpoint");
        }
        (Some(sig), None) => {
            info!(last_sig = %sig, "Cold start: backfilling from signature checkpoint");
        }
        _ => {
            info!("No checkpoint — starting from current tip");
        }
    }

    if let Some(ref sig) = last_sig {
        let missed = rpc.get_signatures_since(&program_id, sig, batch_size).await?;
        if !missed.is_empty() {
            info!(count = missed.len(), "Backfill: processing missed transactions");
            let sigs: Vec<String> = missed.iter().map(|s| s.signature.clone()).collect();
            processor.process_signatures(&sigs).await?;
        } else {
            info!("Backfill: no missed transactions");
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: Real-time WebSocket with automatic reconnect
    // -----------------------------------------------------------------------
    info!(ws_url = %ws_url, "Starting real-time WebSocket indexing");

    let prog_id_clone = program_id.clone();
    let ws_url_clone  = ws_url.clone();
    let (log_tx, mut log_rx) = tokio::sync::mpsc::channel::<String>(1024);

    // WS runs in a blocking thread — reconnects automatically on any error
    tokio::task::spawn_blocking(move || {
        let filter = RpcTransactionLogsFilter::Mentions(vec![prog_id_clone]);
        let config = RpcTransactionLogsConfig {
            commitment: Some(CommitmentConfig::confirmed()),
        };

        let mut reconnect_delay = Duration::from_secs(2);
        const MAX_DELAY: Duration = Duration::from_secs(60);

        loop {
            info!("WebSocket: connecting to {}", ws_url_clone);
            match PubsubClient::logs_subscribe(&ws_url_clone, filter.clone(), config.clone()) {
                Ok((_subscription, receiver)) => {
                    info!("WebSocket: subscription established");
                    // Reset backoff on successful connect
                    reconnect_delay = Duration::from_secs(2);

                    loop {
                        match receiver.recv() {
                            Ok(response) => {
                                let sig = response.value.signature;
                                if log_tx.blocking_send(sig).is_err() {
                                    // Async receiver dropped — shutdown
                                    info!("WebSocket: channel closed, stopping");
                                    return;
                                }
                            }
                            Err(e) => {
                                // crossbeam RecvError — channel disconnected (WS dropped)
                                warn!("WebSocket: stream closed ({e}), reconnecting in {}s", reconnect_delay.as_secs());
                                break;
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("WebSocket: connect failed ({e}), retrying in {}s", reconnect_delay.as_secs());
                }
            }

            std::thread::sleep(reconnect_delay);
            // Exponential backoff: 2s → 4s → 8s → … → 60s
            reconnect_delay = (reconnect_delay * 2).min(MAX_DELAY);
        }
    });

    // Drain signatures from WS thread, batch and flush
    let mut pending: Vec<String> = Vec::new();
    let mut flush_interval = tokio::time::interval(poll_interval);

    loop {
        tokio::select! {
            sig = log_rx.recv() => {
                match sig {
                    Some(s) => {
                        pending.push(s);
                        if pending.len() >= batch_size {
                            let batch = std::mem::take(&mut pending);
                            if let Err(e) = processor.process_signatures(&batch).await {
                                error!("process batch error: {e}");
                            }
                        }
                    }
                    None => {
                        info!("WS log channel closed");
                        break;
                    }
                }
            }

            _ = flush_interval.tick() => {
                if !pending.is_empty() {
                    let batch = std::mem::take(&mut pending);
                    info!(count = batch.len(), "Flushing pending signatures");
                    if let Err(e) = processor.process_signatures(&batch).await {
                        error!("process batch error: {e}");
                    }
                }
            }

            _ = shutdown.changed() => {
                if *shutdown.borrow() {
                    info!("Shutdown signal in realtime loop");
                    if !pending.is_empty() {
                        let batch = std::mem::take(&mut pending);
                        info!(count = batch.len(), "Final flush before shutdown");
                        if let Err(e) = processor.process_signatures(&batch).await {
                            error!("Final flush error: {e}");
                        }
                    }
                    break;
                }
            }
        }
    }

    info!("Real-time indexer stopped");
    Ok(())
}
