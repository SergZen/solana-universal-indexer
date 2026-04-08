mod api;
mod config;
mod db;
mod idl;
mod indexer;

use anyhow::Result;
use std::sync::Arc;
use tokio::sync::watch;
use tracing::{error, info};
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use crate::{
    api::{router::build_router, state::AppState},
    config::{Config, IndexerMode},
    db::{connect, queries::{get_checkpoint, save_slot_checkpoint_direct, LAST_SLOT_KEY}},
    idl::{loader::load_idl, schema::IdlSchema},
    indexer::{
        processor::Processor,
        realtime::run_realtime,
        rpc::RpcClientWithRetry,
    },
};

#[tokio::main]
async fn main() -> Result<()> {
    let use_json = std::env::var("LOG_FORMAT")
        .map(|v| v.to_lowercase() == "json")
        .unwrap_or(false);

    if use_json {
        tracing_subscriber::registry()
            .with(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
            .with(fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(EnvFilter::from_default_env().add_directive("info".parse().unwrap()))
            .with(fmt::layer().pretty())
            .init();
    }

    let cfg = Config::from_env()?;
    info!(mode = ?cfg.mode, program_id = %cfg.program_id, "Solana indexer starting");

    // ── IDL (load before DB so we can generate schema) ───────────────────
    let rpc = Arc::new(RpcClientWithRetry::new(&cfg.rpc_url, cfg.rpc_max_retries));
    let idl = load_idl(&cfg.idl_source, &rpc.client, &cfg.program_id)?;
    let schema = Arc::new(IdlSchema::from_idl(&idl));

    // ── DB (runs base migrations + generates typed tables from IDL) ───────
    let pool = Arc::new(connect(&cfg.database_url, &schema).await?);

    // ── Graceful shutdown ─────────────────────────────────────────────────
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    {
        let shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            #[cfg(unix)]
            {
                use tokio::signal::unix::{signal, SignalKind};
                let mut sigterm = signal(SignalKind::terminate()).unwrap();
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            #[cfg(not(unix))]
            { tokio::signal::ctrl_c().await.ok(); }
            info!("Shutdown signal received, finishing current batch…");
            let _ = shutdown_tx.send(true);
        });
    }

    // ── REST API ──────────────────────────────────────────────────────────
    let app_state = AppState {
        pool: pool.clone(),
        schema: schema.clone(),
        program_id: cfg.program_id.clone(),
    };
    let router = build_router(app_state);
    let api_addr = format!("{}:{}", cfg.api_host, cfg.api_port);
    let listener = tokio::net::TcpListener::bind(&api_addr).await?;
    info!(addr = %api_addr, "API server listening");

    let mut api_shutdown = shutdown_rx.clone();
    let api_handle = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                loop {
                    if api_shutdown.changed().await.is_err() { break; }
                    if *api_shutdown.borrow() { break; }
                }
            })
            .await
            .ok();
        info!("API server stopped");
    });

    // ── Indexer ───────────────────────────────────────────────────────────
    let processor = Arc::new(Processor::new(
        rpc.clone(),
        (*pool).clone(),
        cfg.program_id.clone(),
        &idl,
        &schema,
        cfg.batch_size,
        shutdown_rx.clone(),
    ));

    let indexer_handle = match cfg.mode {
        IndexerMode::Batch => {
            let processor = processor.clone();
            let cfg = cfg.clone();
            let pool_clone = (*pool).clone();
            tokio::spawn(async move {
                if let Err(e) = run_batch(processor, rpc, cfg, &pool_clone).await {
                    error!("Batch indexer error: {e}");
                }
            })
        }
        IndexerMode::Realtime => {
            let processor = processor.clone();
            let ws_url = cfg.ws_url.clone();
            let program_id = cfg.program_id.clone();
            let batch_size = cfg.batch_size;
            let poll_interval = cfg.poll_interval();
            let shutdown_rx2 = shutdown_rx.clone();
            tokio::spawn(async move {
                if let Err(e) = run_realtime(
                    processor, rpc, ws_url, program_id,
                    batch_size, poll_interval, shutdown_rx2,
                ).await {
                    error!("Realtime indexer error: {e}");
                }
            })
        }
    };

    if cfg.mode == IndexerMode::Batch {
        let _ = indexer_handle.await;
        info!("Batch indexing finished. API remains active. Press Ctrl+C to stop.");

        let _ = api_handle.await;
    } else {
        tokio::select! {
            _ = indexer_handle => info!("Indexer stopped"),
            _ = api_handle => info!("API server stopped"),
        }
    }

    info!("Solana indexer shut down cleanly");
    Ok(())
}

async fn save_slot_checkpoint(pool: &sqlx::PgPool, slot: u64) -> Result<()> {
    save_slot_checkpoint_direct(pool, slot).await
}

async fn run_batch(
    processor: Arc<Processor>,
    rpc: Arc<RpcClientWithRetry>,
    cfg: Config,
    pool: &sqlx::PgPool,
) -> Result<()> {
    // Explicit signature list
    if !cfg.signatures.is_empty() {
        info!(count = cfg.signatures.len(), "Batch mode: explicit signature list");
        processor.process_signatures(&cfg.signatures).await?;
        info!("Batch complete");
        return Ok(());
    }

    // Slot range via getBlock
    if let (Some(start), Some(end)) = (cfg.start_slot, cfg.end_slot) {
        let saved_slot = get_checkpoint(pool, LAST_SLOT_KEY)
            .await
            .unwrap_or(None)
            .and_then(|s| s.parse::<u64>().ok());

        // If last saved slot == end_slot — range fully scanned, nothing to do
        if let Some(last) = saved_slot {
            if last >= end {
                info!(
                    last_indexed_slot = last,
                    end_slot = end,
                    "Range fully indexed, skipping"
                );
                return Ok(());
            }
        }

        // Resume from saved slot + 1, or from start
        let resume_from = saved_slot
            .filter(|&last| last >= start && last < end)
            .map(|last| {
                info!(last_slot = last, resume = last + 1, "Resuming batch from checkpoint");
                last + 1
            })
            .unwrap_or(start);

        info!(
            start = resume_from, end,
            program_id = %cfg.program_id,
            "Batch mode: slot range via getBlock"
        );

        let sigs = rpc
            .get_signatures_for_slot_range(&cfg.program_id, resume_from, end, cfg.batch_size)
            .await?;

        info!(count = sigs.len(), start = resume_from, end, "Signatures found in slot range");

        if sigs.is_empty() {
            tracing::warn!(
                start = resume_from, end, program_id = %cfg.program_id,
                "No signatures found in range"
            );
        } else {
            if let Some(first) = sigs.first() {
                info!(slot = first.slot, sig = %first.signature, "First signature in range");
            }
            if let Some(last) = sigs.last() {
                info!(slot = last.slot, sig = %last.signature, "Last signature in range");
            }
            let sig_strings: Vec<String> = sigs.iter().map(|s| s.signature.clone()).collect();
            processor.process_signatures(&sig_strings).await?;
        }

        // Always save end_slot as checkpoint — marks this range as fully scanned
        // even if no matching transactions were found
        save_slot_checkpoint(pool, end).await?;
        info!(end_slot = end, "Batch complete — end slot saved to checkpoint");
        return Ok(());
    }

    // No range specified — fetch latest N signatures
    info!(program_id = %cfg.program_id, limit = cfg.batch_size, "Batch mode: latest signatures");
    let sigs = rpc
        .get_signatures_page(&cfg.program_id, None, None, cfg.batch_size)
        .await?;
    info!(count = sigs.len(), "Latest signatures fetched");
    if sigs.is_empty() {
        tracing::warn!(
            program_id = %cfg.program_id,
            "No signatures found. Check PROGRAM_ID is correct and has transactions on this network"
        );
        return Ok(());
    }
    let sig_strings: Vec<String> = sigs.iter().map(|s| s.signature.clone()).collect();
    processor.process_signatures(&sig_strings).await?;
    info!("Batch complete");
    Ok(())
}
