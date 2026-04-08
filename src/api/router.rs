use axum::{Router, routing::get};
use http::Method;
use tower_http::{cors::{Any, CorsLayer}, trace::TraceLayer};

use crate::api::{handlers::*, state::AppState};

pub fn build_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET])
        .allow_origin(Any)
        .allow_headers(Any);

    Router::new()
        .route("/", get(|| async { "Indexer is running!" }))
        // Health & schema
        .route("/health",  get(health))
        .route("/schema",  get(get_schema))
        // Transactions
        .route("/tx/{signature}", get(get_tx))
        // Per-instruction: list calls + stats
        .route("/ix/{ix_name}",       get(list_ix))
        .route("/ix/{ix_name}/stats", get(ix_stats))
        // Program-wide stats
        .route("/stats", get(stats_handler))
        // Account states
        .route("/accounts",                       get(list_accounts_handler))
        .route("/accounts/{address}",             get(get_account_handler))
        .route("/accounts/{address}/history",     get(get_account_history_handler))
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}
