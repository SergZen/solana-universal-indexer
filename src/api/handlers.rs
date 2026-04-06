use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::{
    api::state::AppState,
    db::queries::{
        get_account_history, get_account_latest, get_checkpoint, get_transaction,
        ix_call_count, ix_time_series, list_accounts_typed, program_stats,
        query_ix_typed, IxFilter,
    },
    idl::schema::to_snake_case,
    db::queries::LAST_SIG_KEY,
};

type ApiResult<T> = Result<Json<T>, (StatusCode, Json<Value>)>;

fn internal(e: impl std::fmt::Display) -> (StatusCode, Json<Value>) {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": e.to_string() })))
}
fn not_found(msg: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::NOT_FOUND, Json(json!({ "error": msg })))
}
fn bad_request(msg: &str) -> (StatusCode, Json<Value>) {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg })))
}

/// Resolve instruction name → table name from schema, or derive it.
fn ix_table(state: &AppState, ix_name: &str) -> String {
    state.schema.instructions.iter()
        .find(|ix| ix.name == ix_name)
        .map(|ix| ix.table_name.clone())
        .unwrap_or_else(|| format!("ix_{}", to_snake_case(ix_name)))
}

/// Resolve account type name → table name from schema, or derive it.
fn acc_table(state: &AppState, account_type: &str) -> String {
    state.schema.accounts.iter()
        .find(|a| a.name == account_type)
        .map(|a| a.table_name.clone())
        .unwrap_or_else(|| format!("acc_{}", to_snake_case(account_type)))
}

// ---------------------------------------------------------------------------
// GET /health
// ---------------------------------------------------------------------------
pub async fn health(State(state): State<AppState>) -> ApiResult<Value> {
    let checkpoint = get_checkpoint(&state.pool, LAST_SIG_KEY).await.unwrap_or(None);
    Ok(Json(json!({
        "status": "ok",
        "program_id": state.program_id,
        "last_checkpoint": checkpoint,
    })))
}

// ---------------------------------------------------------------------------
// GET /schema
// ---------------------------------------------------------------------------
pub async fn get_schema(State(state): State<AppState>) -> Json<Value> {
    Json(json!(*state.schema))
}

// ---------------------------------------------------------------------------
// GET /tx/{signature}
// ---------------------------------------------------------------------------
pub async fn get_tx(
    State(state): State<AppState>,
    Path(sig): Path<String>,
) -> ApiResult<Value> {
    match get_transaction(&state.pool, &sig).await.map_err(internal)? {
        Some(t) => Ok(Json(json!(t))),
        None    => Err(not_found("transaction not found")),
    }
}

// ---------------------------------------------------------------------------
// GET /ix/{ix_name}?tx_sig=&from_slot=&to_slot=&from=&to=&limit=&offset=
// ---------------------------------------------------------------------------
#[derive(Deserialize)]
pub struct IxQuery {
    pub tx_sig:    Option<String>,
    pub from_slot: Option<i64>,
    pub to_slot:   Option<i64>,
    pub from:      Option<DateTime<Utc>>,
    pub to:        Option<DateTime<Utc>>,
    pub limit:     Option<i64>,
    pub offset:    Option<i64>,
}

pub async fn list_ix(
    State(state): State<AppState>,
    Path(ix_name): Path<String>,
    Query(q): Query<IxQuery>,
) -> ApiResult<Value> {
    let table_name = ix_table(&state, &ix_name);
    let filter = IxFilter {
        tx_sig:    q.tx_sig,
        from_slot: q.from_slot,
        to_slot:   q.to_slot,
        from_time: q.from,
        to_time:   q.to,
        limit:     q.limit.unwrap_or(50).min(1000),
        offset:    q.offset.unwrap_or(0),
    };
    let rows = query_ix_typed(&state.pool, &table_name, &filter)
        .await.map_err(internal)?;
    Ok(Json(json!({ "ix_name": ix_name, "table": table_name, "data": rows, "count": rows.len() })))
}

// ---------------------------------------------------------------------------
// GET /ix/{ix_name}/stats?from=&to=&bucket=hour|day|week
// ---------------------------------------------------------------------------
#[derive(Deserialize)]
pub struct TimeSeriesQuery {
    pub from:   Option<DateTime<Utc>>,
    pub to:     Option<DateTime<Utc>>,
    pub bucket: Option<String>,
}

pub async fn ix_stats(
    State(state): State<AppState>,
    Path(ix_name): Path<String>,
    Query(q): Query<TimeSeriesQuery>,
) -> ApiResult<Value> {
    let table_name = ix_table(&state, &ix_name);
    let bucket = q.bucket.as_deref().unwrap_or("hour");

    let total = ix_call_count(&state.pool, &table_name).await.map_err(internal)?;
    let series = ix_time_series(&state.pool, &table_name, &ix_name, q.from, q.to, bucket)
        .await.map_err(internal)?;

    Ok(Json(json!({
        "ix_name": ix_name,
        "table":   table_name,
        "total_calls": total,
        "bucket":  bucket,
        "series":  series,
    })))
}

// ---------------------------------------------------------------------------
// GET /stats  — program-level overview
// ---------------------------------------------------------------------------
pub async fn stats_handler(State(state): State<AppState>) -> ApiResult<Value> {
    let ix_tables: Vec<String> = state.schema.instructions.iter()
        .map(|ix| ix.table_name.clone()).collect();
    let s = program_stats(&state.pool, &ix_tables).await.map_err(internal)?;
    Ok(Json(s))
}

// ---------------------------------------------------------------------------
// GET /accounts?type=BondingCurve
// ---------------------------------------------------------------------------
#[derive(Deserialize)]
pub struct AccountsQuery {
    #[serde(rename = "type")]
    pub account_type: Option<String>,
}

pub async fn list_accounts_handler(
    State(state): State<AppState>,
    Query(q): Query<AccountsQuery>,
) -> ApiResult<Value> {
    let account_type = match q.account_type {
        Some(t) => t,
        None => return Err(bad_request(
            "query param `type` is required (e.g. ?type=BondingCurve). \
             Use GET /schema to see available account types."
        )),
    };
    let table_name = acc_table(&state, &account_type);
    let rows = list_accounts_typed(&state.pool, &table_name).await.map_err(internal)?;
    Ok(Json(json!({ "account_type": account_type, "table": table_name, "data": rows, "count": rows.len() })))
}

// ---------------------------------------------------------------------------
// GET /accounts/{address}?type=BondingCurve
// ---------------------------------------------------------------------------
pub async fn get_account_handler(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(q): Query<AccountsQuery>,
) -> ApiResult<Value> {
    let account_type = match q.account_type {
        Some(t) => t,
        None => return Err(bad_request(
            "query param `type` is required. Use GET /schema to see available account types."
        )),
    };
    let table_name = acc_table(&state, &account_type);
    match get_account_latest(&state.pool, &table_name, &address).await.map_err(internal)? {
        Some(r) => Ok(Json(json!(r))),
        None    => Err(not_found("account not found")),
    }
}

// ---------------------------------------------------------------------------
// GET /accounts/{address}/history?type=BondingCurve&limit=20
// ---------------------------------------------------------------------------
#[derive(Deserialize)]
pub struct HistoryQuery {
    #[serde(rename = "type")]
    pub account_type: Option<String>,
    pub limit: Option<i64>,
}

pub async fn get_account_history_handler(
    State(state): State<AppState>,
    Path(address): Path<String>,
    Query(q): Query<HistoryQuery>,
) -> ApiResult<Value> {
    let account_type = match q.account_type {
        Some(t) => t,
        None => return Err(bad_request(
            "query param `type` is required. Use GET /schema to see available account types."
        )),
    };
    let table_name = acc_table(&state, &account_type);
    let rows = get_account_history(
        &state.pool, &table_name, &address,
        q.limit.unwrap_or(20).min(100),
    ).await.map_err(internal)?;
    Ok(Json(json!({ "data": rows, "count": rows.len() })))
}
