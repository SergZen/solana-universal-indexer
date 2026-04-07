use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Row, Transaction};

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize, sqlx::FromRow)]
pub struct TxRow {
    pub signature: String,
    pub slot: i64,
    pub block_time: Option<i64>,
    pub err: Option<Value>,
    pub created_at: DateTime<Utc>,
}

/// A row from a dynamic ix_<name> table.
/// We can't know columns at compile time, so we return the whole row as JSONB.
#[derive(Debug, Serialize, Deserialize)]
pub struct IxRow {
    pub tx_sig: String,
    pub slot: i64,
    pub block_time: Option<i64>,
    pub created_at: DateTime<Utc>,
    pub args: Value, // all decoded arg columns as a JSON object
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AccountStateRow {
    pub address: String,
    pub slot: i64,
    pub lamports: Option<i64>,
    pub data: Value,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IxStats {
    pub ix_name: String,
    pub calls: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IxTimeSeries {
    pub ix_name: String,
    pub period: DateTime<Utc>,
    pub calls: i64,
}

// ---------------------------------------------------------------------------
// Smart value binding — converts JSON value to the correct PostgreSQL type.
// sqlx binds serde_json::Value as JSONB/text by default, which causes type
// mismatch errors when the column is BIGINT, INTEGER, BOOLEAN, etc.
// ---------------------------------------------------------------------------

/// Bind a serde_json::Value to a query with correct PG type inference.
/// u64 values are stored as strings in JSON (to avoid JS precision loss) but
/// must be bound as i64 for BIGINT columns.
fn bind_value<'q>(
    q: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    v: &'q Value,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match v {
        Value::Bool(b) => q.bind(b),
        Value::Object(map) if map.contains_key("raw_hex") => {
            let hex_str = map.get("raw_hex").and_then(|h| h.as_str());
            
            match hex_str {
                Some(s) if s.len() == 2 => q.bind(s == "01"),
                Some(_) | None => q.bind(v),
            }
        }
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                q.bind(i)
            } else if let Some(f) = n.as_f64() {
                q.bind(f)
            } else {
                q.bind(v.to_string())
            }
        }
        // Strings that look like integers (u64/u128 serialized as strings)
        Value::String(s) => {
            if let Ok(i) = s.parse::<i64>() {
                q.bind(i)
            } else {
                q.bind(s.as_str())
            }
        }
        // Arrays and objects → JSONB
        _ => q.bind(v),
    }
}

// ---------------------------------------------------------------------------
// Checkpoint
// ---------------------------------------------------------------------------

pub async fn get_checkpoint(pool: &PgPool, key: &str) -> Result<Option<String>> {
    let row = sqlx::query("SELECT value FROM indexer_state WHERE key = $1")
        .bind(key)
        .fetch_optional(pool)
        .await?;
    Ok(row.map(|r| r.get::<String, _>("value")))
}

pub const LAST_SIG_KEY:  &str = "last_indexed_sig";
pub const LAST_SLOT_KEY: &str = "last_indexed_slot";

/// Save slot checkpoint outside of a transaction — used after batch scan completes.
pub async fn save_slot_checkpoint_direct(pool: &PgPool, slot: u64) -> anyhow::Result<()> {
    sqlx::query(
        r#"INSERT INTO indexer_state (key, value, updated_at)
           VALUES ($1, $2, now())
           ON CONFLICT (key) DO UPDATE
             SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at"#,
    )
    .bind(LAST_SLOT_KEY)
    .bind(slot.to_string())
    .execute(pool)
    .await?;
    Ok(())
}

pub async fn upsert_checkpoint(
    tx: &mut Transaction<'_, Postgres>,
    key: &str,
    value: &str,
) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO indexer_state (key, value, updated_at)
           VALUES ($1, $2, now())
           ON CONFLICT (key) DO UPDATE
             SET value = EXCLUDED.value, updated_at = EXCLUDED.updated_at"#,
    )
    .bind(key)
    .bind(value)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

pub async fn upsert_transaction(
    tx: &mut Transaction<'_, Postgres>,
    signature: &str,
    slot: i64,
    block_time: Option<i64>,
    err: Option<&Value>,
) -> Result<()> {
    sqlx::query(
        r#"INSERT INTO transactions (signature, slot, block_time, err)
           VALUES ($1, $2, $3, $4)
           ON CONFLICT (signature) DO NOTHING"#,
    )
    .bind(signature)
    .bind(slot)
    .bind(block_time)
    .bind(err)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub async fn get_transaction(pool: &PgPool, signature: &str) -> Result<Option<TxRow>> {
    Ok(sqlx::query_as::<_, TxRow>(
        "SELECT signature, slot, block_time, err, created_at \
         FROM transactions WHERE signature = $1",
    )
    .bind(signature)
    .fetch_optional(pool)
    .await?)
}

// ---------------------------------------------------------------------------
// Typed instruction tables: ix_<name>
// ---------------------------------------------------------------------------

/// Insert decoded instruction call into its typed table.
pub async fn upsert_ix_typed(
    tx: &mut Transaction<'_, Postgres>,
    table_name: &str,
    tx_sig: &str,
    slot: i64,
    block_time: Option<i64>,
    data: &Value,
) -> Result<()> {
    let obj = match data.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    let cols = vec!["tx_sig", "slot", "block_time"];
    let placeholders = vec!["$1", "$2", "$3"];
    let mut idx = 4usize;
    let mut extra_cols: Vec<String> = Vec::new();
    let mut extra_phs: Vec<String> = Vec::new();
    let mut values: Vec<Value> = vec![
        Value::String(tx_sig.to_string()),
        Value::Number(slot.into()),
        block_time.map(|v| Value::Number(v.into())).unwrap_or(Value::Null),
    ];

    for (key, val) in obj {
        if val.is_null() {
            continue;
        }
        extra_cols.push(format!("\"{}\"", sanitize_col_runtime(key)));
        extra_phs.push(format!("${idx}"));
        values.push(val.clone());
        idx += 1;
    }

    let all_cols: Vec<&str> = cols.iter().copied()
        .chain(extra_cols.iter().map(|s| s.as_str()))
        .collect();
    let all_phs: Vec<&str> = placeholders.iter().copied()
        .chain(extra_phs.iter().map(|s| s.as_str()))
        .collect();

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT DO NOTHING",
        table_name,
        all_cols.join(", "),
        all_phs.join(", "),
    );

    let mut q = sqlx::query(&sql);
    for v in &values { q = bind_value(q, v); }
    q.execute(&mut **tx).await?;
    Ok(())
}

/// Query rows from a specific ix_* table with optional filters.
/// Returns each row as JSON (args embedded in the row object).
pub struct IxFilter {
    pub tx_sig: Option<String>,
    pub from_slot: Option<i64>,
    pub to_slot: Option<i64>,
    pub from_time: Option<DateTime<Utc>>,
    pub to_time: Option<DateTime<Utc>>,
    pub limit: i64,
    pub offset: i64,
}

impl Default for IxFilter {
    fn default() -> Self {
        Self {
            tx_sig: None,
            from_slot: None,
            to_slot: None,
            from_time: None,
            to_time: None,
            limit: 50,
            offset: 0,
        }
    }
}

pub async fn query_ix_typed(
    pool: &PgPool,
    table_name: &str,
    f: &IxFilter,
) -> Result<Vec<Value>> {
    let mut conditions: Vec<String> = Vec::new();
    let mut param_idx = 1usize;

    if f.tx_sig.is_some() {
        conditions.push(format!("tx_sig = ${param_idx}"));
        param_idx += 1;
    }
    if f.from_slot.is_some() {
        conditions.push(format!("slot >= ${param_idx}"));
        param_idx += 1;
    }
    if f.to_slot.is_some() {
        conditions.push(format!("slot <= ${param_idx}"));
        param_idx += 1;
    }
    if f.from_time.is_some() {
        conditions.push(format!("created_at >= ${param_idx}"));
        param_idx += 1;
    }
    if f.to_time.is_some() {
        conditions.push(format!("created_at <= ${param_idx}"));
        param_idx += 1;
    }

    let where_clause = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    // Return full row as JSONB — columns are unknown at compile time
    let sql = format!(
        "SELECT to_jsonb({tbl}.*) AS row FROM {tbl} {where} \
         ORDER BY created_at DESC LIMIT ${limit_idx} OFFSET ${offset_idx}",
        tbl = table_name,
        where = where_clause,
        limit_idx = param_idx,
        offset_idx = param_idx + 1,
    );

    let mut q = sqlx::query(&sql);
    if let Some(v) = &f.tx_sig    { q = q.bind(v); }
    if let Some(v) = f.from_slot  { q = q.bind(v); }
    if let Some(v) = f.to_slot    { q = q.bind(v); }
    if let Some(v) = f.from_time  { q = q.bind(v); }
    if let Some(v) = f.to_time    { q = q.bind(v); }
    q = q.bind(f.limit).bind(f.offset);

    let rows = q.fetch_all(pool).await?;
    Ok(rows.into_iter().map(|r| r.get::<Value, _>("row")).collect())
}

/// Call count for a specific instruction table.
pub async fn ix_call_count(pool: &PgPool, table_name: &str) -> Result<i64> {
    let row = sqlx::query(&format!(
        "SELECT COUNT(*)::bigint AS c FROM {}", table_name
    ))
    .fetch_one(pool)
    .await?;
    Ok(row.get("c"))
}

/// Time-series aggregation for a specific ix_* table.
pub async fn ix_time_series(
    pool: &PgPool,
    table_name: &str,
    ix_name: &str,
    from: Option<DateTime<Utc>>,
    to: Option<DateTime<Utc>>,
    bucket: &str,
) -> Result<Vec<IxTimeSeries>> {
    let safe_bucket = match bucket { "day" => "day", "week" => "week", _ => "hour" };
    let from_ts = from.unwrap_or_else(|| Utc::now() - chrono::Duration::days(7));
    let to_ts   = to.unwrap_or_else(|| Utc::now());

    let sql = format!(
        "SELECT DATE_TRUNC('{bucket}', created_at) AS period, COUNT(*)::bigint AS calls
         FROM {tbl}
         WHERE created_at BETWEEN $1 AND $2
         GROUP BY DATE_TRUNC('{bucket}', created_at)
         ORDER BY period DESC",
        bucket = safe_bucket,
        tbl = table_name,
    );

    let ix_name = ix_name.to_string();
    let rows = sqlx::query(&sql)
        .bind(from_ts)
        .bind(to_ts)
        .fetch_all(pool)
        .await?;

    Ok(rows.into_iter().map(|r| IxTimeSeries {
        ix_name: ix_name.clone(),
        period: r.get("period"),
        calls:  r.get("calls"),
    }).collect())
}

// ---------------------------------------------------------------------------
// Typed account tables: acc_<name>
// ---------------------------------------------------------------------------

pub async fn upsert_account_typed(
    tx: &mut Transaction<'_, Postgres>,
    table_name: &str,
    address: &str,
    slot: i64,
    lamports: Option<i64>,
    data: &Value,
) -> Result<()> {
    let obj = match data.as_object() {
        Some(o) => o,
        None => return Ok(()),
    };

    let mut cols = vec!["address".to_string(), "slot".to_string(), "lamports".to_string()];
    let mut phs  = vec!["$1".to_string(), "$2".to_string(), "$3".to_string()];
    let mut idx  = 4usize;
    let mut values: Vec<Value> = vec![
        Value::String(address.to_string()),
        Value::Number(slot.into()),
        lamports.map(|v| Value::Number(v.into())).unwrap_or(Value::Null),
    ];

    for (key, val) in obj {
        cols.push(format!("\"{}\"", sanitize_col_runtime(key)));
        phs.push(format!("${idx}"));
        values.push(val.clone());
        idx += 1;
    }

    let update_set = cols.iter().skip(2)
        .map(|c| format!("{c} = EXCLUDED.{c}"))
        .collect::<Vec<_>>().join(", ");

    let sql = format!(
        "INSERT INTO {} ({}) VALUES ({}) \
         ON CONFLICT (address, slot) DO UPDATE SET {}, updated_at = now()",
        table_name,
        cols.join(", "),
        phs.join(", "),
        update_set,
    );

    let mut q = sqlx::query(&sql);
    for v in &values { q = bind_value(q, v); }
    q.execute(&mut **tx).await?;
    Ok(())
}

pub async fn get_account_latest(
    pool: &PgPool,
    table_name: &str,
    address: &str,
) -> Result<Option<AccountStateRow>> {
    let sql = format!(
        "SELECT address, slot, lamports, updated_at, \
         to_jsonb({tbl}.*) - 'address' - 'slot' - 'lamports' - 'updated_at' AS data \
         FROM {tbl} WHERE address = $1 ORDER BY slot DESC LIMIT 1",
        tbl = table_name,
    );
    let row = sqlx::query(&sql).bind(address).fetch_optional(pool).await?;
    Ok(row.map(|r| AccountStateRow {
        address:    r.get("address"),
        slot:       r.get("slot"),
        lamports:   r.get("lamports"),
        data:       r.get("data"),
        updated_at: r.get("updated_at"),
    }))
}

pub async fn get_account_history(
    pool: &PgPool,
    table_name: &str,
    address: &str,
    limit: i64,
) -> Result<Vec<AccountStateRow>> {
    let sql = format!(
        "SELECT address, slot, lamports, updated_at, \
         to_jsonb({tbl}.*) - 'address' - 'slot' - 'lamports' - 'updated_at' AS data \
         FROM {tbl} WHERE address = $1 ORDER BY slot DESC LIMIT $2",
        tbl = table_name,
    );
    let rows = sqlx::query(&sql).bind(address).bind(limit).fetch_all(pool).await?;
    Ok(rows.into_iter().map(|r| AccountStateRow {
        address:    r.get("address"),
        slot:       r.get("slot"),
        lamports:   r.get("lamports"),
        data:       r.get("data"),
        updated_at: r.get("updated_at"),
    }).collect())
}

pub async fn list_accounts_typed(
    pool: &PgPool,
    table_name: &str,
) -> Result<Vec<AccountStateRow>> {
    let sql = format!(
        "SELECT DISTINCT ON (address) address, slot, lamports, updated_at, \
         to_jsonb({tbl}.*) - 'address' - 'slot' - 'lamports' - 'updated_at' AS data \
         FROM {tbl} ORDER BY address, slot DESC",
        tbl = table_name,
    );
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    Ok(rows.into_iter().map(|r| AccountStateRow {
        address:    r.get("address"),
        slot:       r.get("slot"),
        lamports:   r.get("lamports"),
        data:       r.get("data"),
        updated_at: r.get("updated_at"),
    }).collect())
}

// ---------------------------------------------------------------------------
// Program-level stats (across all ix_* tables via schema)
// ---------------------------------------------------------------------------

pub async fn program_stats(
    pool: &PgPool,
    ix_table_names: &[String],
) -> Result<Value> {
    let total_txs: i64 = sqlx::query("SELECT COUNT(*)::bigint AS c FROM transactions")
        .fetch_one(pool).await?.get("c");

    let first_slot: Option<i64> = sqlx::query("SELECT MIN(slot) AS s FROM transactions")
        .fetch_one(pool).await?.get("s");
    let last_slot: Option<i64> = sqlx::query("SELECT MAX(slot) AS s FROM transactions")
        .fetch_one(pool).await?.get("s");

    let last_checkpoint: Option<String> =
        sqlx::query("SELECT value FROM indexer_state WHERE key = 'last_indexed_sig'")
            .fetch_optional(pool).await?
            .map(|r| r.get("value"));

    // Count calls per instruction table
    let mut ix_counts = serde_json::Map::new();
    let mut total_ix_calls: i64 = 0;
    for tbl in ix_table_names {
        let count: i64 = sqlx::query(&format!("SELECT COUNT(*)::bigint AS c FROM {tbl}"))
            .fetch_one(pool).await?.get("c");
        ix_counts.insert(tbl.clone(), Value::Number(count.into()));
        total_ix_calls += count;
    }

    Ok(serde_json::json!({
        "total_transactions":   total_txs,
        "total_ix_calls":       total_ix_calls,
        "ix_call_counts":       ix_counts,
        "first_indexed_slot":   first_slot,
        "last_indexed_slot":    last_slot,
        "last_checkpoint":      last_checkpoint,
    }))
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn sanitize_col_runtime(name: &str) -> String {
    let s: String = name.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    let s = to_snake_runtime(&s);
    let reserved = ["authorization", "between", "case", "cast", "check", "collate", "column", "constraint", "create", "cross", "current_date", "current_role", "current_time", "current_timestamp", "current_user", "data", "default", "desc", "distinct", "drop", "else", "end", "fetch", "for", "foreign", "from", "grant", "group", "in", "index", "into", "join", "like", "limit", "not", "null", "offset", "on", "only", "open", "or", "order", "primary", "references", "returning", "row", "select", "session", "some", "table", "then", "to", "trigger", "union", "unique", "user", "using", "value", "values", "where", "with"];
    if reserved.contains(&s.as_str()) {
        format!("f_{}", &s[..s.len().min(57)])
    } else {
        s[..s.len().min(59)].to_string()
    }
}

fn to_snake_runtime(s: &str) -> String {
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 { out.push('_'); }
        out.push(ch.to_lowercase().next().unwrap());
    }
    out
}
