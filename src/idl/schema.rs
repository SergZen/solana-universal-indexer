use serde_json::Value;
use tracing::info;

use super::Idl;

/// Parsed IDL schema — used for API introspection and DDL generation.
#[derive(Debug, Clone, serde::Serialize)]
pub struct IdlSchema {
    pub program_name: String,
    pub instructions: Vec<IxSchema>,
    pub accounts: Vec<AccountSchema>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct IxSchema {
    pub name: String,
    pub args: Vec<FieldSchema>,
    /// Generated table name: ix_<snake_case_name>
    pub table_name: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct AccountSchema {
    pub name: String,
    pub fields: Vec<FieldSchema>,
    /// Generated table name: acc_<snake_case_name>
    pub table_name: String,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct FieldSchema {
    pub name: String,
    pub ty: String,
}

impl IdlSchema {
    pub fn from_idl(idl: &Idl) -> Self {
        let program_name = idl
            .name
            .clone()
            .unwrap_or_else(|| "unknown".to_string());

        let instructions = idl
            .instructions
            .iter()
            .filter_map(|ix| {
                let name = ix.get("name").and_then(Value::as_str)?.to_string();
                let args = extract_fields(ix.get("args")?);
                let table_name = format!("ix_{}", to_snake_case(&name));
                Some(IxSchema { name, args, table_name })
            })
            .collect();

        // In Anchor >=0.30 the accounts section only contains name + discriminator.
        // Fields live in the `types` section under the same name.
        // We support both formats: fields inline in accounts (legacy) and via types (0.30+).
        let accounts = idl
            .accounts
            .iter()
            .filter_map(|acc| {
                let name = acc.get("name").and_then(Value::as_str)?.to_string();

                // Try inline fields first (Anchor <0.30)
                let fields_val = acc
                    .get("type").and_then(|t| t.get("fields"))
                    .or_else(|| acc.get("fields"));

                let fields = if let Some(fv) = fields_val {
                    extract_fields(fv)
                } else {
                    // Anchor >=0.30: look up fields in idl.types by matching name
                    let type_def = idl.types.iter().find(|t| {
                        t.get("name").and_then(Value::as_str) == Some(&name)
                    })?;
                    let fv = type_def
                        .get("type").and_then(|t| t.get("fields"))
                        .or_else(|| type_def.get("fields"))?;
                    extract_fields(fv)
                };

                let table_name = format!("acc_{}", to_snake_case(&name));
                Some(AccountSchema { name, fields, table_name })
            })
            .collect();

        let schema = IdlSchema { program_name, instructions, accounts };

        info!(
            program = %schema.program_name,
            instructions = schema.instructions.len(),
            account_types = schema.accounts.len(),
            "IDL schema parsed"
        );

        schema
    }

    /// Generate CREATE TABLE / CREATE INDEX / ALTER TABLE ADD COLUMN statements.
    /// Returns one SQL command per element — sqlx does not support multi-statement strings.
    /// Idempotent: safe to run on every startup. New columns are added automatically
    /// when the IDL changes (ALTER TABLE ... ADD COLUMN IF NOT EXISTS).
    pub fn generate_ddl(&self) -> Vec<String> {
        let mut stmts = Vec::new();

        // One table per instruction
        for ix in &self.instructions {
            let mut cols = vec![
                "    id         BIGSERIAL PRIMARY KEY".to_string(),
                "    tx_sig     TEXT NOT NULL".to_string(),
                "    slot       BIGINT NOT NULL".to_string(),
                "    block_time BIGINT".to_string(),
            ];
            for field in &ix.args {
                let pg_type = idl_type_to_pg(&field.ty);
                let col_name = sanitize_col(&field.name);
                cols.push(format!("    \"{col_name}\" {pg_type}"));
            }
            cols.push("    created_at TIMESTAMPTZ NOT NULL DEFAULT now()".to_string());

            let body = cols.join(",\n");
            stmts.push(format!(
                "CREATE TABLE IF NOT EXISTS {tbl} (\n{body}\n)",
                tbl = ix.table_name, body = body,
            ));
            // UNIQUE on tx_sig ensures ON CONFLICT DO NOTHING deduplicates on re-index
            stmts.push(format!(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_{tbl}_tx_sig ON {tbl}(tx_sig)",
                tbl = ix.table_name,
            ));
            stmts.push(format!(
                "CREATE INDEX IF NOT EXISTS idx_{tbl}_slot ON {tbl}(slot DESC)",
                tbl = ix.table_name,
            ));
            // Add any new columns introduced by IDL changes
            for field in &ix.args {
                let pg_type = idl_type_to_pg(&field.ty);
                let col_name = sanitize_col(&field.name);
                stmts.push(format!(
                    "ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS \"{col}\" {ty}",
                    tbl = ix.table_name, col = col_name, ty = pg_type,
                ));
            }
            info!(table = %ix.table_name, "DDL: instruction table");
        }

        // One table per account type
        for acc in &self.accounts {
            let mut cols = vec![
                "    address    TEXT NOT NULL".to_string(),
                "    slot       BIGINT NOT NULL".to_string(),
                "    lamports   BIGINT".to_string(),
            ];
            for field in &acc.fields {
                let pg_type = idl_type_to_pg(&field.ty);
                let col_name = sanitize_col(&field.name);
                cols.push(format!("    \"{col_name}\" {pg_type}"));
            }
            cols.push("    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()".to_string());

            let body = cols.join(",\n");
            stmts.push(format!(
                "CREATE TABLE IF NOT EXISTS {tbl} (\n{body},\n    PRIMARY KEY (address, slot)\n)",
                tbl = acc.table_name, body = body,
            ));
            stmts.push(format!(
                "CREATE INDEX IF NOT EXISTS idx_{tbl}_address ON {tbl}(address)",
                tbl = acc.table_name,
            ));
            stmts.push(format!(
                "CREATE INDEX IF NOT EXISTS idx_{tbl}_slot ON {tbl}(slot DESC)",
                tbl = acc.table_name,
            ));
            // Add any new columns introduced by IDL changes
            for field in &acc.fields {
                let pg_type = idl_type_to_pg(&field.ty);
                let col_name = sanitize_col(&field.name);
                stmts.push(format!(
                    "ALTER TABLE {tbl} ADD COLUMN IF NOT EXISTS \"{col}\" {ty}",
                    tbl = acc.table_name, col = col_name, ty = pg_type,
                ));
            }
            info!(table = %acc.table_name, "DDL: account table");
        }

        stmts
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// CamelCase → snake_case
pub fn to_snake_case(s: &str) -> String {
    let mut out = String::new();
    for (i, ch) in s.chars().enumerate() {
        if ch.is_uppercase() && i > 0 {
            out.push('_');
        }
        out.push(ch.to_lowercase().next().unwrap());
    }
    out
}

/// Sanitize an IDL field name to a safe PostgreSQL column name.
/// Strips non-alphanumeric chars, lowercases, truncates to 59 chars.
pub fn sanitize_col(name: &str) -> String {
    let name = to_snake_case(name);

    let s: String = name
        .chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect();
    let s = s.to_lowercase();
    // Avoid reserved words by prefixing with f_ if needed
    let reserved = ["authorization", "between", "case", "cast", "check", "collate", "column", "constraint", "create", "cross", "current_date", "current_role", "current_time", "current_timestamp", "current_user", "data", "default", "desc", "distinct", "drop", "else", "end", "fetch", "for", "foreign", "from", "grant", "group", "in", "index", "into", "join", "like", "limit", "not", "null", "offset", "on", "only", "open", "or", "order", "primary", "references", "returning", "row", "select", "session", "some", "table", "then", "to", "trigger", "union", "unique", "user", "using", "value", "values", "where", "with"];
    if reserved.contains(&s.as_str()) {
        format!("f_{}", &s[..s.len().min(57)])
    } else {
        s[..s.len().min(59)].to_string()
    }
}

/// Map IDL type string → PostgreSQL type.
fn idl_type_to_pg(ty: &str) -> String {
    // Handle Option<T> → nullable T
    if let Some(inner) = ty.strip_prefix("Option<").and_then(|s| s.strip_suffix('>')) {
        return idl_type_to_pg(inner); // nullable by default in PG
    }
    // Handle Vec<T> and arrays → JSONB
    if ty.starts_with("Vec<") || ty.starts_with('[') {
        return "JSONB".to_string();
    }
    match ty {
        "bool" | "OptionBool"   => "BOOLEAN",
        "u8" | "u16" | "u32"
        | "i8" | "i16" | "i32"  => "INTEGER",
        "u64" | "i64"           => "BIGINT",
        "u128" | "i128"         => "NUMERIC",
        "f32"                   => "REAL",
        "f64"                   => "DOUBLE PRECISION",
        "string"                => "TEXT",
        "publicKey" | "pubkey"  => "TEXT",
        "bytes"                 => "BYTEA",
        _                       => "JSONB", // defined/complex types
    }
    .to_string()
}

fn extract_fields(val: &Value) -> Vec<FieldSchema> {
    val.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|f| {
                    let name = f.get("name").and_then(Value::as_str)?.to_string();
                    let ty = type_to_string(f.get("type")?);
                    Some(FieldSchema { name, ty })
                })
                .collect()
        })
        .unwrap_or_default()
}

fn type_to_string(val: &Value) -> String {
    match val {
        Value::String(s) => s.clone(),
        Value::Object(obj) => {
            if let Some(inner) = obj.get("vec") {
                format!("Vec<{}>", type_to_string(inner))
            } else if let Some(inner) = obj.get("option") {
                format!("Option<{}>", type_to_string(inner))
            } else if let Some(arr) = obj.get("array").and_then(|a| a.as_array()) {
                if arr.len() == 2 {
                    format!("[{}; {}]", type_to_string(&arr[0]), arr[1])
                } else {
                    "array".to_string()
                }
            } else if let Some(name) = obj.get("defined")
                .and_then(|d| d.get("name"))
                .and_then(Value::as_str)
                .filter(|&n| n == "OptionBool")
            {
                name.to_string()
            } else if let Some(hex) = obj.get("raw_hex").and_then(Value::as_str) {
                match hex.len() {
                    2 => "bool".to_string(),
                    _ => "complex".to_string(),
                }
            } else {
                "complex".to_string()
            }
        }
        _ => "unknown".to_string(),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_snake_case() {
        assert_eq!(to_snake_case("BondingCurve"), "bonding_curve");
        assert_eq!(to_snake_case("GlobalState"), "global_state");
        assert_eq!(to_snake_case("buy"), "buy");
    }

    #[test]
    fn test_idl_type_to_pg() {
        assert_eq!(idl_type_to_pg("u64"), "BIGINT");
        assert_eq!(idl_type_to_pg("bool"), "BOOLEAN");
        assert_eq!(idl_type_to_pg("publicKey"), "TEXT");
        assert_eq!(idl_type_to_pg("Option<u64>"), "BIGINT");
        assert_eq!(idl_type_to_pg("Vec<u8>"), "JSONB");
        assert_eq!(idl_type_to_pg("u128"), "NUMERIC");
    }

    #[test]
    fn test_generate_ddl_from_idl() {
        use crate::idl::loader::Idl;
        use serde_json::json;

        let idl: Idl = serde_json::from_value(json!({
            "name": "test",
            "instructions": [{
                "name": "buy",
                "args": [
                    { "name": "amount", "type": "u64" },
                    { "name": "maxSolCost", "type": "u64" }
                ]
            }],
            "accounts": [{
                "name": "BondingCurve",
                "type": { "kind": "struct", "fields": [
                    { "name": "virtualTokenReserves", "type": "u64" },
                    { "name": "complete", "type": "bool" }
                ]}
            }]
        })).unwrap();

        let schema = IdlSchema::from_idl(&idl);
        let ddl = schema.generate_ddl();

        assert_eq!(ddl.len(), 10);

        let ix_table = ddl.iter().find(|s| s.contains("CREATE TABLE IF NOT EXISTS ix_buy")).unwrap();
        assert!(ix_table.contains("\"amount\" BIGINT"));
        assert!(ix_table.contains("\"max_sol_cost\" BIGINT"));

        let acc_table = ddl.iter().find(|s| s.contains("CREATE TABLE IF NOT EXISTS acc_bonding_curve")).unwrap();
        assert!(acc_table.contains("\"virtual_token_reserves\" BIGINT"));
        assert!(acc_table.contains("\"complete\" BOOLEAN"));
    }
}
