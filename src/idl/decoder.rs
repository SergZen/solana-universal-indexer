use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tracing::warn;

use super::Idl;

/// Anchor instruction discriminator: sha256("global:<ix_name>")[..8]
pub fn instruction_discriminator(name: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(format!("global:{name}"));
    let hash = hasher.finalize();
    hash[..8].try_into().unwrap()
}

/// Anchor account discriminator: sha256("account:<AccountName>")[..8]
pub fn account_discriminator(name: &str) -> [u8; 8] {
    let mut hasher = Sha256::new();
    hasher.update(format!("account:{name}"));
    let hash = hasher.finalize();
    hash[..8].try_into().unwrap()
}

pub struct IxDecoder {
    /// (discriminator, ix_name, args_schema)
    handlers: Vec<([u8; 8], String, Vec<Value>)>,
}

impl IxDecoder {
    pub fn new(idl: &Idl) -> Self {
        let handlers = idl
            .instructions
            .iter()
            .filter_map(|ix| {
                let name = ix.get("name").and_then(Value::as_str)?;

                // Anchor >=0.30: discriminator provided explicitly
                // Anchor  <0.30: compute sha256("global:<n>")[..8]
                let disc: [u8; 8] = if let Some(arr) = ix.get("discriminator")
                    .and_then(Value::as_array)
                {
                    let bytes: Vec<u8> = arr.iter()
                        .filter_map(|v| v.as_u64().map(|n| n as u8))
                        .collect();
                    if bytes.len() == 8 {
                        bytes.try_into().ok()?
                    } else {
                        instruction_discriminator(name)
                    }
                } else {
                    instruction_discriminator(name)
                };

                let args = ix
                    .get("args")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();

                tracing::debug!(
                    name, disc = %hex::encode(disc),
                    args = args.len(),
                    "IxDecoder: registered handler"
                );

                Some((disc, name.to_string(), args))
            })
            .collect();
        Self { handlers }
    }

    /// Decode instruction data bytes → (ix_name, decoded_args_json)
    pub fn decode(&self, data: &[u8]) -> Option<(String, Value)> {
        if data.len() < 8 {
            return None;
        }
        let disc: [u8; 8] = data[..8].try_into().ok()?;
        for (handler_disc, name, args) in &self.handlers {
            if *handler_disc == disc {
                let decoded = decode_args(&data[8..], args);
                return Some((name.clone(), decoded));
            }
        }
        None
    }
}

pub struct AccountDecoder {
    /// (discriminator, account_name, fields_schema)
    handlers: Vec<([u8; 8], String, Vec<Value>)>,
}

impl AccountDecoder {
    pub fn new(idl: &Idl) -> Self {
        let handlers = idl
            .accounts
            .iter()
            .filter_map(|acc| {
                let name = acc.get("name").and_then(Value::as_str)?;

                // ── Discriminator ──────────────────────────────────────────
                // Anchor >=0.30: discriminator is provided explicitly as [u8;8]
                // Anchor  <0.30: must compute sha256("account:<Name>")[..8]
                let disc: [u8; 8] = if let Some(arr) = acc.get("discriminator")
                    .and_then(Value::as_array)
                {
                    let bytes: Vec<u8> = arr.iter()
                        .filter_map(|v| v.as_u64().map(|n| n as u8))
                        .collect();
                    if bytes.len() == 8 {
                        bytes.try_into().ok()?
                    } else {
                        account_discriminator(name)
                    }
                } else {
                    account_discriminator(name)
                };

                // ── Fields ────────────────────────────────────────────────
                // Anchor >=0.30: fields live in idl.types under the same name
                // Anchor  <0.30: fields inline in accounts[].type.fields
                let fields = {
                    // Try inline first (legacy)
                    let inline = acc
                        .get("type").and_then(|t| t.get("fields"))
                        .or_else(|| acc.get("fields"))
                        .and_then(Value::as_array);

                    if let Some(f) = inline {
                        f.clone()
                    } else {
                        // Anchor 0.30+: look up in idl.types by name
                        let type_def = idl.types.iter().find(|t| {
                            t.get("name").and_then(Value::as_str) == Some(name)
                        });
                        type_def
                            .and_then(|t| t.get("type").and_then(|tt| tt.get("fields")))
                            .or_else(|| type_def.and_then(|t| t.get("fields")))
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default()
                    }
                };

                tracing::debug!(
                    name, disc = %hex::encode(disc),
                    fields = fields.len(),
                    "AccountDecoder: registered handler"
                );

                Some((disc, name.to_string(), fields))
            })
            .collect();
        Self { handlers }
    }

    /// Decode raw account data → (account_type_name, decoded_fields_json)
    pub fn decode(&self, data: &[u8]) -> Option<(String, Value)> {
        if data.len() < 8 {
            return None;
        }
        let disc: [u8; 8] = data[..8].try_into().ok()?;
        for (handler_disc, name, fields) in &self.handlers {
            if *handler_disc == disc {
                let decoded = decode_struct_fields(&data[8..], fields);
                tracing::debug!(
                    account = %name,
                    fields = fields.len(),
                    data_len = data.len(),
                    "AccountDecoder: decoded account"
                );
                return Some((name.clone(), decoded));
            }
        }
        tracing::debug!(
            disc = %hex::encode(disc),
            "AccountDecoder: no handler for discriminator"
        );
        None
    }

    pub fn has_accounts(&self) -> bool {
        !self.handlers.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Borsh decoder
// ---------------------------------------------------------------------------

struct Reader<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.pos)
    }

    fn read_bytes(&mut self, n: usize) -> Option<&'a [u8]> {
        if self.pos + n > self.data.len() {
            return None;
        }
        let s = &self.data[self.pos..self.pos + n];
        self.pos += n;
        Some(s)
    }

    fn read_u8(&mut self) -> Option<u8> {
        let b = self.data.get(self.pos).copied()?;
        self.pos += 1;
        Some(b)
    }

    fn read_u16(&mut self) -> Option<u16> {
        let b = self.read_bytes(2)?;
        Some(u16::from_le_bytes(b.try_into().ok()?))
    }

    fn read_u32(&mut self) -> Option<u32> {
        let b = self.read_bytes(4)?;
        Some(u32::from_le_bytes(b.try_into().ok()?))
    }

    fn read_u64(&mut self) -> Option<u64> {
        let b = self.read_bytes(8)?;
        Some(u64::from_le_bytes(b.try_into().ok()?))
    }

    fn read_u128(&mut self) -> Option<u128> {
        let b = self.read_bytes(16)?;
        Some(u128::from_le_bytes(b.try_into().ok()?))
    }

    fn read_i8(&mut self) -> Option<i8> {
        Some(self.read_u8()? as i8)
    }

    fn read_i16(&mut self) -> Option<i16> {
        let b = self.read_bytes(2)?;
        Some(i16::from_le_bytes(b.try_into().ok()?))
    }

    fn read_i32(&mut self) -> Option<i32> {
        let b = self.read_bytes(4)?;
        Some(i32::from_le_bytes(b.try_into().ok()?))
    }

    fn read_i64(&mut self) -> Option<i64> {
        let b = self.read_bytes(8)?;
        Some(i64::from_le_bytes(b.try_into().ok()?))
    }

    fn read_i128(&mut self) -> Option<i128> {
        let b = self.read_bytes(16)?;
        Some(i128::from_le_bytes(b.try_into().ok()?))
    }

    fn read_bool(&mut self) -> Option<bool> {
        Some(self.read_u8()? != 0)
    }

    fn read_string(&mut self) -> Option<String> {
        let len = self.read_u32()? as usize;
        let bytes = self.read_bytes(len)?;
        String::from_utf8(bytes.to_vec()).ok()
    }

    fn read_pubkey(&mut self) -> Option<String> {
        let bytes = self.read_bytes(32)?;
        Some(bs58::encode(bytes).into_string())
    }
}

fn decode_type(reader: &mut Reader, ty: &Value) -> Value {
    // Primitive string type
    if let Some(s) = ty.as_str() {
        return decode_primitive_str(reader, s);
    }
    // Object type: { kind: "..." } or { vec: ... } etc.
    if let Some(obj) = ty.as_object() {
        // { vec: inner }
        if let Some(inner) = obj.get("vec") {
            let len = reader.read_u32().unwrap_or(0) as usize;
            let items: Vec<Value> = (0..len).map(|_| decode_type(reader, inner)).collect();
            return Value::Array(items);
        }
        // { option: inner }
        if let Some(inner) = obj.get("option") {
            let is_some = reader.read_u8().unwrap_or(0) != 0;
            if is_some {
                return decode_type(reader, inner);
            } else {
                return Value::Null;
            }
        }
        // { array: [inner, len] }
        if let Some(arr) = obj.get("array").and_then(|a| a.as_array()) {
            if arr.len() == 2 {
                let inner = &arr[0];
                let len = arr[1].as_u64().unwrap_or(0) as usize;
                let items: Vec<Value> = (0..len).map(|_| decode_type(reader, inner)).collect();
                return Value::Array(items);
            }
        }
        // { defined: "TypeName" } — best effort, return hex
        if obj.contains_key("defined") {
            if reader.remaining() > 0 {
                let bytes = reader.data[reader.pos..].to_vec();
                reader.pos = reader.data.len();
                return json!({ "raw_hex": hex::encode(bytes) });
            }
            return Value::Null;
        }
        // { kind: "struct", fields: [...] } (nested struct)
        if let Some(fields) = obj.get("fields").and_then(Value::as_array) {
            return decode_struct_fields_reader(reader, fields);
        }
    }
    Value::Null
}

fn decode_primitive_str(reader: &mut Reader, ty: &str) -> Value {
    match ty {
        "bool" => reader.read_bool().map(Value::Bool).unwrap_or(Value::Null),
        "u8" => reader.read_u8().map(|v| json!(v)).unwrap_or(Value::Null),
        "u16" => reader.read_u16().map(|v| json!(v)).unwrap_or(Value::Null),
        "u32" => reader.read_u32().map(|v| json!(v)).unwrap_or(Value::Null),
        "u64" => reader.read_u64().map(|v| json!(v.to_string())).unwrap_or(Value::Null),
        "u128" => reader.read_u128().map(|v| json!(v.to_string())).unwrap_or(Value::Null),
        "i8" => reader.read_i8().map(|v| json!(v)).unwrap_or(Value::Null),
        "i16" => reader.read_i16().map(|v| json!(v)).unwrap_or(Value::Null),
        "i32" => reader.read_i32().map(|v| json!(v)).unwrap_or(Value::Null),
        "i64" => reader.read_i64().map(|v| json!(v.to_string())).unwrap_or(Value::Null),
        "i128" => reader.read_i128().map(|v| json!(v.to_string())).unwrap_or(Value::Null),
        "f32" => reader
            .read_bytes(4)
            .map(|b| json!(f32::from_le_bytes(b.try_into().unwrap())))
            .unwrap_or(Value::Null),
        "f64" => reader
            .read_bytes(8)
            .map(|b| json!(f64::from_le_bytes(b.try_into().unwrap())))
            .unwrap_or(Value::Null),
        "string" => reader.read_string().map(Value::String).unwrap_or(Value::Null),
        "publicKey" | "pubkey" => reader.read_pubkey().map(Value::String).unwrap_or(Value::Null),
        "bytes" => {
            let len = reader.read_u32().unwrap_or(0) as usize;
            reader
                .read_bytes(len)
                .map(|b| json!(hex::encode(b)))
                .unwrap_or(Value::Null)
        }
        other => {
            warn!(ty = other, "Unknown primitive type, skipping remaining");
            Value::Null
        }
    }
}

fn decode_struct_fields_reader(reader: &mut Reader, fields: &[Value]) -> Value {
    let mut obj = serde_json::Map::new();
    for field in fields {
        let name = field
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("_unknown");
        let ty = match field.get("type") {
            Some(t) => t,
            None => continue,
        };
        obj.insert(name.to_string(), decode_type(reader, ty));
    }
    Value::Object(obj)
}

/// Decode Borsh-encoded struct from bytes using IDL field definitions
pub fn decode_struct_fields(data: &[u8], fields: &[Value]) -> Value {
    let mut reader = Reader::new(data);
    decode_struct_fields_reader(&mut reader, fields)
}

/// Decode instruction arguments from data bytes (after 8-byte discriminator)
fn decode_args(data: &[u8], args: &[Value]) -> Value {
    if args.is_empty() {
        return json!({});
    }
    let mut reader = Reader::new(data);
    let mut obj = serde_json::Map::new();
    for arg in args {
        let name = arg.get("name").and_then(Value::as_str).unwrap_or("_arg");
        let ty = match arg.get("type") {
            Some(t) => t,
            None => continue,
        };
        obj.insert(name.to_string(), decode_type(&mut reader, ty));
    }
    Value::Object(obj)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instruction_discriminator_known() {
        // Anchor uses sha256("global:<name>")[..8]
        let disc = instruction_discriminator("initialize");
        assert_eq!(disc.len(), 8);
        // Should be deterministic
        assert_eq!(disc, instruction_discriminator("initialize"));
        assert_ne!(disc, instruction_discriminator("swap"));
    }

    #[test]
    fn test_account_discriminator_known() {
        let disc = account_discriminator("BondingCurve");
        assert_eq!(disc.len(), 8);
        assert_ne!(disc, account_discriminator("GlobalState"));
    }

    #[test]
    fn test_decode_u8() {
        let mut r = Reader::new(&[42u8]);
        assert_eq!(decode_primitive_str(&mut r, "u8"), json!(42));
    }

    #[test]
    fn test_decode_u64() {
        let v: u64 = 1_000_000_000;
        let bytes = v.to_le_bytes();
        let mut r = Reader::new(&bytes);
        // u64 is returned as string to avoid JS precision loss
        assert_eq!(decode_primitive_str(&mut r, "u64"), json!("1000000000"));
    }

    #[test]
    fn test_decode_bool() {
        let mut r = Reader::new(&[1u8]);
        assert_eq!(decode_primitive_str(&mut r, "bool"), json!(true));
        let mut r = Reader::new(&[0u8]);
        assert_eq!(decode_primitive_str(&mut r, "bool"), json!(false));
    }

    #[test]
    fn test_decode_string() {
        let s = "hello";
        let mut data = (s.len() as u32).to_le_bytes().to_vec();
        data.extend_from_slice(s.as_bytes());
        let mut r = Reader::new(&data);
        assert_eq!(decode_primitive_str(&mut r, "string"), json!("hello"));
    }

    #[test]
    fn test_decode_pubkey() {
        // 32 zero bytes → known base58 string
        let bytes = [0u8; 32];
        let mut r = Reader::new(&bytes);
        let v = decode_primitive_str(&mut r, "publicKey");
        assert!(v.is_string());
        assert_eq!(v.as_str().unwrap().len(), 32); // base58 of 32 zeros
    }

    #[test]
    fn test_decode_struct_fields() {
        use serde_json::json;
        // Struct: { amount: u64, active: bool }
        let fields = vec![
            json!({"name": "amount", "type": "u64"}),
            json!({"name": "active", "type": "bool"}),
        ];
        let amount: u64 = 500;
        let mut data = amount.to_le_bytes().to_vec();
        data.push(1u8); // active = true
        let result = decode_struct_fields(&data, &fields);
        assert_eq!(result["amount"], json!("500"));
        assert_eq!(result["active"], json!(true));
    }

    #[test]
    fn test_ix_decoder_unknown_discriminator() {
        let idl = Idl::default();
        let decoder = IxDecoder::new(&idl);
        let result = decoder.decode(&[0u8; 8]);
        assert!(result.is_none());
    }

    #[test]
    fn test_account_decoder_no_accounts() {
        let idl = Idl::default();
        let decoder = AccountDecoder::new(&idl);
        assert!(!decoder.has_accounts());
    }
}
