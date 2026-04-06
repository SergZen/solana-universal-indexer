use serde_json::json;
use solana_indexer::idl::{
    decoder::{account_discriminator, instruction_discriminator, AccountDecoder, IxDecoder},
    loader::Idl,
};

fn minimal_idl() -> Idl {
    serde_json::from_value(json!({
        "version": "0.1.0",
        "name": "test_program",
        "instructions": [
            {
                "name": "initialize",
                "accounts": [],
                "args": [
                    { "name": "amount",  "type": "u64"  },
                    { "name": "enabled", "type": "bool" }
                ]
            }
        ],
        "accounts": [
            {
                "name": "Vault",
                "type": {
                    "kind": "struct",
                    "fields": [
                        { "name": "owner",   "type": "publicKey" },
                        { "name": "balance", "type": "u64"       },
                        { "name": "active",  "type": "bool"      }
                    ]
                }
            }
        ]
    }))
    .unwrap()
}

// Discriminator tests

#[test]
fn discriminator_is_deterministic() {
    assert_eq!(
        instruction_discriminator("initialize"),
        instruction_discriminator("initialize")
    );
    assert_ne!(
        instruction_discriminator("initialize"),
        instruction_discriminator("swap")
    );
}

#[test]
fn account_discriminator_differs_from_ix() {
    let ix   = instruction_discriminator("initialize");
    let acc  = account_discriminator("initialize");
    assert_ne!(ix, acc);
}

// IxDecoder tests

#[test]
fn ix_decoder_decodes_initialize() {
    let idl = minimal_idl();
    let decoder = IxDecoder::new(&idl);

    // Build raw bytes: 8-byte discriminator + u64(1000) + bool(true)
    let disc = instruction_discriminator("initialize");
    let amount: u64 = 1000;
    let mut data = disc.to_vec();
    data.extend_from_slice(&amount.to_le_bytes());
    data.push(1u8); // enabled = true

    let result = decoder.decode(&data);
    assert!(result.is_some(), "should decode known discriminator");
    let (name, args) = result.unwrap();
    assert_eq!(name, "initialize");
    assert_eq!(args["amount"], json!("1000"));
    assert_eq!(args["enabled"], json!(true));
}

#[test]
fn ix_decoder_returns_none_for_unknown() {
    let idl = minimal_idl();
    let decoder = IxDecoder::new(&idl);
    let data = [0u8; 8]; // unknown discriminator
    assert!(decoder.decode(&data).is_none());
}

#[test]
fn ix_decoder_returns_none_for_short_data() {
    let idl = minimal_idl();
    let decoder = IxDecoder::new(&idl);
    assert!(decoder.decode(&[1, 2, 3]).is_none());
}

// AccountDecoder tests

#[test]
fn account_decoder_decodes_vault() {
    let idl = minimal_idl();
    let decoder = AccountDecoder::new(&idl);

    let disc = account_discriminator("Vault");
    // owner: 32-byte pubkey (all zeros), balance: u64(500), active: bool(true)
    let mut data = disc.to_vec();
    data.extend_from_slice(&[0u8; 32]); // owner
    data.extend_from_slice(&500u64.to_le_bytes()); // balance
    data.push(1u8); // active

    let result = decoder.decode(&data);
    assert!(result.is_some());
    let (name, fields) = result.unwrap();
    assert_eq!(name, "Vault");
    assert_eq!(fields["balance"], json!("500"));
    assert_eq!(fields["active"], json!(true));
    assert!(fields["owner"].is_string());
}

#[test]
fn account_decoder_no_match_for_wrong_discriminator() {
    let idl = minimal_idl();
    let decoder = AccountDecoder::new(&idl);
    let data = vec![0u8; 40]; // zeros — won't match Vault discriminator
    assert!(decoder.decode(&data).is_none());
}

#[test]
fn account_decoder_has_accounts_true() {
    let idl = minimal_idl();
    let decoder = AccountDecoder::new(&idl);
    assert!(decoder.has_accounts());
}

#[test]
fn account_decoder_has_accounts_false_for_empty_idl() {
    let decoder = AccountDecoder::new(&Idl::default());
    assert!(!decoder.has_accounts());
}
