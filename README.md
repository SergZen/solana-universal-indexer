# Solana Universal Indexer

A production-ready, **universal** Solana indexer that automatically adapts to any Anchor IDL — no manual schema definitions required.

Built in **Rust** with Tokio, Axum, sqlx, and the Solana RPC/WebSocket client stack.

---

## Table of Contents

1. [Features](#features)
2. [Architecture Overview](#architecture-overview)
3. [Quick Start](#quick-start)
4. [Configuration](#configuration)
5. [Indexing Modes](#indexing-modes)
6. [API Reference](#api-reference)
7. [Key Architectural Decisions](#key-architectural-decisions)
8. [Trade-offs](#trade-offs)
9. [Running Tests](#running-tests)
10. [Project Structure](#project-structure)

---

## Features

| Capability | Details |
|---|---|
| **Dynamic schema generation** | At startup, reads the IDL and runs `CREATE TABLE IF NOT EXISTS ix_<name>` and `acc_<name>` — no manual SQL |
| **Schema evolution** | New fields in the IDL → `ALTER TABLE ADD COLUMN IF NOT EXISTS` runs automatically |
| **Account state decoding** | Fetches on-chain account data and Borsh-decodes into typed columns |
| **Batch mode (slot range)** | Scans slots via `getBlock` — no backwards pagination from chain tip |
| **Batch mode (resume)** | Remembers last indexed slot, resumes from there on restart |
| **Realtime mode** | WebSocket `logsSubscribe` with cold-start backfill and exponential backoff reconnect |
| **No duplicates** | `ON CONFLICT DO NOTHING` on all tables + UNIQUE index on `tx_sig` per ix table |
| **Exponential backoff** | All RPC calls: 500ms → 1s → 2s → … → 30s cap |
| **Graceful shutdown** | SIGTERM/Ctrl-C drains current batch before exit |
| **Advanced API** | Per-instruction filtering, time-series aggregation, program stats |
| **Docker Compose** | `docker compose up --build` — one command start |
| **Structured logging** | JSON or pretty via `LOG_FORMAT` env var |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Compose                           │
│  ┌─────────────┐   ┌──────────────────────────────────────┐    │
│  │  PostgreSQL │◄──│           solana-indexer             │    │
│  │             │   │                                      │    │
│  │ transactions│   │  ┌──────────┐  ┌─────────────────┐  │    │
│  │ ix_buy      │   │  │IDL Loader│  │  Axum REST API  │  │    │
│  │ ix_sell     │   │  └────┬─────┘  └─────────────────┘  │    │
│  │ acc_bonding │   │       │                              │    │
│  │  _curve     │   │  ┌────▼──────────────────────────┐  │    │
│  │ indexer_    │   │  │  IdlSchema::generate_ddl()    │  │    │
│  │  state      │   │  │  CREATE TABLE ix_* / acc_*    │  │    │
│  └─────────────┘   │  │  ALTER TABLE ADD COLUMN ...   │  │    │
│                    │  └────┬──────────────────────────┘  │    │
│                    │       │                              │    │
│                    │  ┌────▼──────────┐  ┌────────────┐  │    │
│                    │  │ Batch Mode    │  │ Realtime   │  │    │
│                    │  │ getBlock scan │  │ WS + cold  │  │    │
│                    │  │ slot resume   │  │ start +    │  │    │
│                    │  │               │  │ reconnect  │  │    │
│                    │  └───────────────┘  └────────────┘  │    │
│                    │       │                              │    │
│                    │  ┌────▼──────────────────────────┐  │    │
│                    │  │  RpcClientWithRetry            │  │    │
│                    │  │  (exponential backoff)         │  │    │
│                    │  └───────────────────────────────-┘  │    │
│                    └──────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                              │ RPC / WebSocket
                       ┌──────▼──────┐
                       │  Solana RPC │
                       └─────────────┘
```

### Data flow

1. **IDL loading** — IDL is read from file or fetched on-chain (auto-detects zlib/raw, Anchor legacy and v0.30+).
2. **Dynamic schema** — `IdlSchema::generate_ddl()` produces `CREATE TABLE IF NOT EXISTS ix_<n>` (one per instruction) and `acc_<n>` (one per account type), plus `ALTER TABLE ADD COLUMN IF NOT EXISTS` for schema evolution. All executed at startup.
3. **Indexing** — signatures collected via `getBlock` (batch) or WebSocket (realtime), decoded with Anchor discriminators + Borsh, written to typed tables.
4. **Checkpointing** — last indexed signature and slot are stored atomically in `indexer_state`. On restart, batch mode resumes from the saved slot; realtime mode backfills from the saved signature.
5. **Deduplication** — `transactions` has a PRIMARY KEY on `signature`; each `ix_*` table has a UNIQUE index on `tx_sig`. Re-indexing the same range is safe.

---

## Quick Start

### Prerequisites
- Docker & Docker Compose
- A Solana RPC endpoint (Helius, QuickNode, or public devnet)

### 1. Clone and configure

```bash
git clone https://github.com/SergZen/solana-universal-indexer.git
cd solana-universal-indexer
cp .env.example .env
```

Edit `.env`:
```env
RPC_URL=https://api.devnet.solana.com
PROGRAM_ID=6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
INDEXER_MODE=realtime
```

### 2. Generate sqlx offline cache (first time only)

```bash
docker compose up -d postgres
cargo install sqlx-cli --no-default-features --features postgres
DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer cargo sqlx migrate run
DATABASE_URL=postgres://indexer:indexer@localhost:5432/indexer cargo sqlx prepare
docker compose down
```

### 3. Run

```bash
docker compose up --build
```

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `RPC_URL` | **required** | Solana HTTP RPC URL |
| `WS_URL` | derived from `RPC_URL` | Solana WebSocket URL |
| `PROGRAM_ID` | **required** | Target program public key |
| `DATABASE_URL` | **required** | PostgreSQL connection string |
| `IDL_SOURCE` | `onchain` | `onchain` or `file` |
| `IDL_PATH` | — | Path to IDL JSON (when `IDL_SOURCE=file`) |
| `INDEXER_MODE` | `realtime` | `realtime` or `batch` |
| `START_SLOT` | — | Batch: start slot |
| `END_SLOT` | — | Batch: end slot |
| `SIGNATURES` | — | Batch: comma-separated signature list |
| `BATCH_SIZE` | `100` | Signatures per DB commit |
| `POLL_INTERVAL_SECS` | `5` | Realtime flush interval |
| `RPC_MAX_RETRIES` | `5` | Max RPC retry attempts |
| `API_PORT` | `3000` | REST API port |
| `RUST_LOG` | `info` | Log level |
| `LOG_FORMAT` | `pretty` | `json` or `pretty` |

---

## Indexing Modes

### Batch — slot range

```env
INDEXER_MODE=batch
START_SLOT=300000000
END_SLOT=300001000
```

Uses `getBlock` to scan each slot directly — no backwards pagination from the chain tip. On restart, automatically resumes from the last saved slot checkpoint so no slot is processed twice.

### Batch — explicit signatures

```env
INDEXER_MODE=batch
SIGNATURES=5abc...,6def...
```

### Realtime (default)

```env
INDEXER_MODE=realtime
WS_URL=wss://api.devnet.solana.com
```

On startup:
1. Reads last saved signature checkpoint
2. Backfills all missed transactions since that checkpoint
3. Switches to `logsSubscribe` WebSocket

On WebSocket disconnect: reconnects with exponential backoff (2s → 4s → 8s → … → 60s).

---

## API Reference

Base URL: `http://localhost:3000`

### Health
```
GET /health
→ { "status": "ok", "program_id": "...", "last_checkpoint": "5abc..." }
```

### Schema introspection
```
GET /schema
→ { "program_name": "pump_fun", "instructions": [...], "accounts": [...] }
```

### Transaction lookup
```
GET /tx/{signature}
```

### Instruction calls
```
GET /ix/{ix_name}?tx_sig=&from_slot=&to_slot=&from=&to=&limit=50&offset=0
```

Example:
```
GET /ix/buy?from_slot=300000000&to_slot=300001000&limit=100
```

### Instruction stats & time-series
```
GET /ix/{ix_name}/stats?from=2024-08-01T00:00:00Z&to=2024-08-15T23:59:59Z&bucket=hour
```

`bucket`: `hour` (default), `day`, `week`

Response:
```json
{
  "ix_name": "buy",
  "total_calls": 12345,
  "bucket": "hour",
  "series": [
    { "ix_name": "buy", "period": "2024-08-15T12:00:00Z", "calls": 234 }
  ]
}
```

### Program statistics
```
GET /stats
→ {
    "total_transactions": 15234,
    "total_ix_calls": 18901,
    "ix_call_counts": { "ix_buy": 12345, "ix_sell": 3210 },
    "first_indexed_slot": 300000000,
    "last_indexed_slot": 300001850,
    "last_checkpoint": "5abc..."
  }
```

### Account states
```
GET /accounts?type=BondingCurve
GET /accounts/{address}?type=BondingCurve
GET /accounts/{address}/history?type=BondingCurve&limit=20
```

Use `GET /schema` to see available account type names.

---

## Key Architectural Decisions

### 1. Typed tables per IDL type (not a single JSONB table)

Each instruction and account type gets its own table with typed columns derived from the IDL. This enables real SQL queries, proper indexes, and type-safe aggregation — unlike a single `JSONB` column approach.

Schema is generated at runtime so no SQL migrations are needed when the IDL changes. New fields are added via `ALTER TABLE ADD COLUMN IF NOT EXISTS`.

### 2. getBlock for historical slot ranges

`getSignaturesForAddress` paginates backwards from the chain tip — scanning old slots requires fetching thousands of pages to reach the target range. `getBlock` goes directly to the slot, which is far more efficient for historical data.

### 3. Idempotent inserts — safe re-indexing

- `transactions`: `PRIMARY KEY (signature)`
- `ix_*` tables: `UNIQUE INDEX ON (tx_sig)` + `ON CONFLICT DO NOTHING`
- `acc_*` tables: `PRIMARY KEY (address, slot)` + `ON CONFLICT DO UPDATE`

Re-running the indexer over the same range produces no duplicates.

### 4. Slot checkpoint for batch resume

The last indexed slot is saved atomically with each batch commit. On restart, batch mode picks up from `last_indexed_slot + 1` instead of scanning from `START_SLOT` again.

### 5. WebSocket reconnect with exponential backoff

Public RPC nodes drop WebSocket connections after ~60s of inactivity. The indexer reconnects automatically: 2s → 4s → 8s → … → 60s cap. Signatures received during the gap are recovered via cold-start backfill on reconnect.

### 6. Anchor 0.30+ account field resolution

In Anchor ≥0.30 the `accounts` section only contains name + discriminator. Fields live in `types`. The schema parser checks both locations automatically.

---

## Trade-offs

| Area | Choice | What we give up |
|---|---|---|
| Schema | Typed columns per IDL type | Must reconnect to DB to add columns when IDL changes (handled automatically) |
| Batch | `getBlock` per slot | Slower for sparse programs; better for dense ones |
| Realtime | WS + HTTP batch flush | ~seconds of latency vs per-event (acceptable for most use-cases) |
| Account decode | On instruction processing | No separate account subscription stream |
| IDL | Anchor only | Non-Anchor programs (custom discriminators) |

---

## Running Tests

```bash
cargo test
```

Covers: discriminator computation, all Borsh primitives, struct field decoding, `IxDecoder`/`AccountDecoder`, DDL generation, `to_snake_case`, IDL type → PG type mapping.

---

## Project Structure

```
solana-indexer/
├── Cargo.toml
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── migrations/
│   └── 001_init.sql          # transactions, indexer_state (fixed)
│                             # ix_* and acc_* created dynamically from IDL
├── src/
│   ├── main.rs               # startup, mode dispatch, graceful shutdown
│   ├── config.rs             # env-based config
│   ├── idl/
│   │   ├── loader.rs         # IDL from file or on-chain
│   │   ├── decoder.rs        # IxDecoder, AccountDecoder, Borsh reader
│   │   └── schema.rs         # IdlSchema, generate_ddl(), ALTER TABLE evolution
│   ├── db/
│   │   ├── mod.rs            # connect() — runs migrations + dynamic DDL
│   │   └── queries.rs        # all DB operations, bind_value() type coercion
│   ├── indexer/
│   │   ├── rpc.rs            # RpcClientWithRetry, getBlock slot scan
│   │   ├── processor.rs      # batch processing, account decoding
│   │   └── realtime.rs       # WebSocket cold-start + reconnect loop
│   └── api/
│       ├── handlers.rs       # route handlers
│       └── router.rs         # Axum router
└── tests/
    └── decoder_tests.rs      # integration-level tests
```
