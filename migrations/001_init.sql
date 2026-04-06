-- Core transaction storage (fixed schema — program-agnostic)
CREATE TABLE IF NOT EXISTS transactions (
    signature   TEXT PRIMARY KEY,
    slot        BIGINT NOT NULL,
    block_time  BIGINT,
    err         JSONB,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_transactions_slot       ON transactions(slot DESC);
CREATE INDEX IF NOT EXISTS idx_transactions_block_time ON transactions(block_time DESC);

-- Indexer state / checkpoints
CREATE TABLE IF NOT EXISTS indexer_state (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- NOTE: typed instruction tables (ix_*) and account state tables (acc_*)
-- are generated dynamically at startup from the IDL via IdlSchema::generate_ddl().
