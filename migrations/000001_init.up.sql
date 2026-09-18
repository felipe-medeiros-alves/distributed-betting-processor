CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE wallets (
    id UUID PRIMARY KEY,
    player_id UUID NOT NULL,
    currency CHAR(3) NOT NULL,
    balance_minor BIGINT NOT NULL CHECK (balance_minor >= 0),
    version BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (player_id, currency)
);

CREATE TYPE transaction_origin AS ENUM ('INTERNAL', 'EXTERNAL');
CREATE TYPE transaction_kind AS ENUM ('OPENING', 'BET', 'WIN', 'LOSS', 'REFUND', 'ROLLBACK');
CREATE TYPE transaction_status AS ENUM ('PENDING', 'PENDING_REFERENCE', 'PROCESSED', 'REJECTED', 'FAILED');
CREATE TYPE ledger_direction AS ENUM ('DEBIT', 'CREDIT');

CREATE TABLE wager_transactions (
    id UUID PRIMARY KEY,
    origin transaction_origin NOT NULL,
    kind transaction_kind NOT NULL,
    status transaction_status NOT NULL,
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    player_id UUID NOT NULL,
    provider_id TEXT,
    external_transaction_id TEXT,
    idempotency_key TEXT,
    payload_hash TEXT,
    round_id TEXT,
    game_id TEXT,
    reference_external_transaction_id TEXT,
    reference_transaction_id UUID REFERENCES wager_transactions(id),
    amount_minor BIGINT NOT NULL,
    currency CHAR(3) NOT NULL,
    failure_code TEXT,
    observed_balance_minor BIGINT,
    observed_balance_currency CHAR(3),
    reference_attempts INT NOT NULL DEFAULT 0,
    next_reference_attempt_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    processed_at TIMESTAMPTZ,
    CONSTRAINT wager_external_ids CHECK (
        (origin = 'EXTERNAL' AND provider_id IS NOT NULL AND external_transaction_id IS NOT NULL AND idempotency_key IS NOT NULL)
        OR (origin = 'INTERNAL' AND kind = 'OPENING' AND provider_id IS NULL AND external_transaction_id IS NULL)
    )
);

CREATE UNIQUE INDEX idx_wager_provider_external ON wager_transactions (provider_id, external_transaction_id)
    WHERE origin = 'EXTERNAL';
CREATE UNIQUE INDEX idx_wager_idempotency ON wager_transactions (idempotency_key)
    WHERE idempotency_key IS NOT NULL;
CREATE UNIQUE INDEX idx_wager_opening_per_wallet ON wager_transactions (wallet_id)
    WHERE origin = 'INTERNAL' AND kind = 'OPENING';

CREATE TABLE wallet_ledger_entries (
    id UUID PRIMARY KEY,
    wallet_id UUID NOT NULL REFERENCES wallets(id),
    transaction_id UUID NOT NULL REFERENCES wager_transactions(id),
    direction ledger_direction NOT NULL,
    amount_minor BIGINT NOT NULL CHECK (amount_minor > 0),
    currency CHAR(3) NOT NULL,
    balance_before_minor BIGINT NOT NULL CHECK (balance_before_minor >= 0),
    balance_after_minor BIGINT NOT NULL CHECK (balance_after_minor >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (wallet_id, transaction_id),
    CONSTRAINT ledger_balance_consistency CHECK (
        (direction = 'DEBIT' AND balance_after_minor = balance_before_minor - amount_minor)
        OR (direction = 'CREDIT' AND balance_after_minor = balance_before_minor + amount_minor)
    )
);

CREATE OR REPLACE FUNCTION prevent_ledger_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'wallet_ledger_entries is append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER ledger_no_update
    BEFORE UPDATE OR DELETE ON wallet_ledger_entries
    FOR EACH ROW EXECUTE FUNCTION prevent_ledger_mutation();

CREATE TABLE inbox_messages (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    consumer_name TEXT NOT NULL,
    message_id TEXT NOT NULL,
    payload_hash TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    UNIQUE (consumer_name, message_id)
);

CREATE TABLE outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    event_id UUID NOT NULL UNIQUE,
    aggregate_id UUID NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    correlation_id TEXT,
    causation_id TEXT,
    occurred_at TIMESTAMPTZ NOT NULL,
    version INT NOT NULL DEFAULT 1,
    attempts INT NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    locked_until TIMESTAMPTZ,
    locked_by TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_outbox_pending ON outbox_events (next_attempt_at)
    WHERE published_at IS NULL;

CREATE INDEX idx_wager_pending_reference ON wager_transactions (next_reference_attempt_at)
    WHERE status = 'PENDING_REFERENCE';
