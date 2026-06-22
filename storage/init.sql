-- storage/init.sql
-- Runs automatically on first PostgreSQL container start.
-- Creates the transaction log and fraud audit tables.

-- ── Transaction event log ─────────────────────────────────────────────────────
-- Every transaction processed by the pipeline is written here.
-- This is the operational record — fast lookups by ID, sender, receiver.

CREATE TABLE IF NOT EXISTS transactions (
    id                      SERIAL PRIMARY KEY,
    transaction_id          TEXT NOT NULL UNIQUE,
    timestamp               TIMESTAMPTZ,
    type                    TEXT NOT NULL,
    amount                  NUMERIC(18, 2) NOT NULL,
    sender_id               TEXT NOT NULL,
    sender_balance_before   NUMERIC(18, 2),
    sender_balance_after    NUMERIC(18, 2),
    receiver_id             TEXT NOT NULL,
    receiver_balance_before NUMERIC(18, 2),
    receiver_balance_after  NUMERIC(18, 2),
    source                  TEXT,           -- 'paysim' or 'synthetic'
    kafka_partition         INTEGER,
    kafka_offset            BIGINT,
    created_at              TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_transactions_sender   ON transactions(sender_id);
CREATE INDEX IF NOT EXISTS idx_transactions_receiver ON transactions(receiver_id);
CREATE INDEX IF NOT EXISTS idx_transactions_timestamp ON transactions(timestamp);

-- ── Fraud alert log ───────────────────────────────────────────────────────────
-- Written when the ML pipeline flags a transaction as fraud.
-- Includes model votes and the reason string for auditability.

CREATE TABLE IF NOT EXISTS fraud_alerts (
    id                  SERIAL PRIMARY KEY,
    transaction_id      TEXT NOT NULL REFERENCES transactions(transaction_id),
    timestamp           TIMESTAMPTZ,
    sender_id           TEXT NOT NULL,
    receiver_id         TEXT NOT NULL,
    amount              NUMERIC(18, 2) NOT NULL,
    confidence          NUMERIC(5, 4),
    reason              TEXT,
    rf_score            NUMERIC(5, 4),      -- Random Forest fraud probability
    xgb_score           NUMERIC(5, 4),      -- XGBoost fraud probability
    iso_flag            BOOLEAN,            -- Isolation Forest anomaly flag
    is_fraud_label      BOOLEAN,            -- ground truth if available
    fraud_pattern       TEXT,               -- 'cycle', 'ring', 'burst', or NULL
    graph_pattern       TEXT,               -- filled in by Neo4j query results
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alerts_sender    ON fraud_alerts(sender_id);
CREATE INDEX IF NOT EXISTS idx_alerts_timestamp ON fraud_alerts(timestamp);
CREATE INDEX IF NOT EXISTS idx_alerts_confidence ON fraud_alerts(confidence DESC);
