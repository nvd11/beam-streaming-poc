CREATE TABLE IF NOT EXISTS transaction (
    transaction_id    VARCHAR(64)  NOT NULL PRIMARY KEY,
    account_id        VARCHAR(32)  NOT NULL,
    amount            DECIMAL(16,2) NOT NULL,
    trade_type        VARCHAR(8)   NOT NULL,
    counterparty      VARCHAR(4)   NOT NULL,
    event_timestamp   BIGINT       NOT NULL,
    event_time        DATETIME     NOT NULL,
    processing_delay  INT          DEFAULT 0,
    ingestion_time    DATETIME     DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_txn_account (account_id),
    INDEX idx_txn_event_time (event_time),
    INDEX idx_txn_counterparty (counterparty)
);
