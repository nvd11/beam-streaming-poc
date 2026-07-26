CREATE TABLE IF NOT EXISTS window_sliding (
    window_start      DATETIME    NOT NULL,
    window_end        DATETIME    NOT NULL,
    account_id        VARCHAR(32) NOT NULL,
    fire_index        INT         NOT NULL,
    total_amount      DECIMAL(16,2) NOT NULL,
    transaction_count INT         NOT NULL,
    avg_amount        DECIMAL(16,2),
    max_amount        DECIMAL(16,2),
    min_amount        DECIMAL(16,2),
    late_data_count   INT         DEFAULT 0,
    late_data_amount  DECIMAL(16,2) DEFAULT 0.00,
    trigger_type      VARCHAR(16),
    slide_period      INT         DEFAULT 10,
    created_at        DATETIME    DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (window_start, window_end, account_id, fire_index),
    INDEX idx_sliding_account (account_id),
    INDEX idx_sliding_time (window_start, window_end)
);
