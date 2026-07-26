CREATE TABLE IF NOT EXISTS late_data_log (
    log_id              BIGINT      NOT NULL AUTO_INCREMENT,
    transaction_id      VARCHAR(64) NOT NULL,
    account_id          VARCHAR(32) NOT NULL,
    amount              DECIMAL(16,2) NOT NULL,
    counterparty        VARCHAR(4)  NOT NULL,
    event_time          DATETIME    NOT NULL,
    arrival_time        DATETIME    NOT NULL,
    late_duration_sec   INT,
    watermark_at_arrival DATETIME,
    window_start        DATETIME    NOT NULL,
    window_end          DATETIME    NOT NULL,
    window_type         VARCHAR(16) NOT NULL,
    was_processed       BOOLEAN     DEFAULT FALSE,
    PRIMARY KEY (log_id),
    INDEX idx_late_account (account_id),
    INDEX idx_late_counterparty (counterparty),
    INDEX idx_late_duration (late_duration_sec)
);
