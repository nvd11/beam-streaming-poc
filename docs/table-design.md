# 跨境贸易实时风控 POC 表设计

## 1. 概述

本文档定义 Beam Streaming POC 中的数据模型与表结构。整个实验涉及三类数据：

- **源数据**：模拟流入的原始交易记录
- **中间聚合**：窗口计算后的聚合结果
- **报警输出**：超额触发的风控报警记录

---

## 2. 交易流水表（transaction）

原始交易记录，模拟从 Kafka / Pub/Sub 流入的实时数据。

| 字段名 | 类型 | 长度 | 主键 | 必填 | 说明 |
|--------|------|------|------|------|------|
| transaction_id | VARCHAR | 64 | PK | Y | 交易唯一 ID，格式：`TXN-{UUID8}` |
| account_id | VARCHAR | 32 | | Y | 企业账号，如 `CORP-ALPHA` |
| amount | DECIMAL | 16,2 | | Y | 交易金额（美元），如 `40000.00` |
| trade_type | VARCHAR | 8 | | Y | 交易类型：`import` / `export` |
| counterparty | VARCHAR | 4 | | Y | 对手地区代码：`HK` / `SG` / `UK` / `US` |
| event_timestamp | BIGINT | | | Y | 事件时间戳（Unix 毫秒） |
| event_time | TIMESTAMP | | | Y | 事件时间（可读格式），由 event_timestamp 转换 |
| processing_delay | INT | | | N | 模拟延迟秒数，用于生成迟到数据，默认 `0` |
| ingestion_time | TIMESTAMP | | | N | 数据流入 Beam 的处理时间（Pipeline 自动填充） |

**索引设计：**

```sql
CREATE INDEX idx_txn_account ON transaction(account_id);
CREATE INDEX idx_txn_event_time ON transaction(event_time);
CREATE INDEX idx_txn_counterparty ON transaction(counterparty);
```

**示例数据：**

| transaction_id | account_id | amount | trade_type | counterparty | event_timestamp | event_time | processing_delay |
|---|---|---|---|---|---|---|---|
| TXN-a1b2c3d4 | CORP-ALPHA | 40000.00 | export | HK | 1711363200000 | 2026-07-26 12:00:00 | 0 |
| TXN-e5f6g7h8 | CORP-ALPHA | 35000.00 | import | SG | 1711363205000 | 2026-07-26 12:00:05 | 3 |
| TXN-i9j0k1l2 | CORP-BETA | 120000.00 | export | UK | 1711363210000 | 2026-07-26 12:00:10 | 10 |
| TXN-m3n4o5p6 | CORP-GAMMA | 250000.00 | import | US | 1711363195000 | 2026-07-26 11:59:55 | 25 |

---

## 3. Fixed Window 聚合结果表（window_fixed）

时段结算。每 30 秒一个窗口，互不重叠，每个账号一条汇总。

| 字段名 | 类型 | 长度 | 主键 | 必填 | 说明 |
|--------|------|------|------|------|------|
| window_start | TIMESTAMP | | PK (1) | Y | 窗口开始时间 |
| window_end | TIMESTAMP | | PK (2) | Y | 窗口结束时间 |
| account_id | VARCHAR | 32 | PK (3) | Y | 企业账号 |
| fire_index | INT | | PK (4) | Y | 同一窗口的第几次触发，从 `1` 开始 |
| total_amount | DECIMAL | 16,2 | | Y | 窗口内累计交易额 |
| transaction_count | INT | | | Y | 窗口内交易笔数 |
| avg_amount | DECIMAL | 16,2 | | N | 平均每笔交易额 |
| max_amount | DECIMAL | 16,2 | | N | 窗口内最大单笔交易额 |
| min_amount | DECIMAL | 16,2 | | N | 窗口内最小单笔交易额 |
| late_data_count | INT | | | N | 窗口内迟到数据笔数，默认 `0` |
| late_data_amount | DECIMAL | 16,2 | | N | 窗口内迟到数据总金额，默认 `0.00` |
| trigger_type | VARCHAR | 16 | | N | 触发方式：`WATERMARK` / `EARLY` / `LATE` / `ON_COUNT` |
| created_at | TIMESTAMP | | | N | 记录生成时间 |

**索引设计：**

```sql
CREATE INDEX idx_fixed_account ON window_fixed(account_id);
CREATE INDEX idx_fixed_time ON window_fixed(window_start, window_end);
```

**示例数据：**

| window_start | window_end | account_id | fire_index | total_amount | transaction_count | late_data_count | trigger_type |
|-------------|-----------|-----------|-----------|-------------|----------------|----------------|-------------|
| 2026-07-26 12:00:00 | 2026-07-26 12:00:30 | CORP-ALPHA | 1 | 245000.00 | 8 | 2 | WATERMARK |
| 2026-07-26 12:00:00 | 2026-07-26 12:00:30 | CORP-BETA | 1 | 52000.00 | 3 | 0 | WATERMARK |
| 2026-07-26 12:00:30 | 2026-07-26 12:01:00 | CORP-ALPHA | 1 | 32000.00 | 2 | 0 | WATERMARK |

---

## 4. Sliding Window 聚合结果表（window_sliding）

滚动敞口监控。窗口固定 30s，每 10s 滑动一次，同一笔交易可落入多个重叠窗口。

相比 Fixed Window 多一个参数 `slide_period`，记录该窗口的滑动间隔。

| 字段名 | 类型 | 长度 | 主键 | 必填 | 说明 |
|--------|------|------|------|------|------|
| window_start | TIMESTAMP | | PK (1) | Y | 窗口开始时间 |
| window_end | TIMESTAMP | | PK (2) | Y | 窗口结束时间 |
| account_id | VARCHAR | 32 | PK (3) | Y | 企业账号 |
| fire_index | INT | | PK (4) | Y | 同一窗口的第几次触发，从 `1` 开始 |
| total_amount | DECIMAL | 16,2 | | Y | 窗口内累计交易额 |
| transaction_count | INT | | | Y | 窗口内交易笔数 |
| avg_amount | DECIMAL | 16,2 | | N | 平均每笔交易额 |
| max_amount | DECIMAL | 16,2 | | N | 窗口内最大单笔交易额 |
| min_amount | DECIMAL | 16,2 | | N | 窗口内最小单笔交易额 |
| late_data_count | INT | | | N | 窗口内迟到数据笔数，默认 `0` |
| late_data_amount | DECIMAL | 16,2 | | N | 窗口内迟到数据总金额，默认 `0.00` |
| trigger_type | VARCHAR | 16 | | N | 触发方式：`WATERMARK` / `EARLY` / `LATE` / `ON_COUNT` |
| slide_period | INT | | | N | 滑动间隔（秒），默认 `10` |
| created_at | TIMESTAMP | | | N | 记录生成时间 |

**索引设计：**

```sql
CREATE INDEX idx_sliding_account ON window_sliding(account_id);
CREATE INDEX idx_sliding_time ON window_sliding(window_start, window_end);
```

**示例数据（同一账号跨 3 个重叠窗口）：**

| window_start | window_end | account_id | fire_index | total_amount | slide_period |
|-------------|-----------|-----------|-----------|-------------|-------------|
| 2026-07-26 12:00:00 | 2026-07-26 12:00:30 | CORP-ALPHA | 1 | 140000.00 | 10 |
| 2026-07-26 12:00:10 | 2026-07-26 12:00:40 | CORP-ALPHA | 1 | 140000.00 | 10 |
| 2026-07-26 12:00:20 | 2026-07-26 12:00:50 | CORP-ALPHA | 1 | 60000.00 | 10 |

---

## 5. Session Window 聚合结果表（window_session）

可疑密集交易检测。无固定窗口大小，由 `gap_duration` 和数据的实际行为共同决定窗口边界。

相比前两者多两个特有字段：`gap_duration`（配置的会话间隙）和 `actual_duration`（窗口实际时长）。

| 字段名 | 类型 | 长度 | 主键 | 必填 | 说明 |
|--------|------|------|------|------|------|
| window_start | TIMESTAMP | | PK (1) | Y | 窗口开始时间（Session 第一笔交易的 event_time） |
| window_end | TIMESTAMP | | PK (2) | Y | 窗口结束时间（Session 最后一笔交易 + gap_duration） |
| account_id | VARCHAR | 32 | PK (3) | Y | 企业账号 |
| fire_index | INT | | PK (4) | Y | 同一窗口的第几次触发，从 `1` 开始 |
| total_amount | DECIMAL | 16,2 | | Y | 窗口内累计交易额 |
| transaction_count | INT | | | Y | 窗口内交易笔数 |
| avg_amount | DECIMAL | 16,2 | | N | 平均每笔交易额 |
| max_amount | DECIMAL | 16,2 | | N | 窗口内最大单笔交易额 |
| min_amount | DECIMAL | 16,2 | | N | 窗口内最小单笔交易额 |
| late_data_count | INT | | | N | 窗口内迟到数据笔数，默认 `0` |
| late_data_amount | DECIMAL | 16,2 | | N | 窗口内迟到数据总金额，默认 `0.00` |
| trigger_type | VARCHAR | 16 | | N | 触发方式：`WATERMARK` / `EARLY` / `LATE` / `ON_COUNT` |
| gap_duration | INT | | | N | 配置的会话间隙（秒），默认 `15` |
| actual_duration | INT | | | N | 窗口实际时长（秒），即 `window_end - window_start` |
| created_at | TIMESTAMP | | | N | 记录生成时间 |

**索引设计：**

```sql
CREATE INDEX idx_session_account ON window_session(account_id);
CREATE INDEX idx_session_time ON window_session(window_start, window_end);
```

**示例数据（同一账号的两个独立 Session）：**

| window_start | window_end | account_id | fire_index | total_amount | transaction_count | gap_duration | actual_duration |
|-------------|-----------|-----------|-----------|-------------|----------------|-------------|---------------|
| 2026-07-26 10:00:01 | 2026-07-26 10:00:27 | CORP-ALPHA | 1 | 155000.00 | 4 | 15 | 26 |
| 2026-07-26 10:05:30 | 2026-07-26 10:05:45 | CORP-ALPHA | 1 | 10000.00 | 1 | 15 | 15 |

---

## 6. 风控报警表（alert）

当窗口聚合结果超过阈值时，生成报警记录。

| 字段名 | 类型 | 长度 | 主键 | 必填 | 说明 |
|--------|------|------|------|------|------|
| alert_id | BIGINT | | PK (自增) | Y | 报警唯一 ID |
| window_type | VARCHAR | 16 | | Y | 窗口类型 |
| account_id | VARCHAR | 32 | | Y | 触发报警的企业账号 |
| total_amount | DECIMAL | 16,2 | | Y | 触发报警的累计交易额 |
| alert_level | VARCHAR | 8 | | Y | 报警级别：`YELLOW` / `RED` |
| threshold | DECIMAL | 16,2 | | Y | 触发阈值，`100000.00` 或 `500000.00` |
| exceeded_amount | DECIMAL | 16,2 | | N | 超出阈值的金额（total_amount - threshold） |
| transaction_count | INT | | | N | 窗口内交易笔数 |
| window_start | TIMESTAMP | | | Y | 窗口开始时间 |
| window_end | TIMESTAMP | | | Y | 窗口结束时间 |
| has_late_data | BOOLEAN | | | N | 是否包含迟到数据，默认 `FALSE` |
| alert_time | TIMESTAMP | | | N | 报警生成时间 |
| status | VARCHAR | 16 | | N | 报警状态：`NEW` / `ACKED` / `RESOLVED`，默认 `NEW` |

**索引设计：**

```sql
CREATE INDEX idx_alert_account ON alert(account_id);
CREATE INDEX idx_alert_level ON alert(alert_level);
CREATE INDEX idx_alert_time ON alert(alert_time);
CREATE INDEX idx_alert_status ON alert(status);
```

**外键约束（逻辑关联，BigQuery 不强制物理 FK）：**

```sql
-- alert 与 window_aggregation 通过 (window_type, window_start, window_end, account_id, fire_index) 逻辑关联
-- 实际查询时通过 JOIN 关联两张表
```

**示例数据：**

| alert_id | account_id | total_amount | alert_level | threshold | exceeded_amount | transaction_count |
|----------|-----------|-------------|------------|----------|----------------|-----------------|
| 1 | CORP-ALPHA | 245000.00 | YELLOW | 100000.00 | 145000.00 | 8 |
| 2 | CORP-BETA | 523000.00 | RED | 500000.00 | 23000.00 | 12 |
| 3 | CORP-ALPHA | 155000.00 | YELLOW | 100000.00 | 55000.00 | 4 |

---

## 7. 迟到数据日志表（late_data_log）

记录被 Watermark 判定为迟到但仍在 Allowed Lateness 窗口内的数据。

| 字段名 | 类型 | 长度 | 主键 | 必填 | 说明 |
|--------|------|------|------|------|------|
| log_id | BIGINT | | PK (自增) | Y | 日志 ID |
| transaction_id | VARCHAR | 64 | | Y | 对应的原始交易 ID |
| account_id | VARCHAR | 32 | | Y | 企业账号 |
| amount | DECIMAL | 16,2 | | Y | 交易金额 |
| counterparty | VARCHAR | 4 | | Y | 来源地区 |
| event_time | TIMESTAMP | | | Y | 交易事件时间 |
| arrival_time | TIMESTAMP | | | Y | 数据实际到达 Beam 的时间 |
| late_duration_sec | INT | | | N | 迟到秒数（arrival_time - watermark_time） |
| watermark_at_arrival | TIMESTAMP | | | N | 数据到达时的 Watermark 水位 |
| window_start | TIMESTAMP | | | Y | 该数据所属的目标窗口开始时间 |
| window_end | TIMESTAMP | | | Y | 该数据所属的目标窗口结束时间 |
| window_type | VARCHAR | 16 | | Y | 窗口类型 |
| was_processed | BOOLEAN | | | N | 是否被纳入窗口重算，默认 `FALSE`。先记入日志，确认纳入重算后更新为 `TRUE` |

**索引设计：**

```sql
CREATE INDEX idx_late_account ON late_data_log(account_id);
CREATE INDEX idx_late_counterparty ON late_data_log(counterparty);
CREATE INDEX idx_late_duration ON late_data_log(late_duration_sec);
```

**示例数据：**

| transaction_id | account_id | counterparty | event_time | late_duration_sec | watermark_at_arrival | was_processed |
|---------------|-----------|-------------|-----------|------------------|---------------------|-------------|
| TXN-m3n4o5p6 | CORP-GAMMA | US | 2026-07-26 11:59:55 | 22 | 2026-07-26 12:00:17 | TRUE |
| TXN-q7r8s9t0 | CORP-ALPHA | UK | 2026-07-26 11:59:58 | 18 | 2026-07-26 12:00:16 | TRUE |
| TXN-z9a0b1c2 | CORP-BETA | US | 2026-07-26 11:59:30 | 45 | 2026-07-26 12:00:15 | FALSE |

---

## 8. 表关系图

```
┌──────────────────┐       ┌──────────────────────────┐
│   transaction    │       │      window_fixed         │
├──────────────────┤       ├──────────────────────────┤
│ PK transaction_id│       │ PK (window_start,         │
│ account_id       │       │      window_end,          │
│ amount           │       │      account_id,          │
│ counterparty     │       │      fire_index)          │
│ event_timestamp  │       │    total_amount           │
│ processing_delay │       │    transaction_count      │
└──────────────────┘       └──────────────────────────┘
        │
        │                  ┌──────────────────────────┐
        │                  │      window_sliding        │
        │                  ├──────────────────────────┤
        │                  │ PK (window_start,          │
        │                  │      window_end,           │
        │                  │      account_id,           │
        │                  │      fire_index)           │
        │                  │    total_amount            │
        │                  │    slide_period            │
        │                  └──────────────────────────┘
        │
        │                  ┌──────────────────────────┐
        │                  │      window_session        │
        │                  ├──────────────────────────┤
        │                  │ PK (window_start,          │
        │                  │      window_end,           │
        │                  │      account_id,           │
        │                  │      fire_index)           │
        │                  │    total_amount            │
        │                  │    gap_duration            │
        │                  │    actual_duration         │
        │                  └──────────────────────────┘
        │                              │
        │                              │ 逻辑关联 (window_type 区分)
        │                              ▼
        │                  ┌──────────────────────┐
        │                  │        alert          │
        │                  ├──────────────────────┤
        │                  │ PK alert_id           │
        │                  │    window_type        │ ← 区分来自哪张窗口表
        │                  │    account_id         │
        │                  │    alert_level        │
        │                  │    total_amount       │
        │                  │    threshold          │
        │                  └──────────────────────┘
        │
        │ FK
        ▼
┌──────────────────┐
│  late_data_log   │
├──────────────────┤
│ PK log_id        │
│ FK transaction_id│
│    account_id    │
│    counterparty  │
│    late_duration │
│    was_processed │
└──────────────────┘
```

---

## 9. 字段类型映射说明（Beam -> BigQuery -> Java/Python）

| Beam/Python 类型 | BigQuery 类型 | Java 类型 | 说明 |
|-----------------|--------------|----------|------|
| `str` | `STRING` | `String` | 字符串，映射为 VARCHAR |
| `float` | `FLOAT64` | `Double` | 浮点数 |
| `int` | `INT64` | `Long` | 整数 |
| `bool` | `BOOL` | `Boolean` | 布尔值 |
| `datetime` / `Timestamp` | `TIMESTAMP` | `Instant` | 时间戳 |
| `Decimal` | `NUMERIC` | `BigDecimal` | 高精度 decimal，金额专用 |

---

## 10. 设计决策说明

1. **金额使用 DECIMAL(16,2) 而非 FLOAT**：避免浮点精度损失，金融场景必备。
2. **三种窗口分三张表**：每种窗口的业务含义和附加字段不同（Sliding 有 `slide_period`，Session 有 `gap_duration` / `actual_duration`），分开更清晰，方便各自独立演化。
3. **所有窗口表使用相同的 PK 结构**：`(window_start, window_end, account_id, fire_index)`，保证跨表查询格式一致，支持 UNION 对比。
4. **late_data_log 独立建表**：便于分析 Watermark 行为和迟到数据的来源分布（比如看 US 地区的迟到率）。
5. **alert 表通过 `window_type` 区分来源**：`window_type` 字段标识报警来自哪张窗口表，查询时 JOIN 对应的表即可。
6. **fire_index 字段**：记录同一窗口的第几次触发（Watermark 触发 = 第 1 次，Early 触发 = 第 1 次，Late 触发 > 1），用于区分「首次结果」和「迟到更新」。

---

## 11. 建表 DDL（BigQuery 标准 SQL）

<details>
<summary>点击展开 DDL</summary>

```sql
-- 交易流水表
CREATE TABLE IF NOT EXISTS transaction (
    transaction_id    STRING    NOT NULL,
    account_id        STRING    NOT NULL,
    amount            NUMERIC   NOT NULL,
    trade_type        STRING    NOT NULL,
    counterparty      STRING    NOT NULL,
    event_timestamp   INT64     NOT NULL,
    event_time        TIMESTAMP NOT NULL,
    processing_delay  INT64     DEFAULT 0,
    ingestion_time    TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PRIMARY KEY (transaction_id);

-- Fixed Window 聚合结果表
CREATE TABLE IF NOT EXISTS window_fixed (
    window_start      TIMESTAMP NOT NULL,
    window_end        TIMESTAMP NOT NULL,
    account_id        STRING    NOT NULL,
    fire_index        INT64     NOT NULL,
    total_amount      NUMERIC   NOT NULL,
    transaction_count INT64     NOT NULL,
    avg_amount        NUMERIC,
    max_amount        NUMERIC,
    min_amount        NUMERIC,
    late_data_count   INT64     DEFAULT 0,
    late_data_amount  NUMERIC   DEFAULT 0.00,
    trigger_type      STRING,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PRIMARY KEY (window_start, window_end, account_id, fire_index);

-- Sliding Window 聚合结果表
CREATE TABLE IF NOT EXISTS window_sliding (
    window_start      TIMESTAMP NOT NULL,
    window_end        TIMESTAMP NOT NULL,
    account_id        STRING    NOT NULL,
    fire_index        INT64     NOT NULL,
    total_amount      NUMERIC   NOT NULL,
    transaction_count INT64     NOT NULL,
    avg_amount        NUMERIC,
    max_amount        NUMERIC,
    min_amount        NUMERIC,
    late_data_count   INT64     DEFAULT 0,
    late_data_amount  NUMERIC   DEFAULT 0.00,
    trigger_type      STRING,
    slide_period      INT64     DEFAULT 10,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PRIMARY KEY (window_start, window_end, account_id, fire_index);

-- Session Window 聚合结果表
CREATE TABLE IF NOT EXISTS window_session (
    window_start      TIMESTAMP NOT NULL,
    window_end        TIMESTAMP NOT NULL,
    account_id        STRING    NOT NULL,
    fire_index        INT64     NOT NULL,
    total_amount      NUMERIC   NOT NULL,
    transaction_count INT64     NOT NULL,
    avg_amount        NUMERIC,
    max_amount        NUMERIC,
    min_amount        NUMERIC,
    late_data_count   INT64     DEFAULT 0,
    late_data_amount  NUMERIC   DEFAULT 0.00,
    trigger_type      STRING,
    gap_duration      INT64     DEFAULT 15,
    actual_duration   INT64,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PRIMARY KEY (window_start, window_end, account_id, fire_index);

-- 风控报警表
CREATE TABLE IF NOT EXISTS alert (
    alert_id          INT64     NOT NULL,
    window_type       STRING    NOT NULL,
    account_id        STRING    NOT NULL,
    total_amount      NUMERIC   NOT NULL,
    alert_level       STRING    NOT NULL,
    threshold         NUMERIC   NOT NULL,
    exceeded_amount   NUMERIC,
    transaction_count INT64,
    window_start      TIMESTAMP NOT NULL,
    window_end        TIMESTAMP NOT NULL,
    has_late_data     BOOL      DEFAULT FALSE,
    alert_time        TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    status            STRING    DEFAULT 'NEW'
)
PRIMARY KEY (alert_id);

-- 迟到数据日志表
CREATE TABLE IF NOT EXISTS late_data_log (
    log_id              INT64     NOT NULL,
    transaction_id      STRING    NOT NULL,
    account_id          STRING    NOT NULL,
    amount              NUMERIC   NOT NULL,
    counterparty        STRING    NOT NULL,
    event_time          TIMESTAMP NOT NULL,
    arrival_time        TIMESTAMP NOT NULL,
    late_duration_sec   INT64,
    watermark_at_arrival TIMESTAMP,
    window_start        TIMESTAMP NOT NULL,
    window_end          TIMESTAMP NOT NULL,
    window_type         STRING    NOT NULL,
    was_processed       BOOL      DEFAULT FALSE
)
PRIMARY KEY (log_id);
```

</details>
