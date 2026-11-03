# 6. Accounts, Funding & Data

## 6.1 Balance ownership

| Balance view | Owner | Purpose |
|---|---|---|
| Trading balance (`available`, `held`) | **Matching Engine** (authoritative) | Funds holds and settlement on the hot path |
| Ledger (double-entry journal) | **Funding Service** / PostgreSQL | Money in/out of the platform, regulatory record |
| Balance history / portfolio | **History Service** / PostgreSQL | Projection of engine `BalanceUpdate` events for UI |

A nightly + continuous **reconciliation job** checks: ledger customer liability per currency == Σ engine balances (from latest snapshot / projection). Breaks raise alerts and can freeze affected accounts.

## 6.2 Deposit flow (summary — full sequence in [07](07-interactions.md#73-deposit))

1. User initiates deposit → Funding creates `deposit(PENDING)` and PSP payment intent.
2. PSP confirms (webhook, signature verified, idempotent on PSP reference).
3. Funding posts ledger entries `DR bank_clearing / CR customer_liability:<user>` and status `CONFIRMED`.
4. Funding sends `Deposit` command (fundingTxId = idempotency key) to the Matching Engine.
5. Engine credits `available`, emits `BalanceUpdate` → Funding marks `CREDITED`; Trading Service pushes balance to the user.

Phase 1 ships a **Mock PSP** (simulated bank with instant/delayed/failed outcomes) behind a `PaymentProvider` interface.

## 6.3 Fees

Maker/taker fee schedule per instrument and user tier (default maker 0.05%, taker 0.10%, configurable). Fees charged in the currency received, accrued to a platform fee account inside the engine, reported in `ExecutionReport`.

## 6.4 PostgreSQL schema (main tables)

```mermaid
erDiagram
    USERS ||--o{ USER_SETTINGS : has
    USERS ||--o{ SESSIONS : has
    USERS ||--o{ DEPOSITS : makes
    USERS ||--o{ WITHDRAWALS : makes
    USERS ||--o{ ORDERS : places
    ORDERS ||--o{ FILLS : has
    INSTRUMENTS ||--o{ ORDERS : for
    CURRENCIES ||--o{ INSTRUMENTS : base_quote
    LEDGER_TX ||--|{ LEDGER_ENTRIES : contains
    LEDGER_ACCOUNTS ||--o{ LEDGER_ENTRIES : posts
    USERS ||--o{ BALANCE_SNAPSHOTS : has

    USERS {
        bigint user_id PK
        citext email UK
        text password_hash
        text status
        int kyc_tier
        bool mfa_enabled
        text totp_secret_enc
        timestamptz created_at
    }
    USER_SETTINGS {
        bigint user_id PK
        text theme
        text colour_scheme
        text up_down_colours
        int default_instrument_id
        text default_order_type
        text default_tif
        bool confirm_orders
        jsonb layout
        text reporting_ccy
    }
    SESSIONS {
        uuid session_id PK
        bigint user_id
        text refresh_hash
        text device
        inet ip
        timestamptz expires_at
        bool revoked
    }
    CURRENCIES {
        int currency_id PK
        text code UK
        int scale
        text type
    }
    INSTRUMENTS {
        int instrument_id PK
        text symbol UK
        int base_ccy
        int quote_ccy
        int price_scale
        int qty_scale
        bigint tick_size
        bigint lot_size
        bigint min_notional
        text status
    }
    DEPOSITS {
        uuid deposit_id PK
        bigint user_id
        int currency_id
        numeric amount
        text psp_ref UK
        text status
        timestamptz created_at
        timestamptz credited_at
    }
    WITHDRAWALS {
        uuid withdrawal_id PK
        bigint user_id
        int currency_id
        numeric amount
        text status
    }
    LEDGER_ACCOUNTS {
        bigint account_id PK
        text type
        bigint user_id
        int currency_id
    }
    LEDGER_TX {
        uuid tx_id PK
        text kind
        text ref
        timestamptz posted_at
    }
    LEDGER_ENTRIES {
        bigint entry_id PK
        uuid tx_id
        bigint account_id
        numeric debit
        numeric credit
    }
    ORDERS {
        bigint order_id PK
        text client_order_id
        bigint user_id
        int instrument_id
        text side
        text type
        text tif
        numeric price
        numeric qty
        numeric filled_qty
        numeric avg_price
        text status
        text reject_reason
        bigint engine_seq
        timestamptz created_at
        timestamptz updated_at
    }
    FILLS {
        bigint trade_id
        bigint order_id
        bigint user_id
        int instrument_id
        text side
        text liquidity
        numeric price
        numeric qty
        numeric fee
        int fee_ccy
        timestamptz ts
    }
    BALANCE_SNAPSHOTS {
        bigint user_id
        int currency_id
        numeric available
        numeric held
        bigint engine_seq
        timestamptz ts
    }
```

Additional: `candles(instrument_id, interval, open_time, o,h,l,c, volume)` (TimescaleDB hypertable optional), `audit_log`, `processed_positions(consumer, archive_position, engine_seq)` for exactly-once projection, `api_keys` (phase 6).

Indexes: `orders(user_id, status, created_at desc)`, `fills(user_id, ts desc)`, `fills(instrument_id, ts)`. Partition `orders`/`fills` monthly.

## 6.5 Persistence pipeline

```mermaid
flowchart LR
    ME["Matching Engine events"] --> ARC[("Aeron Archive<br/>recording")]
    ARC -- "replay from last position<br/>then live tail" --> TAIL["Archive tailer<br/>(Aeron poller)"]
    TAIL --> RB[["Disruptor"]]
    RB --> MAP["Mapper<br/>SBE → row batches"]
    MAP --> W["Batch writer<br/>JDBC COPY / multi-row upsert<br/>+ processed_positions in same tx"]
    RB --> CAN["Candle aggregator"]
    CAN --> W
    W --> PG[("PostgreSQL")]
```

Exactly-once effect: rows and the consumer's `engine_seq` are committed in one transaction; on restart replay resumes from the stored archive position and skips `engine_seq ≤ stored`.

## 6.6 History Service REST

`GET /api/v1/orders?status=&symbol=&from=&to=&cursor=` · `GET /api/v1/fills?...` · `GET /api/v1/portfolio` (balances + valuation from latest mid prices) · `GET /api/v1/candles?symbol=&interval=&from=&to=` · `GET /api/v1/instruments` · `GET /api/v1/export/{orders|fills}.csv`. Cursor-based pagination.

## 6.7 Redis data model

See [§1.5](01-architecture.md#15-redis-usage-kept-off-the-hot-path) for the key spaces. Deployment: Redis 7 primary + replica with Sentinel (3 sentinels), AOF `everysec`. All keys have TTLs, and **no key holds data that can't be rebuilt**. Losing Redis degrades rate limiting and idempotency to per-instance and forces users to log in again, but never affects balances, orders or trades.
