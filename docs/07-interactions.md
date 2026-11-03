# 7. Interactions

## 7.1 Registration

```mermaid
sequenceDiagram
    actor U as User
    participant W as Web SPA
    participant ID as Identity Service
    participant DB as PostgreSQL
    participant MC as Matching Engine
    participant M as Email provider
    U->>W: fill registration form
    W->>ID: POST /auth/register {email, password, country}
    ID->>ID: validate, Argon2id hash, rate limit
    ID->>DB: insert user(status=PENDING_VERIFICATION), default settings
    ID->>M: send verification link
    ID-->>W: 201 Created
    U->>ID: GET /auth/verify?token=…
    ID->>DB: status=ACTIVE
    ID->>MC: AddUser(userId) via Aeron (creates zero balances)
    MC-->>ID: UserAdded
    ID-->>U: redirect to login
```

## 7.2 Login → live session

See [Web Platform §5.4](05-web-platform.md#54-auth-flow). After WS auth, the Trading Service sends `RequestUserState` if the user's state is not cached, then pushes `balances` and `orders` snapshots followed by live updates.

## 7.3 Deposit

```mermaid
sequenceDiagram
    actor U as User
    participant W as Web SPA
    participant F as Funding Service
    participant PSP as Payment provider (mock)
    participant DB as PostgreSQL
    participant MC as Matching Engine
    participant TS as Trading Service
    U->>W: Deposit 10,000 USD
    W->>F: POST /deposits {ccy, amount, method}
    F->>DB: deposit PENDING
    F->>PSP: create payment
    PSP-->>W: redirect / bank reference
    PSP->>F: webhook payment.succeeded (signed)
    F->>DB: ledger DR clearing / CR customer, deposit CONFIRMED (idempotent on psp_ref)
    F->>MC: Deposit(userId, USD, 10000, fundingTxId)
    MC->>MC: available[USD] += 10000
    MC-->>F: BalanceUpdate(reason=DEPOSIT, refId)
    F->>DB: deposit CREDITED
    MC-->>TS: BalanceUpdate (user events stream)
    TS-->>W: balances update + toast "Deposit credited"
```

## 7.4 Place limit order (rests on book)

```mermaid
sequenceDiagram
    actor U as User
    participant W as Web SPA
    participant TS as Trading Service
    participant R as Core: Risk
    participant ME as Core: Matcher
    participant MD as Core: Market data
    U->>W: BUY 10,000 EURUSD @ 1.08345 GTC
    W->>W: client validation (tick, lot, funds)
    W->>TS: order.new {clOrdId, …}
    TS->>TS: auth, validate, idempotency, rate limit, soft funds
    TS->>R: NewOrder (1001) → sequenced, journaled (100), replicated (101)
    R->>R: hold 10,834.50 USD + fee
    R->>ME: (Disruptor) command
    ME->>ME: no cross → add to bid level 1.08345 (tail of FIFO)
    ME-->>TS: OrderAccepted(status=NEW)
    ME->>MD: level changed
    MD-->>TS: BookDelta(bid 1.08345 total, count, seq)
    R-->>TS: BalanceUpdate(held)
    TS-->>W: ack ok · orders(NEW) · balances
    TS-->>W: book.delta (conflated, all subscribers)
    W->>W: open-orders blotter adds row, book highlights own level
```

## 7.5 Market order matched against several makers

```mermaid
sequenceDiagram
    actor T as Taker user
    participant TS as Trading Service(s)
    participant R1 as Core: Risk-Hold
    participant ME as Core: Matcher
    participant R2 as Core: Settle
    participant MD as Core: Market data
    actor M as Maker users
    T->>TS: MARKET SELL 30,000 EURUSD
    TS->>R1: NewOrder(MARKET, IOC)
    R1->>R1: hold 30,000 EUR
    R1->>ME: command
    ME->>ME: compute collar = bestBid × 0.95
    ME->>ME: fill 20,000 @1.08345 (maker A, oldest)
    ME->>ME: fill 10,000 @1.08340 (maker B)
    ME->>R2: trades
    R2->>R2: settle taker & makers, fees
    R2-->>TS: ExecutionReport×2 (taker, TAKER) → status FILLED
    R2-->>TS: ExecutionReport (maker A, MAKER, FILLED), (maker B, PARTIALLY_FILLED)
    R2-->>TS: BalanceUpdate × 3 users
    ME->>MD: levels changed + trades
    MD-->>TS: BookDelta + PublicTrade×2
    TS-->>T: fills · order FILLED · balances
    TS-->>M: fills · orders updated · balances · toast
```

If liquidity runs out or the collar is reached, the remainder is cancelled: `OrderCancelled(reason=MARKET_NO_LIQUIDITY)` and the unused hold released.

## 7.6 Cancel order

```mermaid
sequenceDiagram
    actor U as User
    participant W as Web SPA
    participant TS as Trading Service
    participant MC as Matching Engine
    U->>W: click ✕ on open order
    W->>TS: order.cancel {orderId}
    TS->>TS: ownership check (cache), rate limit
    TS->>MC: CancelOrder(userId, orderId)
    alt order still open
        MC->>MC: unlink from FIFO, release hold
        MC-->>TS: OrderCancelled(USER) + BalanceUpdate + BookDelta
        TS-->>W: orders(CANCELLED) · balances · book.delta
    else already filled / unknown
        MC-->>TS: CancelRejected(TOO_LATE / UNKNOWN_ORDER)
        TS-->>W: ack ok=false
    end
```

## 7.7 Order book subscription & resync

```mermaid
sequenceDiagram
    participant W as Web SPA (worker)
    participant TS as Trading Service
    participant MC as Matching Engine
    W->>TS: subscribe book.EURUSD:50
    TS-->>W: snapshot(seq=S)
    loop every ≤50 ms while changes
        TS-->>W: delta(seq=S+n)
    end
    Note over W: gap or WS reconnect
    W->>TS: re-subscribe
    TS-->>W: fresh snapshot
    Note over TS,MC: TS replica itself gapped →<br/>RequestBookSnapshot to MC, mark channel STALE to clients
```

## 7.8 Engine failover (primary → standby)

```mermaid
sequenceDiagram
    participant TS as Trading Services
    participant P as Engine A (PRIMARY, epoch e)
    participant S as Engine B (STANDBY)
    participant W as Arbiter
    participant H as History Service
    TS->>P: commands (1001)
    P->>S: journal replication (101)
    S-->>P: ack + heartbeat (102)
    P-->>TS: events released after ack (2001/3001, epoch e)
    Note over P: host A fails
    S->>W: primary silent 50 ms
    W->>W: also no heartbeat from A → decide
    W->>S: PROMOTE(epoch e+1)
    S->>S: apply remaining replicated journal, open ring for input
    S-->>TS: Role(PRIMARY, e+1) on 2001/3001
    S-->>H: Role(PRIMARY, e+1)
    TS->>S: switch 1001 destination, resend un-acked by clOrdId
    S-->>TS: duplicates answered with original status, new events continue (engineSeq contiguous)
    Note over P: A returns → sees epoch e+1 → rejoins as STANDBY (snapshot + replay 4001)
```

## 7.9 Order state machine

```mermaid
stateDiagram-v2
    [*] --> PENDING_NEW: submitted by TS
    PENDING_NEW --> REJECTED: validation / funds / band
    PENDING_NEW --> NEW: accepted, resting
    PENDING_NEW --> PARTIALLY_FILLED: crossed partially (GTC rests)
    PENDING_NEW --> FILLED: fully crossed
    PENDING_NEW --> CANCELLED: IOC/market remainder, FOK unfilled
    NEW --> PARTIALLY_FILLED: maker fill
    NEW --> FILLED
    NEW --> CANCELLED: user / admin / STP
    PARTIALLY_FILLED --> PARTIALLY_FILLED: further fill
    PARTIALLY_FILLED --> FILLED
    PARTIALLY_FILLED --> CANCELLED
    REJECTED --> [*]
    FILLED --> [*]
    CANCELLED --> [*]
```

`PENDING_NEW` exists only in the Trading Service / UI (between submit and engine response); the engine itself emits only final-of-command states.
