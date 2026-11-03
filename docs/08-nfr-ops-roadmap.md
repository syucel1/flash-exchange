# 8. Non-functional requirements, Operations & Roadmap

## 8.1 Performance targets

| Metric | Target |
|---|---|
| Engine internal (ingress poll → egress offer) | p50 ≤ 3 µs, p99 ≤ 10 µs, p99.9 ≤ 50 µs |
| Trading Service in → ack out (same box as engine, IPC) | p99 ≤ 100 µs |
| Trading Service in → ack out (across network, single-node engine) | p99 ≤ 250 µs (10/25 GbE, kernel bypass optional) |
| With SYNC replication to standby (adds one core-link RTT) | p99 ≤ 150 µs same box, ≤ 300 µs across network |
| Browser order → ack displayed (same region) | ≤ 50 ms (network dominated) |
| Throughput | ≥ 1 M cmds/s engine; ≥ 50 k concurrent WS sessions per Trading Service instance |
| Market data to browser | ≤ 20 updates/s per book, ≤ 100 ms staleness |
| Availability | 99.95 % trading hours; RPO 0 (SYNC replication), RTO < 1 s failover |

## 8.2 Security

- TLS 1.3 everywhere external; Aeron on a private, isolated network segment (VLAN); optional Aeron transport encryption not needed inside the trusted segment.
- Argon2id passwords; TOTP 2FA; account lockout & IP rate limiting; refresh-token rotation with reuse detection.
- JWT RS256/EdDSA, 5-min access tokens, JWKS rotation.
- OWASP ASVS L2 for web; CSP, CSRF protection for cookie endpoints, input validation server-side.
- PSP webhooks: signature verification + idempotency.
- Least-privilege DB roles; secrets in Vault/KMS; encrypted TOTP secrets.
- Immutable audit log (who/what/when) for admin actions, logins, funding.

## 8.3 Observability

- Engine: HdrHistogram per hop (ingress→risk, risk→match, match→publish), ring-buffer remaining capacity, Aeron counters (back-pressure, NAKs, retransmits, lag), snapshot duration — exported off-thread via Agrona counters → Prometheus.
- Trading Service: sessions, msg rates, conflation drop rate, reject reasons, per-hop latency.
- Business dashboards: orders/s, trades/s, volume per instrument, open orders, balances reconciliation status.
- Alerts: latency SLO breach, engine failover / replication downgrade, Redis unavailable, consumer lag, reconciliation break, PSP failures.
- Structured logs (async, off hot path) to Loki; distributed tracing on REST paths only.

## 8.4 Testing strategy

| Level | What |
|---|---|
| Unit | Order book ops, matching rules, fee/hold arithmetic, validation |
| Property-based (jqwik) | Random order streams: invariants — book never crossed, FIFO respected, conservation of funds, holds ≥ 0 |
| Determinism | Replay same log twice / on two nodes → byte-identical output |
| Golden-file | Scenario files (commands → expected events) shared by engine and Trading Service tests |
| Microbenchmarks (JMH) | add/cancel/match per op; allocation profiler must report 0 B/op on hot path |
| Latency harness | Open-loop load generator with coordinated-omission-correct measurement |
| Integration | Testcontainers (PostgreSQL, Redis) + embedded Media Driver |
| E2E | Playwright UI tests; simulated trader bots for soak tests |
| Chaos | Kill leader, drop packets, slow consumer, disk full on Archive |

## 8.5 Operations

- Environments: dev (docker-compose, single embedded driver), test, perf (tuned hardware), prod.
- Trading sessions are 24/7 for spot; maintenance via instrument `HALTED` + snapshot + rolling upgrade (upgrade standby → switchover → upgrade old primary).
- Admin actions: halt/resume instrument, mass-cancel, suspend user, adjust fee tiers — all as engine commands (audited, replayable).
- Backups: Archive recordings replicated off-host; PostgreSQL PITR.

## 8.6 Delivery roadmap

| Phase | Deliverables | Exit criteria |
|---|---|---|
| **0 Foundations** (1–2 wks) | Gradle multi-module repo, SBE schemas v1 + codegen, common lib (fixed-point, ids), Media Driver configs, CI, docker-compose dev stack | Schemas reviewed; build green |
| **1 Matching Engine** (4–6 wks) | Order book, matching (limit/market, GTC/IOC/FOK, post-only, STP), risk holds & settlement, Disruptor pipeline, sequencer + journal (Archive), snapshots & replay, single node, admin commands, JMH + latency harness | All invariants pass; p99 ≤ 10 µs on perf host; replay determinism proven |
| **2 Trading Service** (3–4 wks) | Netty WS/REST, JWT auth, validation, routing, book replicas, conflation, gap recovery, user-state resync | Bot traders through WS end-to-end; resync tests |
| **3 Identity, Funding, Persistence** (3–4 wks) | Registration/login/2FA/settings; deposits with mock PSP + ledger; archive tailer → PostgreSQL; history & candles REST; reconciliation job | Deposit → trade → history visible; reconciliation zero breaks |
| **4 Web Platform** (4–6 wks, can start in parallel with 2 against a mock WS) | All screens of §5.1, themes, responsive layouts | Playwright suite green; Lighthouse ≥ 90 |
| **5 Hardening** (3–4 wks) | Standby engine + replication (101/102) + arbiter, failover drills, OS/JVM tuning, security review, observability dashboards, load & soak tests | SLOs met under 2× expected peak; chaos tests pass |
| **6 Extensions** | Withdrawals, KYC tiers, API keys + FIX/REST for algos, admin console, GTD, realised P&L, price-band circuit breakers | — |
