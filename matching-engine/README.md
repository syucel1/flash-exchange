# Matching Engine

Price-time priority matching engine for Flash Exchange spot currency pairs. Commands arrive over Aeron, are
processed by an LMAX Disruptor pipeline, and executions plus live order-book market data go back out over Aeron.
Aeron runs over **shared memory (IPC)** when components share a box, and over **reliable UDP** when they don't.
You choose between them in configuration.

Scope so far: the matching engine, its Aeron integration, and (roadmap phase 2) durability and high availability:
a command journal and state snapshots on Aeron Archive, cold recovery, and a primary/standby pair supervised by an
arbiter (details in [docs/10-durability-ha.md](../docs/10-durability-ha.md)). Balances, settlement and the trading
service come later (see `docs/` and the roadmap).

## Architecture

```
            Aeron stream 1001 (commands)                                Aeron stream 2001 (execution events)
 clients ─────────────────────────────▶ CommandIngress ──┐         ┌──▶ ExecutionReport, TradeExecution
                                        (Agrona agent,   │         │
                                         SBE decode,     ▼         │    Aeron stream 3001 (market data)
                                         stamps seq/ts)  ring  ──▶ matcher shard 0..N ──▶ publisher ──▶ BookUpdate, BookSnapshot,
                                                         (Disruptor, pre-allocated OrderCommand slots)      PublicTrade
```

| Package | Responsibility |
|---|---|
| `com.flashexchange.protocol` (module `protocol`) | Zero-allocation, little-endian, SBE-style flyweights for every message, enums with wire codes, `EngineMessageDispatcher` for decoding engine output |
| `engine.matching` | `MatchingEngine`, `OrderBook`, `BookSide` (sorted level array, best at end, plus price→level hash index), `PriceLevel` (intrusive FIFO), pooled `Order`s |
| `engine.pipeline` | Disruptor wiring (`EnginePipeline`), ring slot (`OrderCommand`), matcher and publisher stages, `EncodingEventSink`, back-pressure-aware `OfferingMessagePublisher` |
| `engine.transport` | `AeronTransport` (optional embedded media driver, subscription, exclusive publications) and `CommandIngress` (decodes commands into the ring, stamps sequence, timestamp, epoch and duplicate flag) |
| `engine.node` | `EngineConductor` (single ring producer and role state machine: STANDALONE / PENDING / PRIMARY / STANDBY / FENCED), `EngineCore` (sequencer + pipeline + ingress, restore and journal replay), `EpochFile` |
| `engine.durability` | `EngineArchive` (embedded Aeron Archive, journal open/extend, remote archive client), `JournalStage` (Disruptor stage writing stream 100), `DurabilityController` (release gate), `SnapshotStore` (stream 110 snapshots with CRC) |
| `engine.ha` | `Sequencer` and `DedupWindow` (duplicate `clOrdId`), `ReplicationLink` (acks on 102, SYNC/ASYNC, lease, fencing), `Arbiter`, `ArbiterServer`, `ArbiterMain` |
| `engine.config` | `EngineConfig` (builder and properties), `EngineChannels` (IPC / UDP channel presets for the engine and client sides) |
| `engine.client` | `EngineClient`: client side of the Aeron interface (one engine or an HA pair), `OutputDeduplicator` (drops output re-published after recovery/failover); used by the tests, the demo trader and later the trading service |
| `engine` | `MatchingEngineServer` (composition root), `MatchingEngineMain` (process entry point) |

### Threads

| Thread | Work |
|---|---|
| `engine-conductor` | Polls stream 1001 (or, as standby, the primary's journal on 101), claims a ring slot, assigns `engineSequence`, `orderId = sequence + 1`, timestamp and duplicate flag; HA heartbeats, arbiter commands, acks, snapshot timer |
| `engine-pipeline-0..N-1` | Matcher shards: a shard owns the instruments where `instrumentId mod N == shard`, so each book has exactly one writer and no locks |
| `engine-pipeline-N` | Journal stage (when enabled): writes each command to the journal (stream 100) in parallel with matching |
| `engine-pipeline-N+1` | Publisher and release gate: waits until the journal entry is recorded (and, SYNC HA, acked by the standby), then sends the output in engine-sequence order to streams 2001 and 3001; writes state snapshots; retains output as standby |
| archive | Embedded Aeron Archive (SHARED threading) recording the journal and snapshots |
| media driver | Embedded driver (SHARED: 1 thread; DEDICATED: 3 threads) or an external driver process |

## Matching rules

* **Price-time priority.** The best price matches first; within a level, the oldest order matches first. Trades execute at the **resting (maker) price**.
* **LIMIT GTC** (default): any unfilled remainder rests on the book. **LIMIT IOC**: the remainder is cancelled (`IOC_REMAINDER`). **FOK**: filled completely or cancelled (`FOK_NOT_FILLABLE`) without touching the book.
* **MARKET**: never rests; GTC is treated as IOC. Matching is bounded by a price collar (`collarBps` around the best opposite price at entry). A remainder is cancelled with `PRICE_COLLAR` (liquidity exists beyond the collar) or `NO_LIQUIDITY`.
* **Post-only** limit orders that would trade on entry are rejected (`POST_ONLY_WOULD_TAKE`).
* **Self-trade prevention**, set on the incoming order: `NONE`, `CANCEL_TAKER`, `CANCEL_MAKER`, `CANCEL_BOTH`.
* **Validation**: instrument, enums, quantity (> 0, lot multiple, min/max), price (> 0, tick multiple; 0 for market orders). Invalid orders get a single `REJECTED` report.
* **Cancel**: only by the owning user; otherwise `CANCEL_REJECTED` with `UNKNOWN_ORDER`, `NOT_ORDER_OWNER` or `UNKNOWN_INSTRUMENT`.

Events for one accepted order, in order: `NEW`; then for each fill a `TradeExecution`, the maker's `TRADE` report and the taker's `TRADE` report; then `CANCELED` for any remainder that doesn't rest; then **one** `BookUpdate` holding the new totals of every level the command changed. Each fill also produces a `PublicTrade` on the market data stream.

## Messages

All messages start with an 8-byte header (`blockLength`, `templateId`, `schemaId=7001`, `version`), little-endian.

| Stream | Template | Message |
|---|---|---|
| 1001 in | 1 / 2 / 3 | `NewOrder`, `CancelOrder`, `BookSnapshotRequest` (instrument 0 = all) |
| 2001 out | 101 | `ExecutionReport`: NEW, TRADE, CANCELED, REJECTED, CANCEL_REJECTED, with cumulative and leaves quantity, maker/taker flag and reason |
| 2001 out | 102 | `TradeExecution`: both sides of a trade, for downstream settlement and portfolio systems |
| 3001 out | 201 | `BookUpdate`: incremental L2 with per-instrument `bookSequence`; quantity 0 means the level was removed |
| 3001 out | 202 | `BookSnapshot`: full or depth-limited L2, best first; carries the `bookSequence` it reflects. Sent on request and every `snapshot.intervalMs` |
| 3001 out | 203 | `PublicTrade` |
| 2001 + 3001 out | 130 | `EngineRole`: role and epoch, published on start (with a journal), promotion and recovery; consumers use it to drop re-published output |
| 100 journal | 50 | `JournalEntry` (recorded by the archive; replicated to the standby on 101) |
| 110 snapshot | 60 / 61 / 62 | `SnapshotBegin`, `SnapshotChunk`, `SnapshotEnd` (CRC32) |
| 102 | 51 | `ReplicationAck` (standby → primary) |
| 9001 / 9002 | 52 / 53 | `NodeHeartbeat` (node → arbiter), `ArbiterCommand` PROMOTE / FOLLOW / LEASE / FENCE |

How a consumer keeps a live book: apply a snapshot, then apply updates whose `bookSequence` is exactly `last + 1`. On a gap, request a new snapshot.

## Transport: IPC vs reliable UDP

| | Engine side | Client side (e.g. trading service) |
|---|---|---|
| Same box | `flash.engine.transport=IPC`: all streams `aeron:ipc` through the shared media driver directory | `EngineChannels.ipc()` and connect to the same Aeron directory |
| Across network | `flash.engine.transport=UDP` with `udp.host` and ports: commands are unicast to `host:commandPort`; 2001 and 3001 use multi-destination-cast with dynamic control, so any number of gateways can subscribe and the engine sends each message once | `EngineChannels.udpClientSide(engineHost, clientHost, 40101, 40201, 40301)` on the client's own media driver |

Explicit channels (for example multicast market data) can override the defaults with `flash.engine.channel.*`.
See `config/engine-ipc.properties` and `config/engine-udp.properties`.

## Build, test, run

Requires JDK 21+ and Maven 3.9+. Dependencies (Aeron 1.48.6, Agrona 2.2.4, Disruptor 4.0.0, JUnit 5.11) are
downloaded from Maven Central on the first build.

```bash
mvn verify                                  # unit tests + Aeron integration tests + JaCoCo coverage check
mvn -pl matching-engine -am test            # unit tests only
# coverage report: matching-engine/target/site/jacoco/index.html, protocol/target/site/jacoco/index.html

java -jar matching-engine/target/matching-engine-0.1.0-SNAPSHOT-all.jar matching-engine/config/engine-ipc.properties
# override any key: java -Dflash.engine.matcherShards=2 -jar ... ; stop with Ctrl+C (SIGINT/SIGTERM)
```

Running from an IDE or `java -cp`: add `--add-exports java.base/jdk.internal.misc=ALL-UNNAMED
--add-opens java.base/sun.nio.ch=ALL-UNNAMED`. The shaded jar's manifest already does this. On Windows, set
`flash.engine.aeron.dir` to a local path (there is no `/dev/shm`).

### Tests

| Suite | What it covers |
|---|---|
| `MatchingEngineTest` | Every matching rule: priority, partial fills, multi-level sweeps, market orders and the collar, IOC, FOK, post-only, all self-trade-prevention modes, validation, cancels, book updates, snapshots, pool recycling |
| `MatchingEngineRandomizedTest` | Differential test against a simple reference model over 30k random commands; checks the book invariants (never crossed, aggregates equal the sum of orders, published L2 equals the book) |
| `BookStructuresTest` | `PriceLevel`, `BookSide`, `ObjectPool`, `Instrument` |
| protocol `*Test` | Codec round trips, field overlap guard, header validation, dispatcher |
| pipeline / config / ingress `*Test` | Disruptor pipeline (all wait strategies, sharding, ordering, failure isolation, shutdown), encoder, back-pressure policy, configuration parsing, command decoding, periodic snapshots |
| `MatchingEngineServerIT` | End to end over real media drivers: IPC; **UDP between independent media drivers with two subscribing gateways**; periodic snapshots; 6,000 orders across 3 shards; malformed input; no-subscriber drops; external driver; launch failure cleanup; `main` |
| `DurabilityIT` | Crash recovery to byte-identical state, snapshot + journal tail, shutdown snapshot, duplicate `clOrdId` across restarts, corrupt snapshots skipped, client de-duplication |
| `HighAvailabilityIT` | Primary killed under load → standby promoted in < 1 s with no lost or duplicated fills, double failover, ASYNC, SYNC degrade, lease expiry, planned switchover with fencing, 10 % packet loss on the standby |
| `ArbiterTest`, `ReplicationLinkTest`, `SequencerTest`, `DedupWindowTest`, `OutputDeduplicatorTest`, `MatchingEngineStateTest` | HA building blocks |

Coverage at the time of writing: **96% lines, 85% branches** for the engine module (protocol: 99%). The build
fails below 95% lines or 85% branches.

### Indicative performance

Single matcher thread, including encoding of all output, on a 2-vCPU cloud sandbox: about **1.7 M orders/s**,
p50 ≈ 0.4 µs, p99 ≈ 1.3 µs per command, measured in-process without Aeron. Tuned hosts with `BUSY_SPIN`, a
`DEDICATED` driver and pinned cores will do better. Benchmark with JMH before relying on these numbers.

## Operations endpoint and demo trader

* `flash.engine.http.port=8080` enables a small HTTP endpoint on its own thread: `GET /health` (200 UP / 503 DOWN,
  used by container health checks) and `GET /status` (version, channels, counters, publisher drops). Disabled (-1)
  by default.
* `com.flashexchange.engine.tools.DemoTrader [demo.properties]` simulates users sending limit and market orders and
  cancels over IPC or UDP and prints a trading summary. It powers the docker-compose stack and the smoke test (see
  `docker/config/demo-*.properties`).

## Durability and HA in one minute

```bash
# standalone engine with a journal: restart it and it recovers its books
java -Dflash.engine.journal.enabled=true -Dflash.engine.data.dir=/tmp/flash -jar matching-engine-*-all.jar config/engine-ipc.properties
# HA pair + arbiter in containers, then a kill -9 failover drill
docker compose --profile ha up -d && bash scripts/ha-failover-smoke-test.sh
```

`/status` shows `role`, `epoch`, `lastSequence`, `journalPosition` / `journalRecordedPosition`, `snapshotsWritten`,
and for HA `leaseValid`, `standbyAttached`, `degraded`, `ackedSequence`, `followPosition`, `peerVisible`.

## Next steps

Metrics (phase 3), risk and balance holds (phase 6), the trading service, cancel/replace, GTD orders, thread
affinity.
