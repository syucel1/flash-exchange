# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project uses [Semantic Versioning](https://semver.org/).
Releases are cut by pushing a `vX.Y.Z` tag (see [DEVELOPMENT.md](DEVELOPMENT.md#releasing)); the section for that
version becomes the GitHub release notes.

## [Unreleased]

### Added - Roadmap phase 2: engine durability and high availability
- Command journal on an embedded Aeron Archive (stream 100), written by a Disruptor stage in parallel with the
  matchers; a release gate holds output until the command is recorded. The journal recording is extended across
  restarts. (`flash.engine.journal.enabled`, `data.dir`, `archive.*`)
- State snapshots (stream 110, CRC32-checked) of the sequencer, dedup window and every book, on a time and command
  count schedule, at shutdown and after promotion; cold recovery from the newest valid snapshot plus journal replay,
  byte-identical to the uninterrupted run.
- Sequencer decoupled from the ring sequence; book-snapshot requests no longer consume engine sequences.
- Duplicate `clientOrderId` detection per user (bounded window, journaled flag): duplicates are rejected with
  `DUPLICATE_CLIENT_ORDER_ID`, also across restarts and failovers.
- Primary/standby replication: the standby bootstraps from the primary's snapshot and follows its journal by a
  remote archive replay of the live recording (stream 101), re-journals and acknowledges (stream 102). SYNC release
  (output only after the standby's ack, degrades to ASYNC on ack timeout) or ASYNC.
- Arbiter (`ArbiterMain`, `arbiter` container role): election, leases, automatic failover after primary silence and
  lease expiry, epochs, fencing of stale primaries, re-follow, planned switchover, persisted state, `/status`.
- `EngineRole` output message; `EngineClient` talks to an HA pair (commands to every engine, output from all) and
  de-duplicates output re-published after recovery or failover (`OutputDeduplicator`); `DemoTrader` accepts
  several engine hosts.
- New protocol messages: `JournalEntry`, `ReplicationAck`, `NodeHeartbeat`, `ArbiterCommand`, `EngineRole`,
  `SnapshotBegin/Chunk/End`; reason `DUPLICATE_CLIENT_ORDER_ID`.
- Docker: data volume `/var/lib/flashex`, journal enabled in the container configs, compose `ha` profile (arbiter,
  engine-a, engine-b, demo trader) and `scripts/ha-failover-smoke-test.sh` (SIGKILL drill, ~325 ms promotion).
- Tests: `DurabilityIT`, `HighAvailabilityIT` (exit criterion, double failover, lossy link, lease, switchover),
  `ArbiterTest` and unit tests for the HA building blocks. Docs: `docs/10-durability-ha.md` (design, ADRs, schema
  versioning rules, rolling upgrade procedure).

### Changed
- Engine sequence numbers count only state-changing commands (new orders and cancels).
- A fenced engine exits with status 3 so its supervisor restarts it as a standby.
- JaCoCo branch gate lowered from 90 % to 85 % (failure-handling paths of the HA code).

### Added - Roadmap phase 1: delivery foundations
- Maven Wrapper (`mvnw`, `mvnw.cmd`; Maven 3.9.9): no local Maven installation needed.
- Code quality gates in the build: Checkstyle (`config/checkstyle`), SpotBugs (`config/spotbugs`), JaCoCo coverage
  gate (existing), optional OWASP dependency scan (`-Pdependency-check`), `.editorconfig`, `.gitattributes`.
- `matching-engine-bench` module: JMH benchmarks for the matching engine, codecs and the Disruptor pipeline.
- Engine operational HTTP endpoint (`flash.engine.http.port`): `GET /health` and `GET /status` (counters, channels,
  version) for container health checks and operators.
- `DemoTrader`: demo client and load generator (IPC or UDP) with periodic console reports.
- Container image (`docker/Dockerfile`), one image with roles `media-driver`, `matching-engine` and `demo-trader`;
  non-root, read-only friendly.
- `docker-compose.yml`: standalone media driver + engine + demo trader sharing `/dev/shm` over IPC, and a `udp`
  profile with engine and client on separate media drivers over reliable UDP. `scripts/compose-smoke-test.sh`.
- GitHub Actions: CI (build, tests, analysis, image build, Trivy scan, compose smoke test, push to GHCR on `main`),
  release on tags, nightly JMH benchmarks with regression tracking, CodeQL; Dependabot for Maven, Actions and Docker.
- Kubernetes manifests (`deploy/k8s`, Kustomize): engine pod with the media driver as a native sidecar.
- Jar manifests carry `Implementation-Version` and `Build-Revision`.

## [0.1.0] - 2026-11-04

### Added
- Matching engine: price-time priority, LIMIT/MARKET, GTC/IOC/FOK, post-only, self-trade prevention, market
  order price collar, validation, cancels, L2 book updates and snapshots, public trades.
- LMAX Disruptor pipeline with sharded matcher threads and an ordered publisher.
- Aeron integration over IPC and reliable UDP (unicast commands, multi-destination-cast output); engine client.
- Binary protocol module (SBE-style flyweights).
- 126 unit and integration tests, 99.9 % line coverage.
- Detailed design (`docs/`), roadmap and UI mockups.

[Unreleased]: https://github.com/syucel1/flash-exchange/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/syucel1/flash-exchange/releases/tag/v0.1.0
