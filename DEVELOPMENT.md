# Development guide

## Prerequisites

| Tool | Version | Notes |
|---|---|---|
| JDK | 21+ | Temurin recommended |
| Docker | 24+ with Compose v2 | Docker Desktop on Windows/macOS |
| Maven | not required | use the wrapper: `./mvnw` (Linux/macOS) or `mvnw.cmd` (Windows) |

## Repository layout

| Path | Contents |
|---|---|
| `protocol/` | Binary message flyweights shared by the engine and its clients |
| `matching-engine/` | Matching engine, Disruptor pipeline, Aeron transport, HTTP status endpoint, demo trader |
| `matching-engine-bench/` | JMH benchmarks |
| `config/` | Checkstyle and SpotBugs rules |
| `docker/` | Dockerfile, entrypoint, container configuration |
| `docker-compose.yml` | Local stack (IPC, plus `udp` profile) |
| `deploy/k8s/` | Kustomize manifests |
| `scripts/` | Smoke test and helper scripts |
| `.github/` | CI/CD workflows, Dependabot |
| `docs/`, `ROADMAP.md`, `mockups/` | Design, roadmap, UI mockups |

## Build and test

```bash
./mvnw verify                       # everything: checkstyle, unit + Aeron integration tests, coverage gate, SpotBugs
./mvnw test                         # unit tests only
./mvnw -pl matching-engine -am verify -Dit.test=MatchingEngineServerIT   # one integration test class
./mvnw -DskipTests package          # jars only (matching-engine/target/matching-engine-<version>-all.jar)
./mvnw -Pdependency-check verify    # OWASP dependency scan (set NVD_API_KEY to speed it up)
```

Quality gates (all fail the build):

| Gate | Phase | Skip locally with |
|---|---|---|
| Checkstyle (`config/checkstyle/checkstyle.xml`): Allman braces, final parameters, 120 columns, Javadoc on public API | `validate` | `-Dcheckstyle.skip` |
| Unit tests (`*Test`), integration tests (`*IT`, real media drivers) | `test`, `integration-test` | `-DskipTests` |
| JaCoCo: ≥ 95 % lines, ≥ 90 % branches per module | `verify` | `-Djacoco.skip` |
| SpotBugs (medium+, `config/spotbugs/exclude.xml`) | `verify` | `-Dspotbugs.skip` |

Reports: `*/target/site/jacoco/index.html`, `*/target/spotbugsXml.xml`, `*/target/checkstyle-result.xml`.

### Running from the IDE

Add these JVM options to run configurations (Agrona 2 / Aeron):
`--add-exports java.base/jdk.internal.misc=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED`

| Main class | Purpose |
|---|---|
| `com.flashexchange.engine.MatchingEngineMain [engine.properties]` | Engine |
| `com.flashexchange.engine.tools.DemoTrader [demo.properties]` | Demo client / load generator |
| `io.aeron.driver.MediaDriver [driver.properties]` | Standalone media driver |

On Windows set `flash.engine.aeron.dir` / `flash.demo.aeron.dir` to a local directory (there is no `/dev/shm`).

## Local stack with Docker Compose

```bash
./mvnw -DskipTests package
docker compose up --build                       # media-driver + matching-engine + demo-trader (IPC)
docker compose --profile udp up --build         # also: engine and demo trader on separate drivers over UDP
curl localhost:8080/status                      # IPC engine; the UDP engine is on :8081
docker compose logs -f demo-trader              # periodic trading summary
docker compose --profile udp down -v
bash scripts/compose-smoke-test.sh --udp        # what CI runs: asserts traffic, no rejects, no exceptions
```

How IPC works between containers: the `media-driver` service owns a shareable IPC namespace with a 512 MiB
`/dev/shm`. The engine and the demo trader join it (`ipc: service:media-driver`), so all three see the same
Aeron directory `/dev/shm/aeron`, as processes on one host would. In the `udp` profile each container runs its own
embedded driver and traffic crosses the Docker network as Aeron reliable UDP.

### Image

One image, `ghcr.io/syucel1/flash-exchange`, with the role chosen by the first argument:
`media-driver`, `matching-engine [props]`, `demo-trader [props]`, `health`, `driver-health`.
Configuration lives in `/opt/flashex/config` (mount your own to override) and `JAVA_OPTS` sets JVM flags.
The image runs as uid 10001 and works with a read-only root filesystem (mount `/tmp` and `/dev/shm`).

## Kubernetes

```bash
kubectl apply -k deploy/k8s/base            # engine pod (media driver as a native sidecar, K8s >= 1.29)
kubectl apply -k deploy/k8s/overlays/dev    # + demo trader in the same pod
kubectl -n flashex port-forward deploy/matching-engine 8080 && curl localhost:8080/status
```

## Benchmarks

```bash
./mvnw -DskipTests -Dcheckstyle.skip -Dspotbugs.skip package -pl matching-engine-bench -am
java --add-exports java.base/jdk.internal.misc=ALL-UNNAMED -jar matching-engine-bench/target/benchmarks.jar
java ... -jar matching-engine-bench/target/benchmarks.jar MatchingEngineBenchmark -prof gc   # allocation check
```

Nightly CI runs all benchmarks, keeps the history on the `gh-pages` branch (`/benchmarks`) and flags regressions
over 50 %.

## CI/CD

| Workflow | Trigger | What it does |
|---|---|---|
| `ci.yml` | push to `main`, PRs | `mvnw verify` → reports → image build → Trivy scan → compose smoke test (IPC + UDP) → push `ghcr.io/syucel1/flash-exchange:{main,sha-…}` (main only) |
| `release.yml` | tag `vX.Y.Z` | set version, verify, push image `X.Y.Z` / `X.Y`, GitHub release with the engine jar and CHANGELOG notes |
| `benchmarks.yml` | nightly, manual | JMH run, results artifact, regression tracking |
| `codeql.yml` | push, PRs, weekly | CodeQL security analysis |
| Dependabot | weekly | Maven, GitHub Actions and base image updates |

The first push to GHCR creates a private package; make it public or grant access in the package settings.
Enable GitHub Pages for the `gh-pages` branch to browse benchmark charts.

## Releasing

1. Move the `[Unreleased]` entries in `CHANGELOG.md` under a new `## [X.Y.Z] - date` heading and commit.
2. `git tag vX.Y.Z && git push origin vX.Y.Z`
3. The release workflow builds, tests and publishes the image and the GitHub release.

Versions follow SemVer. `main` builds are `0.x.y-SNAPSHOT` and images are tagged with the branch and commit SHA.

## Conventions

* Branch from `main`; PRs need a green CI.
* Line endings are normalised by `.gitattributes` (LF; CRLF for `*.cmd`). After pulling this change once, run
  `git add --renormalize .` to fix files committed with CRLF.
* Scripts are run with `sh ./mvnw` / `bash scripts/...` in CI, so they work even if the executable bit is lost
  on Windows. To set it in git: `git update-index --chmod=+x mvnw scripts/*.sh docker/entrypoint.sh`.
