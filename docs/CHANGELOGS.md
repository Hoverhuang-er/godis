# Godis Changelog

## v1.3.1 (2026-07-22)

### Features
- feat: add Helm chart, K8s operator, multi-arch Docker images
- feat(release): build and push multi-arch Docker image to GHCR
- feat: Prometheus metrics with hot/big key monitoring (redis_exporter compatible)
- feat: `--cli` flag for built-in redis-cli (supports `-h`, `-p`, `-a` flags)

### Improvements
- refactor: restructure project to Go Project Layout
- cleanup: remove legacy .conf, merge build scripts into Makefile
- chore: rename module to github.com/Hoverhuang-er/godis
- build: GOEXPERIMENT=greenteagc for GC tuning (GCPercent=40, thread pinning)
- build: GOAMD64=v3 for x86-64 SIMD (AVX2, AVX, FMA, BMI instruction set)
- build: greenteagc runtime init now correctly imported and activated
- build: Docker buildx layer caching (type=gha,mode=max)
- feat: `--cli` with SUBSCRIBE/PSUBSCRIBE support for real-time push messages
- perf: concurrent TCC — parallel prepare/commit/rollback across nodes (goroutines + sync.WaitGroup)
- perf: RelayWorkerPool — channel-based goroutine pool per peer for async relay
- perf: parallel relay helper with early-error cancellation (context cancellation)
- config: add cluster.worker_pool and cluster.relay_parallel options (default: true)
- docs: Prometheus monitoring section in EN, CN, JA, FI READMEs
- docs: fix language links (blob/master/ → blob/master/docs/)
- docs: add root README.md symlink for GitHub homepage rendering
- docs: Mermaid bar chart replaces raw benchmark text

### Fixes
- fix: add riscv64 support to boltdb/bolt via local patch
- fix(release): remove unsupported windows/riscv64 and darwin/riscv64 targets
- fix(release): partial build failure does not block release
- fix(release): upload-checksums job indentation
- fix(release): add packages: write permission for GHCR push
- Fix test.yml: add GOEXPERIMENT=jsonv2 at step level
- fix: suppress spurious "open rdb file failed" error on fresh startup
- fix: error wrapping in loadRdbFile (use %w for errors.Is compatibility)

### Previous Releases

See git log for detailed history.
