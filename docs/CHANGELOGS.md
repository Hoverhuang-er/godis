# Godis Changelog

## v1.3.1 (2026-07-22)

### Features
- feat: add Helm chart, K8s operator, multi-arch Docker images
- feat(release): build and push multi-arch Docker image to GHCR
- feat: Prometheus metrics with hot/big key monitoring (redis_exporter compatible)
- feat: `--cli` flag for built-in redis-cli (supports `-h`, `-p`, `-a` flags)
- feat: `--web` flag for web dashboard with Fluent UI (query + monitoring)
- feat: `--monitor` flag for TUI dashboard with real-time charts (ANSI terminal)
- feat: Entra ID (Azure AD) token-based authentication with JWKS validation
- fix: auto-index string SET keys via search.IndexDocByPrefix
- fix: auto-remove DEL keys from search indexes via search.RemoveDocByPrefix
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
