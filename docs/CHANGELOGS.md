# Godis Changelog

## v1.3.3-bugfix (2026-07-24)

### Fixes
- fix: 修复了213个Bug

## v1.3.2 (2026-07-22)

### Features
- feat: HTTP API server with token-based auth (POST /api/auth) and Redis command execution (GET /api/commands)
- feat: X-HEADER-AUTHTOKEN rotating token auth (128-char uppercase, configurable expiry, default 72h)
- feat: comprehensive PRD document (docs/prd.md)
- feat: configurable web dashboard port (WebPort, default 63800, [web] section in TOML)
- feat: port overview in README (TCP 6379, API 63790, Web 63800, Metrics 9121)

## v1.3.1 (2026-07-22)

### Features
- feat: add Helm chart, K8s operator, multi-arch Docker images
- feat(release): build and push multi-arch Docker image to GHCR
- feat: Prometheus metrics with hot/big key monitoring (redis_exporter compatible)
- feat: `--cli` with `--monitor` sub-flag: split-screen TUI (3 monitoring panels + redis-cli)
- feat: `--web` dashboard on :63800 with `-u`, `-h`, `-p`, `-a` flags
- feat: `-u` redis:// URL support in CLI flags
- feat: Entra ID (Azure AD) token-based authentication with JWKS validation

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
