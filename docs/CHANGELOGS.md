# Godis Changelog

## v1.3.1 (2026-07-22)

### Features
- feat: add Helm chart, K8s operator, multi-arch Docker images
- feat(release): build and push multi-arch Docker image to GHCR

### Improvements
- refactor: restructure project to Go Project Layout
- cleanup: remove legacy .conf, merge build scripts into Makefile
- chore: rename module to github.com/Hoverhuang-er/godis

### Fixes
- fix: add riscv64 support to boltdb/bolt via local patch
- fix(release): remove unsupported windows/riscv64 and darwin/riscv64 targets
- fix(release): partial build failure does not block release
- fix(release): upload-checksums job indentation
- fix(release): add packages: write permission for GHCR push
- Fix test.yml: add GOEXPERIMENT=jsonv2 at step level

### Previous Releases

See git log for detailed history.
