# Changelog

## v1.3.1 (2026-07-21)

### Features
- TOML configuration format support (standalone.toml, cluster.toml)
- Nacos configuration center integration via nacos-go-sdk
- Redis 8.8.0 command compatibility
- RediSearch support (FT.CREATE, FT.SEARCH, FT.DROPINDEX, etc.)
- Time Series support (TS.CREATE, TS.ADD, TS.GET, TS.RANGE)
- Redis-Vector support (VECTOR field type, KNN search)
- Arena memory management for reduced GC pressure
- Compatible with rueidis and go-redis clients

### Improvements
- Configuration loading refactored with format auto-detection
- Multiple config sources supported (local file, Nacos)
- Cross-platform release packages include standalone.toml
- CI/CD pipeline with GitHub Actions for tests and releases

## v1.3.0 (2026-07-14)

### Features
- RESP3 protocol support
- Array data type (AR* commands)
- INCREX rate limiter
- ZUNION/ZINTER with COUNT
- Hash field-level notifications
- Streams with XNACK
- Compatible with go-redis

### Improvements
- Performance optimizations
- Memory usage improvements
