#!/usr/bin/env bash

CGO_ENABLED=0 GOEXPERIMENT=jsonv2 GOOS=linux GOARCH=amd64     go build -tags greenteagc -o target/godis-linux-amd64 ./cmd/godis/
CGO_ENABLED=0 GOEXPERIMENT=jsonv2 GOOS=linux GOARCH=arm64     go build -tags greenteagc -o target/godis-linux-arm64 ./cmd/godis/
CGO_ENABLED=0 GOEXPERIMENT=jsonv2 GOOS=darwin GOARCH=amd64    go build -tags greenteagc -o target/godis-darwin-amd64 ./cmd/godis/
CGO_ENABLED=0 GOEXPERIMENT=jsonv2 GOOS=darwin GOARCH=arm64    go build -tags greenteagc -o target/godis-darwin-arm64 ./cmd/godis/
CGO_ENABLED=0 GOEXPERIMENT=jsonv2 GOOS=windows GOARCH=amd64  go build -tags greenteagc -o target/godis-windows-amd64.exe ./cmd/godis/
