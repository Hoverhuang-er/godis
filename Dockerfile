# syntax=docker/dockerfile:1
ARG GO_VERSION=1.26

FROM golang:${GO_VERSION}-alpine AS builder
RUN apk add --no-cache git
WORKDIR /build

# Cache Go module downloads (shared across buildx platforms)
COPY go.mod go.sum ./
COPY patches/ ./patches/
RUN --mount=type=cache,target=/go/pkg/mod \
    GOEXPERIMENT=jsonv2 go mod download

# Build godis binary
COPY . .
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOEXPERIMENT=jsonv2 CGO_ENABLED=0 \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -tags greenteagc -ldflags="-s -w" -o godis ./cmd/godis/

# ---- Runtime stage ----
FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata
WORKDIR /data

COPY --from=builder /build/godis /usr/local/bin/godis
COPY config/standalone.toml /etc/godis/standalone.toml

ENV CONFIG=/etc/godis/standalone.toml
EXPOSE 6399
ENTRYPOINT ["godis"]
