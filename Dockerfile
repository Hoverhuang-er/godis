ARG GO_VERSION=1.26

# ---- Build stage ----
FROM golang:${GO_VERSION}-alpine AS builder
RUN apk add --no-cache git
WORKDIR /build

# Cache dependencies
COPY go.mod go.sum ./
COPY patches/ ./patches/
RUN GOEXPERIMENT=jsonv2 go mod download

# Build
COPY . .
ARG TARGETOS TARGETARCH
RUN GOEXPERIMENT=jsonv2 CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
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
