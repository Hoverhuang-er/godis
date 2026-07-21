BINARY   := godis
GO       := go
GOEXPERIMENT := jsonv2,greenteagc
CGO_ENABLED  := 0
TAGS     := greenteagc
LDFLAGS  := -s -w
TARGET   := target

.PHONY: all build build-linux build-darwin build-windows release test clean run

all: build

build:
	GOEXPERIMENT=$(GOEXPERIMENT) CGO_ENABLED=$(CGO_ENABLED) \
		$(GO) build -tags $(TAGS) -ldflags="$(LDFLAGS)" -o $(TARGET)/$(BINARY) ./cmd/godis/

build-linux:
	GOOS=linux GOARCH=amd64 GOAMD64=v3 GOEXPERIMENT=$(GOEXPERIMENT) CGO_ENABLED=$(CGO_ENABLED) \
		$(GO) build -tags $(TAGS) -ldflags="$(LDFLAGS)" -o $(TARGET)/$(BINARY)-linux-amd64 ./cmd/godis/

build-darwin:
	GOOS=darwin GOARCH=amd64 GOAMD64=v3 GOEXPERIMENT=$(GOEXPERIMENT) CGO_ENABLED=$(CGO_ENABLED) \
		$(GO) build -tags $(TAGS) -ldflags="$(LDFLAGS)" -o $(TARGET)/$(BINARY)-darwin-amd64 ./cmd/godis/

build-windows:
	GOOS=windows GOARCH=amd64 GOAMD64=v3 GOEXPERIMENT=$(GOEXPERIMENT) CGO_ENABLED=$(CGO_ENABLED) \
		$(GO) build -tags $(TAGS) -ldflags="$(LDFLAGS)" -o $(TARGET)/$(BINARY)-windows-amd64.exe ./cmd/godis/

release:
	GOOS=linux GOARCH=amd64 GOAMD64=v3 GOEXPERIMENT=$(GOEXPERIMENT) CGO_ENABLED=$(CGO_ENABLED) $(GO) build -tags $(TAGS) -ldflags="$(LDFLAGS)" -o $(TARGET)/$(BINARY)-linux-amd64 ./cmd/godis/ 2>&1
	GOOS=linux GOARCH=arm64 GOEXPERIMENT=$(GOEXPERIMENT) CGO_ENABLED=$(CGO_ENABLED) $(GO) build -tags $(TAGS) -ldflags="$(LDFLAGS)" -o $(TARGET)/$(BINARY)-linux-arm64 ./cmd/godis/
	GOOS=linux GOARCH=riscv64 GOEXPERIMENT=$(GOEXPERIMENT) CGO_ENABLED=$(CGO_ENABLED) $(GO) build -tags $(TAGS) -ldflags="$(LDFLAGS)" -o $(TARGET)/$(BINARY)-linux-riscv64 ./cmd/godis/
	GOOS=darwin GOARCH=amd64 GOAMD64=v3 GOEXPERIMENT=$(GOEXPERIMENT) CGO_ENABLED=$(CGO_ENABLED) $(GO) build -tags $(TAGS) -ldflags="$(LDFLAGS)" -o $(TARGET)/$(BINARY)-darwin-amd64 ./cmd/godis/
	GOOS=darwin GOARCH=arm64 GOEXPERIMENT=$(GOEXPERIMENT) CGO_ENABLED=$(CGO_ENABLED) $(GO) build -tags $(TAGS) -ldflags="$(LDFLAGS)" -o $(TARGET)/$(BINARY)-darwin-arm64 ./cmd/godis/
	GOOS=windows GOARCH=amd64 GOAMD64=v3 GOEXPERIMENT=$(GOEXPERIMENT) CGO_ENABLED=$(CGO_ENABLED) $(GO) build -tags $(TAGS) -ldflags="$(LDFLAGS)" -o $(TARGET)/$(BINARY)-windows-amd64.exe ./cmd/godis/
	GOOS=windows GOARCH=arm64 GOEXPERIMENT=$(GOEXPERIMENT) CGO_ENABLED=$(CGO_ENABLED) $(GO) build -tags $(TAGS) -ldflags="$(LDFLAGS)" -o $(TARGET)/$(BINARY)-windows-arm64.exe ./cmd/godis/

test:
	GOEXPERIMENT=$(GOEXPERIMENT) $(GO) test ./... 2>&1

run:
	GOEXPERIMENT=$(GOEXPERIMENT) CGO_ENABLED=$(CGO_ENABLED) \
		$(GO) run -tags $(TAGS) ./cmd/godis/

clean:
	rm -rf $(TARGET)
