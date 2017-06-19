
LDFLAGS += -X "github.com/dantin/syncer/utils.Version=0.0.1~git.$(shell git rev-parse --short HEAD)"
LDFLAGS += -X "github.com/dantin/syncer/utils.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "github.com/dantin/syncer/utils.GitHash=$(shell git rev-parse HEAD)"

CURDIR := $(shell pwd)
GO := go

.PHONY: build

build: syncer

syncer:
	$(GO) build -ldflags '$(LDFLAGS)' -o bin/syncer
