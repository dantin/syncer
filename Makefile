
CURDIR := $(shell pwd)
GO := go

.PHONY: build

build: syncer

syncer:
	$(GO) build -o bin/syncer