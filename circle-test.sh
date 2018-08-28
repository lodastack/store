#!/bin/bash
set -x
set -e

go version
export GO111MODULE=on
go mod download
go mod verify
go test -timeout 60s -v ./...

cd ./cmd/store && go build -v -mod readonly