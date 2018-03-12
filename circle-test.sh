#!/bin/bash
set -x
set -e

go version


# install gdm
go get github.com/sparrc/gdm && cd /go/src/github.com/sparrc/gdm && go build -v && sudo cp gdm /usr/local/bin/ 
cd /go/src/github.com/lodastack/store && gdm restore

go test -timeout 60s -v ./...

cd ./cmd/store

go build -v