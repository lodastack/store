#!/bin/bash

go version


# install gdm
go get github.com/sparrc/gdm && cd ~/.go_workspace/src/github.com/sparrc/gdm && go build -v && sudo cp gdm /usr/local/bin/ 
cd ${HOME}/.go_workspace/src/github.com/$CIRCLE_PROJECT_USERNAME/$CIRCLE_PROJECT_REPONAME && gdm restore

go test -timeout 60s -v ./...