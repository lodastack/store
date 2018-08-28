all: test

fmt:
	gofmt -l -w -s */

dep:fmt
	go mod download

test:dep
	go test -v ./...
