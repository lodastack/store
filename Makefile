all: test

fmt:
	gofmt -l -w -s */

dep:fmt
	gdm restore

test:dep
	go test -v ./...
