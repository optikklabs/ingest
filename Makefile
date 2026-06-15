.PHONY: build run fmt vet test proto

proto:
	PATH="$(shell go env GOPATH)/bin:$(PATH)" go generate ./...

build:
	go build -v ./cmd/ingest

run:
	go run ./cmd/ingest

fmt:
	gofmt -w .

vet:
	go vet ./...

test:
	go test ./...
