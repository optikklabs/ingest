.PHONY: build run fmt vet test proto

# Regenerates the Kafka wire schemas. The previous `go generate ./...` was a
# no-op: no //go:generate directive has ever existed in this repo.
PROTO_FILES := $(shell find internal -name '*.proto')

proto:
	PATH="$(shell go env GOPATH)/bin:$(PATH)" \
		protoc --go_out=. --go_opt=paths=source_relative $(PROTO_FILES)
	gofmt -w $(patsubst %.proto,%.pb.go,$(PROTO_FILES))

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
