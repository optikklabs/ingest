.PHONY: build run fmt vet proto

                                                                            
                                                                  
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
