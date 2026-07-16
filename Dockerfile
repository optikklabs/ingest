FROM golang:1.25-alpine AS builder
WORKDIR /app

# Copy module manifests first for layer caching.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux \
    go build -ldflags="-s -w" -o ingest ./cmd/ingest

FROM alpine:3.20
WORKDIR /app

COPY --from=builder /app/ingest .
COPY config.yml .

RUN chown -R 1000:1000 /app
USER 1000:1000

# Match default config.yml: health/metrics plus OTLP gRPC and HTTP.
EXPOSE 18090 18317 18318
ENTRYPOINT ["./ingest"]
