# ---------- Build Stage ----------
FROM golang:1.26-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=linux GOARCH=${TARGETARCH:-amd64} \
    go build -ldflags="-s -w" -o ingest ./cmd/ingest

# ---------- Runtime Stage ----------
FROM alpine:3.20

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /app/ingest .
COPY config.yml .

EXPOSE 18090 18317 18318

USER nobody

ENTRYPOINT ["./ingest"]


