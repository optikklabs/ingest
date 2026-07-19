FROM alpine:3.20
WORKDIR /app

COPY ingest .
COPY config.yml .

RUN chown -R 1000:1000 /app
USER 1000:1000

# Match default config.yml: health/metrics plus OTLP gRPC and HTTP.
EXPOSE 18090 18317 18318
ENTRYPOINT ["./ingest"]

