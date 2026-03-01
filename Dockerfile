FROM rust:1-bookworm AS builder

WORKDIR /app

COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches

RUN cargo build --release

FROM debian:bookworm-slim

RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates; \
    rm -rf /var/lib/apt/lists/*; \
    useradd --system --create-home --uid 10001 kvns

COPY --from=builder /app/target/release/kvns /usr/local/bin/kvns

RUN mkdir -p /data && chown -R kvns:kvns /data

USER kvns

ENV KVNS_HOST=0.0.0.0 \
    KVNS_PORT=6480 \
    KVNS_METRICS_HOST=0.0.0.0 \
    KVNS_METRICS_PORT=9090 \
    KVNS_PERSIST_PATH=/data/kvns.db

EXPOSE 6480 9090

VOLUME ["/data"]

ENTRYPOINT ["kvns"]
