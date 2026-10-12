FROM rust:1.98-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock build.rs ./
COPY proto ./proto
COPY src ./src
RUN cargo build --release --locked

FROM bash:5.2 AS config-generator
COPY docker/generate-config.sh /usr/local/bin/generate-config
ENTRYPOINT ["bash", "/usr/local/bin/generate-config"]

FROM debian:bookworm-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends bash curl hostname ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --uid 10001 --create-home raft
COPY --from=builder /app/target/release/raft /usr/local/bin/raft
COPY docker/entrypoint.sh /usr/local/bin/entrypoint
USER raft
EXPOSE 50051 8080
ENTRYPOINT ["bash", "/usr/local/bin/entrypoint"]
CMD ["--config", "/config/cluster.yaml"]
