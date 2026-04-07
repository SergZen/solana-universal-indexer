FROM rust:1.94-slim-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app 

COPY Cargo.toml Cargo.lock ./

RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    touch src/lib.rs && \
    cargo build --release 2>&1 | tail -5 &&  \
    rm -rf src

COPY src ./src
COPY migrations ./migrations

RUN touch src/main.rs && cargo build --release 2>&1 | tail -10

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3 \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/solana-indexer ./
COPY --from=builder /app/migrations ./migrations

EXPOSE 3000

CMD ["./solana-indexer"]
