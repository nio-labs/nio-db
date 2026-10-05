# Multi-stage Dockerfile for NioDB
# Build stage
FROM rust:1-bookworm AS builder
WORKDIR /usr/src/niodb

# Pre-cache dependencies
COPY Cargo.toml Cargo.lock ./
RUN mkdir -p src && echo "fn main() {}" > src/main.rs && touch src/lib.rs
RUN cargo build --release
RUN rm -rf src

# Build actual binary
COPY src ./src
COPY runtime ./runtime
RUN touch src/main.rs src/lib.rs
RUN cargo build --release

# Runtime stage
FROM node:22-bookworm-slim AS runtime
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install production dependencies for AlaSql runtime
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# Copy runtime helpers and launcher
COPY runtime ./runtime
COPY bin ./bin

# Copy compiled Rust binary
COPY --from=builder /usr/src/niodb/target/release/niodb /usr/local/bin/niodb

# Setup entrypoint
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod +x /usr/local/bin/docker-entrypoint.sh

ENV PORT=7432
ENV NIODB_DATA=/data
ENV NODE_ENV=production
ENV PATH="/usr/local/bin:$PATH"

EXPOSE 7432
VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
