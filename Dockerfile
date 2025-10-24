FROM rust:1.90.0 AS builder

WORKDIR /app
RUN apt-get update && apt-get install -y \
    curl \
    unzip \
    && rm -rf /var/lib/apt/lists/*

RUN PROTOC_VERSION=$(curl -s https://api.github.com/repos/protocolbuffers/protobuf/releases/latest | grep -oP '"tag_name": "\K(.*)(?=")') \
    && curl -LO "https://github.com/protocolbuffers/protobuf/releases/download/${PROTOC_VERSION}/protoc-${PROTOC_VERSION#v}-linux-x86_64.zip" \
    && unzip "protoc-${PROTOC_VERSION#v}-linux-x86_64.zip" -d /usr/local \
    && rm "protoc-${PROTOC_VERSION#v}-linux-x86_64.zip"

COPY . .
RUN cargo build --release --package wasimoff-adaptor

FROM ghcr.io/wasimoff/tracebench:latest AS tracebench
# Runtime
FROM debian:bookworm-slim AS artdeco

COPY --from=builder /app/target/release/wasimoff-adaptor /wasimoff-adaptor
COPY --from=tracebench /wasm /wasm
ENTRYPOINT ["/wasimoff-adaptor"]

