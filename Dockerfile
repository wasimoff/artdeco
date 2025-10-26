FROM rust:alpine AS builder

WORKDIR /app
RUN apk update && apk add --no-cache \
    curl \
    unzip \
    musl-dev \
    perl \
    make

RUN PROTOC_VERSION=$(curl -s https://api.github.com/repos/protocolbuffers/protobuf/releases/latest | grep -o '"tag_name": "[^"]*"' | cut -d'"' -f4) \
    && curl -LO "https://github.com/protocolbuffers/protobuf/releases/download/${PROTOC_VERSION}/protoc-${PROTOC_VERSION#v}-linux-x86_64.zip" \
    && unzip "protoc-${PROTOC_VERSION#v}-linux-x86_64.zip" -d /usr/local \
    && rm "protoc-${PROTOC_VERSION#v}-linux-x86_64.zip"

COPY . .
RUN cargo build --release --package wasimoff-adaptor

FROM ghcr.io/wasimoff/tracebench:latest AS tracebench
# Runtime
FROM alpine AS artdeco

COPY --from=builder /app/target/release/wasimoff-adaptor /wasimoff-adaptor
COPY --from=tracebench /wasm /wasm
ENTRYPOINT ["/wasimoff-adaptor"]
