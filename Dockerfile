# syntax=docker/dockerfile:1.7

ARG RUST_IMAGE=rust:1.97.1-bookworm
ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian12:nonroot

FROM ${RUST_IMAGE} AS builder

WORKDIR /workspace
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        binutils \
        build-essential \
        ca-certificates \
        clang \
        cmake \
        git \
        libssl-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/workspace/target \
    cargo build --locked --release --package server --bin server \
    && cp target/release/server /tmp/helix-server \
    && readelf -l /tmp/helix-server \
        | grep -Eq '/(lib/ld-linux-aarch64\.so\.1|lib64/ld-linux-x86-64\.so\.2)' \
    && ! readelf -l /tmp/helix-server | grep -qi musl \
    && install -d -m 1777 /tmp/runtime-root/tmp \
    && install -d -o 65532 -g 65532 -m 0755 \
        /tmp/runtime-root/var/lib/helix \
        /tmp/runtime-root/var/cache/helix

FROM ${RUNTIME_IMAGE}

COPY --from=builder /tmp/runtime-root/ /
COPY --from=builder /tmp/helix-server /bin/helix-server

ENV SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt \
    HELIX_HTTP_ADDR=0.0.0.0:8080 \
    HELIX_GRPC_ADDR=127.0.0.1:8081 \
    DB_PATH=db/ \
    RUST_LOG=server=info,db=info,slatedb=info

LABEL org.opencontainers.image.title="helixdb" \
    org.opencontainers.image.description="HelixDB single-process database server." \
    org.opencontainers.image.licenses="Apache-2.0" \
    org.opencontainers.image.source="https://github.com/HelixDB/helix-db" \
    org.opencontainers.image.base.name="gcr.io/distroless/cc-debian12:nonroot"

WORKDIR /home/nonroot
USER 65532:65532
EXPOSE 8080
STOPSIGNAL SIGTERM
ENTRYPOINT ["/bin/helix-server"]
