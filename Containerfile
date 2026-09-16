# Multi-stage build producing a `FROM scratch` image: a statically-linked
# musl binary with no runtime dependencies at all (no libc, no shell, no
# CA cert store needed -- TLS roots are compiled in via webpki-roots).
#
# This file is used in CI to build release images, invoked from
# .github/workflows/release-build.yml. As a developer, you don't need to
# build production containers -- but here's how to test a change to this
# file:
#
#     podman build -t test-container \
#         --build-arg BUILD_TIMESTAMP=$(date -u +"%Y-%m-%dT%H:%M:%SZ") \
#         -f Containerfile .
#     podman run --rm test-container --help
#
# Building directly on Alpine (musl libc) rather than cross-compiling from
# a glibc host: Alpine's default Rust toolchain target already *is*
# x86_64-unknown-linux-musl (or aarch64-unknown-linux-musl on arm64), so a
# plain `cargo build --release` here produces a static musl binary with no
# --target flag or cross-linker setup needed -- this Containerfile is
# multi-arch as-is, with no arch-specific branches, since `--platform`
# picks the matching builder image and each arch just builds itself
# natively.

FROM rust:1-alpine AS builder

ARG BUILD_TIMESTAMP
ARG VCS_REF
ARG VCS_URL

# build-base: C toolchain needed to compile bundled SQLite (sqlx's
# "sqlite" feature) and other build-time C/asm dependencies.
RUN apk add --no-cache build-base

WORKDIR /build

# Explicit file list rather than `COPY . .`: an allowlist can't accidentally
# pick up a stray local file (target/, a gitignored config.toml/cert/
# db.sqlite someone happened to have on disk) the way a denylist
# (.dockerignore) could if it ever falls out of sync with what actually
# exists locally.
COPY Cargo.toml Cargo.lock .
COPY src src
COPY tests tests

RUN cargo build --release --locked
RUN cargo test --release --locked


FROM scratch

ARG BUILD_TIMESTAMP
ARG VCS_REF
ARG VCS_URL

COPY --from=builder --chown=1000:1000 /build/target/release/kube-shim /kube-shim

# Runs as a non-root UID inside the container too, on top of (not instead
# of) rootless podman on the host (see bootstrap/provision.sh) -- belt and
# suspenders, and free: the binary needs no special capabilities.
USER 1000

ENTRYPOINT ["/kube-shim"]
CMD ["-c", "/data/config.toml"]

LABEL \
    org.opencontainers.image.authors="Sascha Brawer <sascha@brawer.ch>" \
    org.opencontainers.image.created=$BUILD_TIMESTAMP \
    org.opencontainers.image.description="Kubernetes API shim for ephemeral container workloads" \
    org.opencontainers.image.revision=$VCS_REF \
    org.opencontainers.image.source=$VCS_URL
