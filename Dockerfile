# Multi-stage build producing a `FROM scratch` image: a statically-linked
# musl binary with no runtime dependencies at all (no libc, no shell, no
# CA cert store needed -- TLS roots are compiled in via webpki-roots).
#
# Building directly on Alpine (musl libc) rather than cross-compiling from
# a glibc host: Alpine's default Rust toolchain target already *is*
# x86_64-unknown-linux-musl (or aarch64-unknown-linux-musl on arm64), so a
# plain `cargo build --release` here produces a static musl binary with no
# --target flag or cross-linker setup needed.
FROM rust:1-alpine AS builder

# build-base: C toolchain needed to compile bundled SQLite (sqlx's
# "sqlite" feature) and other build-time C/asm dependencies.
RUN apk add --no-cache build-base

WORKDIR /build
COPY . .
RUN cargo build --release --locked

FROM scratch
COPY --from=builder /build/target/release/kube-shim /kube-shim
ENTRYPOINT ["/kube-shim"]
CMD ["-c", "/data/config.toml"]
