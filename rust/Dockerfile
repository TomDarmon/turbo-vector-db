# syntax=docker/dockerfile:1.7
FROM rust:1.91.0-bookworm AS builder
WORKDIR /app

COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/api/Cargo.toml ./crates/api/Cargo.toml
COPY crates/core/Cargo.toml ./crates/core/Cargo.toml
COPY crates/manifest/Cargo.toml ./crates/manifest/Cargo.toml
COPY crates/queue/Cargo.toml ./crates/queue/Cargo.toml
COPY crates/storage/Cargo.toml ./crates/storage/Cargo.toml

RUN mkdir -p crates/api/src crates/core/src crates/manifest/src crates/queue/src crates/storage/src \
	&& printf 'fn main() {}\n' > crates/api/src/main.rs \
	&& printf '// placeholder for dependency cache layer\n' > crates/core/src/lib.rs \
	&& printf '// placeholder for dependency cache layer\n' > crates/manifest/src/lib.rs \
	&& printf '// placeholder for dependency cache layer\n' > crates/queue/src/lib.rs \
	&& printf '// placeholder for dependency cache layer\n' > crates/storage/src/lib.rs

RUN --mount=type=cache,id=turbo-vector-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
	--mount=type=cache,id=turbo-vector-cargo-git,target=/usr/local/cargo/git,sharing=locked \
	cargo fetch --locked

COPY crates ./crates

RUN --mount=type=cache,id=turbo-vector-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
	--mount=type=cache,id=turbo-vector-cargo-git,target=/usr/local/cargo/git,sharing=locked \
	--mount=type=cache,id=turbo-vector-target,target=/build-target,sharing=locked \
	CARGO_TARGET_DIR=/build-target cargo build -p turbo-vector-api --release --locked \
	&& cp /build-target/release/turbo-vector-api /app/turbo-vector-api

FROM debian:bookworm-slim
WORKDIR /srv
RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates \
	&& rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/turbo-vector-api /usr/local/bin/turbo-vector-api
EXPOSE 8080
ENTRYPOINT ["turbo-vector-api"]
