# syntax=docker/dockerfile:1.7

ARG RUST_VERSION=1.85

FROM rust:${RUST_VERSION}-bookworm AS build
WORKDIR /src
RUN apt-get update \
    && apt-get install -y --no-install-recommends binutils lld \
    && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --locked --profile release-ship \
    --bin ultrasqld \
    --bin ultrasql \
    --bin ultrasql-local

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tini \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 ultrasql \
    && useradd --system --uid 10001 --gid ultrasql \
      --home-dir /var/lib/ultrasql --shell /usr/sbin/nologin ultrasql \
    && mkdir -p /var/lib/ultrasql \
    && chown -R ultrasql:ultrasql /var/lib/ultrasql

COPY --from=build /src/target/release-ship/ultrasqld /usr/local/bin/ultrasqld
COPY --from=build /src/target/release-ship/ultrasql /usr/local/bin/ultrasql
COPY --from=build /src/target/release-ship/ultrasql-local /usr/local/bin/ultrasql-local

USER 10001:10001
EXPOSE 5432
VOLUME ["/var/lib/ultrasql"]

ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/ultrasqld"]
CMD ["--listen", "0.0.0.0:5432", "--data-dir", "/var/lib/ultrasql"]
