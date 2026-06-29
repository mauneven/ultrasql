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
    && apt-get install -y --no-install-recommends ca-certificates tini curl \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 ultrasql \
    && useradd --system --uid 10001 --gid ultrasql \
      --home-dir /var/lib/ultrasql --shell /usr/sbin/nologin ultrasql \
    && mkdir -p /var/lib/ultrasql \
    && chown -R ultrasql:ultrasql /var/lib/ultrasql

COPY --from=build /src/target/release-ship/ultrasqld /usr/local/bin/ultrasqld
COPY --from=build /src/target/release-ship/ultrasql /usr/local/bin/ultrasql
COPY --from=build /src/target/release-ship/ultrasql-local /usr/local/bin/ultrasql-local
COPY packaging/docker/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
RUN chmod 0755 /usr/local/bin/docker-entrypoint.sh

USER 10001:10001
EXPOSE 5432
VOLUME ["/var/lib/ultrasql"]

# Secure-by-default: the entrypoint requires ULTRASQL_PASSWORD (SCRAM) or an
# explicit ULTRASQL_HOST_AUTH_METHOD=trust opt-in before it will start a public
# listener; otherwise it refuses with an actionable message. The server itself
# enforces the same rule, so the container can never boot open by accident.
ENTRYPOINT ["/usr/bin/tini", "--", "/usr/local/bin/docker-entrypoint.sh"]
CMD ["ultrasqld"]

# Liveness/readiness via the built-in ops endpoint (loopback by default; see the
# entrypoint). /ready confirms the wire listener is accepting connections.
HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD curl -fsS "http://${ULTRASQL_OPS_LISTEN:-127.0.0.1:9100}/ready" >/dev/null || exit 1
