# syntax=docker/dockerfile:1.7

FROM elixir:1.20-otp-29-slim AS builder

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    apt-get install -y --no-install-recommends build-essential ca-certificates cmake git

WORKDIR /app
ENV MIX_ENV=prod

RUN mix local.hex --force && mix local.rebar --force

# deps/ and _build/ live in image layers rather than cache mounts: ARC runners
# start every job with an empty BuildKit, so a cache mount is always cold in CI
# and only the registry layer cache can carry compiled dependencies forward.
# config.exs is compile-time config, so it lands before deps.compile;
# runtime.exs is read at boot and follows with the application source.
COPY mix.exs mix.lock ./
COPY config/config.exs config/
RUN --mount=type=cache,target=/root/.hex/packages \
    mix deps.get --only prod && \
    mix deps.compile

COPY config/runtime.exs config/
COPY lib/ lib/
RUN mix compile && \
    mix release --path /app/release --overwrite && \
    find /app/release -type f \( -name '*.so' -o -name 'beam.smp' \) \
        -exec strip --strip-unneeded '{}' +

FROM debian:trixie-slim AS runtime-tools

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    apt-get install -y --no-install-recommends busybox-static libsctp1 libtinfo6 tini && \
    install -D -m 0755 /bin/busybox /out/bin/busybox && \
    install -D -m 0755 /usr/bin/tini /out/bin/tini && \
    ln -s /usr/bin/busybox /out/sh && \
    install -D -m 0644 /usr/lib/*-linux-gnu/libsctp.so.1 /out/lib/libsctp.so.1 && \
    install -D -m 0644 /usr/lib/*-linux-gnu/libtinfo.so.6 /out/lib/libtinfo.so.6

FROM gcr.io/distroless/cc-debian13:nonroot AS runtime

WORKDIR /app
ENV HOME=/home/nonroot \
    LANG=C.UTF-8 \
    LD_LIBRARY_PATH=/usr/lib/prism \
    ELIXIR_ERL_OPTIONS=+fnu

COPY --from=runtime-tools /out/bin/ /usr/bin/
COPY --from=runtime-tools /out/sh /bin/sh
COPY --from=runtime-tools /out/lib/ /usr/lib/prism/
COPY --from=builder --chown=65532:65532 /app/release/ ./

USER 65532:65532

EXPOSE 9090
STOPSIGNAL SIGTERM

HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=6 \
    CMD ["/usr/bin/busybox", "wget", "-q", "-T", "2", "-O", "/dev/null", "http://127.0.0.1:9090/ready"]

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["bin/prism", "start"]
