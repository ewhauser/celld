# syntax=docker/dockerfile:1.7
FROM rust:1.97.1-bookworm AS build
WORKDIR /build
COPY runner/Cargo.toml runner/Cargo.lock ./
COPY runner/src ./src
COPY runner/ipc ./ipc
COPY runner/vendor ./vendor
RUN --mount=type=cache,id=celld-wasmer-registry,target=/usr/local/cargo/registry \
    --mount=type=cache,id=celld-wasmer-build,target=/build/target \
    cargo build --release --locked && cp target/release/celld-wasmer-runner /usr/local/bin/

FROM node:24-bookworm-slim
RUN groupadd --gid 10001 sandbox && useradd --uid 10001 --gid sandbox --no-create-home sandbox
WORKDIR /app
COPY --from=build /usr/local/bin/celld-wasmer-runner /usr/local/bin/
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force
COPY service ./service
COPY src ./src
USER 10001:10001
EXPOSE 19877
CMD ["node", "service/main.mjs"]
