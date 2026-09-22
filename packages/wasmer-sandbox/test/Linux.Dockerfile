FROM celld-wasmer-sandbox:qualification
USER root
RUN apt-get update && apt-get install -y --no-install-recommends procps && rm -rf /var/lib/apt/lists/*
COPY --from=celld-wasmer-runtime:qualification /out/celld /usr/local/bin/celld
RUN npm ci --ignore-scripts
COPY test ./test
RUN mkdir -p /app/test/artifacts && chown -R sandbox:sandbox /app/test/artifacts
USER sandbox
ENV CELLD_BIN=/usr/local/bin/celld CELLD_WASMER_RUNNER=/usr/local/bin/celld-wasmer-runner
CMD ["node", "test/integration.mjs"]
