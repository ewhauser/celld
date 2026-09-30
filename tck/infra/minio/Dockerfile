# Upstream's Quay/Docker Hub images are no longer publicly pullable.
# Keep the same release binaries, verified against their GitHub release checksums.
ARG TARGETARCH
FROM alpine:3.22.1@sha256:4bcff63911fcb4448bd4fdacec207030997caf25e9bea4045fa6c8c44de311d1 AS base

FROM base AS minio-amd64
ADD --checksum=sha256:7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f --chmod=755 https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio.linux-amd64.RELEASE.2025-09-07T16-13-09Z /usr/bin/minio

FROM base AS minio-arm64
ADD --checksum=sha256:5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d --chmod=755 https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio.linux-arm64.RELEASE.2025-09-07T16-13-09Z /usr/bin/minio

FROM minio-${TARGETARCH} AS minio
ENTRYPOINT ["/usr/bin/minio"]

FROM base AS mc-amd64
ADD --checksum=sha256:01f866e9c5f9b87c2b09116fa5d7c06695b106242d829a8bb32990c00312e891 --chmod=755 https://github.com/minio/mc/releases/download/RELEASE.2025-08-13T08-35-41Z/mc.linux-amd64.RELEASE.2025-08-13T08-35-41Z /usr/bin/mc

FROM base AS mc-arm64
ADD --checksum=sha256:14c8c9616cfce4636add161304353244e8de383b2e2752c0e9dad01d4c27c12c --chmod=755 https://github.com/minio/mc/releases/download/RELEASE.2025-08-13T08-35-41Z/mc.linux-arm64.RELEASE.2025-08-13T08-35-41Z /usr/bin/mc

FROM mc-${TARGETARCH} AS mc
ENTRYPOINT ["/usr/bin/mc"]
