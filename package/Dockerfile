# syntax=docker/dockerfile:1.27.0@sha256:bde3983e9c939224420ddaf6b784cc30e09b035a4dea01f581230c50809f372e
FROM registry.suse.com/bci/golang:1.27@sha256:bbc484a1bc1a3c9f2417adf60d3e284c246f2c61b7bbb1f7a3e523a3c4a3d90d AS builder

ARG LONGHORN_TWO_MINOR_UPGRADE_DISTROS
ENV LONGHORN_TWO_MINOR_UPGRADE_DISTROS=${LONGHORN_TWO_MINOR_UPGRADE_DISTROS}

WORKDIR /app

# Copy the build script and source code
COPY . /app

# Make the build script executable
RUN chmod +x /app/scripts/build

# Run the build script
RUN /app/scripts/build

FROM registry.suse.com/bci/bci-base:15.7@sha256:4e723af56f41107c2690afaed283ecd702d2b3ee5da673c94e775b1d038728fe AS release

ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" != "linux/amd64" ] && [ "$TARGETPLATFORM" != "linux/arm64" ]; then \
    echo "Error: Unsupported TARGETPLATFORM: $TARGETPLATFORM" && \
    exit 1; \
    fi

ENV ARCH=${TARGETPLATFORM#linux/}

RUN zypper -n ref && \
    zypper update -y

RUN zypper -n install \
    iputils \
    iproute2 \
    nfs-client \
    cifs-utils \
    bind-utils \
    e2fsprogs \
    xfsprogs \
    zip \
    unzip \
    kmod \
    smartmontools \
    && zypper clean --all

# nsmounter needs these utilities.
RUN for cmd in xargs grep tail; do \
    command -v "$cmd" >/dev/null || { echo "Error: $cmd is required by nsmounter"; exit 1; }; \
    done

COPY --from=builder /app/bin/longhorn-manager-${ARCH} /usr/local/sbin/longhorn-manager

COPY --from=builder /app/package/launch-manager /app/package/nsmounter /usr/local/sbin/

EXPOSE 9500
CMD ["launch-manager"]
