# syntax=docker/dockerfile:1.23.0@sha256:2780b5c3bab67f1f76c781860de469442999ed1a0d7992a5efdf2cffc0e3d769
FROM registry.suse.com/bci/golang:1.26@sha256:ffe330184fb07e2e2c089b73229eaaec7085ce802f965b80c84780f163a5f062 AS builder

ARG LONGHORN_TWO_MINOR_UPGRADE_DISTROS
ENV LONGHORN_TWO_MINOR_UPGRADE_DISTROS=${LONGHORN_TWO_MINOR_UPGRADE_DISTROS}

WORKDIR /app

# Copy the build script and source code
COPY . /app

# Make the build script executable
RUN chmod +x /app/scripts/build

# Run the build script
RUN /app/scripts/build

FROM registry.suse.com/bci/bci-base:15.7@sha256:dc0cdb596a8a4c04edd140a12351f5d8d24ab1818b22a898caf598aaa0e89ee9 AS release

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

COPY --from=builder /app/bin/longhorn-manager-${ARCH} /usr/local/sbin/longhorn-manager

COPY --from=builder /app/package/launch-manager /app/package/nsmounter /usr/local/sbin/

EXPOSE 9500
CMD ["launch-manager"]
