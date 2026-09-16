# syntax=docker/dockerfile:1.26.0@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
FROM registry.suse.com/bci/golang:1.26@sha256:f6f04957fbc16399c76b1e8266cc7d0f3b4a5434b824fb251d7e3ae559d6a910 AS builder

ARG LONGHORN_TWO_MINOR_UPGRADE_DISTROS
ENV LONGHORN_TWO_MINOR_UPGRADE_DISTROS=${LONGHORN_TWO_MINOR_UPGRADE_DISTROS}

WORKDIR /app

# Copy the build script and source code
COPY . /app

# Make the build script executable
RUN chmod +x /app/scripts/build

# Run the build script
RUN /app/scripts/build

FROM registry.suse.com/bci/bci-base:15.7@sha256:4a0d40a2c4b0b9c1e58fd270e4030d4defb385ffbfcf70f25493283193042d4d AS release

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
