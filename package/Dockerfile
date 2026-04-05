# syntax=docker/dockerfile:1.20.0@sha256:26147acbda4f14c5add9946e2fd2ed543fc402884fd75146bd342a7f6271dc1d
FROM registry.suse.com/bci/golang:1.25@sha256:71eb104cb6375c29d2c552f1470fafb040d2ab2b68324c00076b0255ecdd0989 AS builder

WORKDIR /app

# Copy the build script and source code
COPY . /app

# Make the build script executable
RUN chmod +x /app/scripts/build

# Run the build script
RUN /app/scripts/build

FROM registry.suse.com/bci/bci-base:15.7@sha256:34a907223b206f02710285deefe08479cee19ec640b6a0958c3a548e76382e41 AS release

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
