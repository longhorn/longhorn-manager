# syntax=docker/dockerfile:1.17.1@sha256:38387523653efa0039f8e1c89bb74a30504e76ee9f565e25c9a09841f9427b05
FROM registry.suse.com/bci/golang:1.25@sha256:3190609b31bbdbd4396ecba6c17ef71ccd282ec6973dba7c106b391b3edb1807 AS builder

WORKDIR /app

# Copy the build script and source code
COPY . /app

# Make the build script executable
RUN chmod +x /app/scripts/build

# Run the build script
RUN /app/scripts/build

FROM registry.suse.com/bci/bci-base:15.7@sha256:3292c81fb9e40b60903e6c88fac34e955b6d5b3acd3eb055d02d5c1538a72aea AS release

ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" != "linux/amd64" ] && [ "$TARGETPLATFORM" != "linux/arm64" ]; then \
    echo "Error: Unsupported TARGETPLATFORM: $TARGETPLATFORM" && \
    exit 1; \
    fi

ENV ARCH ${TARGETPLATFORM#linux/}

RUN zypper -n ref && \
    zypper update -y

RUN zypper -n install iputils iproute2 nfs-client cifs-utils bind-utils e2fsprogs xfsprogs zip unzip kmod && \
    rm -rf /var/cache/zypp/*

COPY --from=builder /app/bin/longhorn-manager-${ARCH} /usr/local/sbin/longhorn-manager

COPY --from=builder /app/package/launch-manager /app/package/nsmounter /usr/local/sbin/

EXPOSE 9500
CMD ["launch-manager"]
