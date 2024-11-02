# syntax=docker/dockerfile:1.11.0

FROM registry.suse.com/bci/bci-base:15.6

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

COPY package/bin/longhorn-manager-${ARCH} /usr/local/sbin/longhorn-manager

COPY package/launch-manager package/nsmounter /usr/local/sbin/

EXPOSE 9500
CMD ["launch-manager"]
