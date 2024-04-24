# syntax=docker/dockerfile:1.7.0

FROM registry.suse.com/bci/bci-base:15.6

ARG TARGETPLATFORM
RUN if [ "$TARGETPLATFORM" != "linux/amd64" ] && [ "$TARGETPLATFORM" != "linux/arm64" ]; then \
    echo "Error: Unsupported TARGETPLATFORM: $TARGETPLATFORM" && \
    exit 1; \
    fi

ENV ARCH ${TARGETPLATFORM#linux/}

COPY package/bin/longhorn-manager-${ARCH} /usr/local/sbin/longhorn-manager

COPY package/launch-manager package/nsmounter /usr/local/sbin/

RUN zypper -n ref && \
    zypper update -y

RUN zypper -n install curl nfs-client iproute2 bind-utils iputils telnet \
    zip unzip e2fsprogs e2fsprogs-devel xfsprogs xfsprogs-devel cifs-utils && \
    rm -rf /var/cache/zypp/*

VOLUME /usr/local/sbin
EXPOSE 9500
CMD ["launch-manager"]
