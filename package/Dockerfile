FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get dist-upgrade -y && \
    apt-get install -y curl vim nfs-common iproute2 dnsutils iputils-ping telnet zip xfsprogs && \
    rm -rf /var/lib/apt/lists/*

COPY bin package/launch-manager /usr/local/sbin/

VOLUME /usr/local/sbin
EXPOSE 9500
CMD ["launch-manager"]
