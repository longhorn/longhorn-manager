FROM ubuntu:18.04

RUN apt-get update && apt-get install -y curl vim nfs-common iproute2 dnsutils iputils-ping telnet zip

COPY bin package/launch-manager /usr/local/sbin/
COPY driver /

VOLUME /usr/local/sbin
EXPOSE 9500
CMD ["launch-manager"]
