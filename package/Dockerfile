FROM ubuntu:16.04

RUN apt-get update && apt-get install -y curl vim nfs-common iproute dnsutils iputils-ping telnet zip

COPY bin package/launch-manager /usr/local/sbin/
COPY driver /

VOLUME /usr/local/sbin
EXPOSE 9500
CMD ["launch-manager"]
