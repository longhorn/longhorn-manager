FROM ubuntu:18.04

RUN apt-get update && apt-get install -y curl vim nfs-common iproute2 dnsutils iputils-ping telnet zip xfsprogs

COPY bin package/launch-manager /usr/local/sbin/

VOLUME /usr/local/sbin
EXPOSE 9500
CMD ["launch-manager"]
