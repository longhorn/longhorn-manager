FROM ubuntu:16.04

RUN apt-get update && apt-get install -y curl vim nfs-common iproute

COPY bin launch-manager /usr/local/sbin/
VOLUME /usr/local/sbin
EXPOSE 9500
CMD ["launch-manager"]
