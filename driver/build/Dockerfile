FROM ubuntu:14.04

RUN apt-get update &&  DEBIAN_FRONTEND=noninteractive apt-get install -q -y dnsutils \
&& rm -rf /var/lib/apt/lists/*

ADD longhorn /
ADD entrypoint.sh /

CMD ["/entrypiont.sh"]
