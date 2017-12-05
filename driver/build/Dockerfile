FROM ubuntu:16.04

RUN apt-get update && apt-get install -y dnsutils

ADD longhorn /
ADD entrypoint.sh /

CMD ["/entrypoint.sh"]
