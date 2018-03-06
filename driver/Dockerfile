FROM ubuntu:16.04

RUN apt-get update && apt-get install -y dnsutils iputils-ping telnet

ADD longhorn /
ADD entrypoint.sh /
ADD jq /
ADD checkdependency.sh /

CMD ["/entrypoint.sh"]
