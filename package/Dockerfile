FROM registry.suse.com/bci/bci-base:15.5

RUN zypper -n ref && \
    zypper update -y

RUN zypper -n install curl nfs-client iproute2 bind-utils iputils telnet \
              zip unzip e2fsprogs e2fsprogs-devel xfsprogs xfsprogs-devel cifs-utils && \
    rm -rf /var/cache/zypp/*

COPY bin package/launch-manager package/nsmounter /usr/local/sbin/

VOLUME /usr/local/sbin
EXPOSE 9500
CMD ["launch-manager"]
