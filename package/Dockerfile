FROM registry.suse.com/bci/bci-base:15.3

RUN zypper -n install curl nfs-client iproute2 bind-utils iputils telnet \
              zip e2fsprogs e2fsprogs-devel xfsprogs xfsprogs-devel && \
    rm -rf /var/cache/zypp/*

COPY bin package/launch-manager package/nsmounter /usr/local/sbin/

VOLUME /usr/local/sbin
EXPOSE 9500
CMD ["launch-manager"]
