FROM registry.suse.com/bci/bci-base:15.6

RUN zypper -n install iputils iproute2 nfs-client cifs-utils bind-utils e2fsprogs xfsprogs zip unzip && \
    rm -rf /var/cache/zypp/*

COPY bin package/launch-manager package/nsmounter /usr/local/sbin/

VOLUME /usr/local/sbin
EXPOSE 9500
CMD ["launch-manager"]
