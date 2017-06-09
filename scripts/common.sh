#!/bin/bash

TEST_PREFIX=longhorn-manager-test

ETCD_SERVER=${TEST_PREFIX}-etcd-server
ETCD_IMAGE=quay.io/coreos/etcd:v3.1.5

NFS_SERVER=${TEST_PREFIX}-nfs-server
NFS_IMAGE=docker.io/erezhorev/dockerized_nfs_server

LONGHORN_ENGINE_IMAGE=rancher/longhorn-engine:17e33fc

LONGHORN_ENGINE_BINARY_NAME=${TEST_PREFIX}-engine-binary
LONGHORN_MANAGER_NAME=${TEST_PREFIX}-manager

BACKUPSTORE_PATH=/opt/backupstore

function check_exists {
    name=$1
    if [ "$(docker ps -aq -f status=exited -f name=${name})" ];
    then
	docker rm -v ${name}
    fi
    if [ "$(docker ps -q -f name=${name})" ]; then
        echo true
        return
    fi
    echo false
}

function start_etcd {
    exists=$(check_exists $ETCD_SERVER)

    if [ "$exists" == "true" ]; then
        echo etcd server exists
        return
    fi

    echo Start etcd server
    docker run -d \
                --name $ETCD_SERVER \
                --volume /etcd-data \
                $ETCD_IMAGE \
                /usr/local/bin/etcd \
                --name longhorn-test-etcd-1 \
                --data-dir /etcd-data \
                --listen-client-urls http://0.0.0.0:2379 \
                --advertise-client-urls http://0.0.0.0:2379 \
                --listen-peer-urls http://0.0.0.0:2380 \
                --initial-advertise-peer-urls http://0.0.0.0:2380 \
                --initial-cluster longhorn-test-etcd-1=http://0.0.0.0:2380 \
                --initial-cluster-token my-etcd-token \
                --initial-cluster-state new \
                --auto-compaction-retention 1

    echo etcd server is up
}

function cleanup_mgr_test {
    echo clean up test containers
    set +e
    docker stop $(docker ps -f name=$TEST_PREFIX -a -q)
    docker rm -v $(docker ps -f name=$TEST_PREFIX -a -q)
    set -e
}

function wait_for {
    url=$1

    ready=false

    set +e
    for i in `seq 1 5`
    do
            sleep 1
            curl -sL --max-time 1 --fail --output /dev/null --silent $url
            if [ $? -eq 0 ]
            then
                    ready=true
                    break
            fi
    done
    set -e

    if [ "$ready" != true ]
    then
            echo Fail to wait for $url
            return -1
    fi
    return 0
}

function wait_for_etcd {
    etcd_ip=$1
    wait_for http://${etcd_ip}:2379/v2/stats/leader
}

function get_container_ip {
    container=$1
    for i in `seq 1 5`
    do
        ip=`docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' $container`
        if [ "$ip" != "" ]
        then
            break
        fi
        sleep 10
    done

    if [ "$ip" == "" ]
    then
        echo cannot find ip for $container
        exit -1
    fi
    echo $ip
}

function start_engine_binary {
    name=${LONGHORN_ENGINE_BINARY_NAME}
    exists=$(check_exists $name)

    if [ "$exists" == "true" ]; then
        echo longhorn engine binary exists
        return
    fi

    docker run --name $name ${LONGHORN_ENGINE_IMAGE} bash
    echo longhorn engine binary is ready
}

function start_mgr {
    image=$1
    name=$2
    etcd_ip=$3
    shift 3
    extra=$@

    exists=$(check_exists $name)
    if [ "$exists" == "true" ]; then
        echo remove old longhorn-manager server
	docker stop ${name}
	docker rm -v ${name}
    fi

    docker run -d --name ${name} \
            --privileged -v /dev:/host/dev \
            -v /var/run:/var/run ${extra} \
            --volumes-from ${LONGHORN_ENGINE_BINARY_NAME} ${image} \
            /usr/local/sbin/launch-manager -d --orchestrator docker \
            --engine-image ${LONGHORN_ENGINE_IMAGE} \
            --etcd-servers http://${etcd_ip}:2379
    echo ${name} is up
}

function wait_for_mgr {
    mgr_ip=$1
    wait_for http://${mgr_ip}:9500/v1
}

function start_nfs {
    name=${NFS_SERVER}
    exists=$(check_exists $name)

    if [ "$exists" == "true" ]; then
        echo nfs server exists
        return
    fi

    docker run -d --name ${NFS_SERVER} --privileged ${NFS_IMAGE} ${BACKUPSTORE_PATH}
    echo nfs server is up
}
