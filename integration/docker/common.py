import os
import string
import time
import subprocess

import pytest

import cattle

ENV_MANAGER_IPS = "LONGHORN_MANAGER_TEST_SERVER_IPS"
ENV_BACKUPSTORE_URL = "LONGHORN_MANAGER_TEST_BACKUPSTORE_URL"

MANAGER = 'http://localhost:9500'

SIZE = str(16 * 1024 * 1024)
VOLUME_NAME = "longhorn-manager-test_vol-1.0"
VOLUME_RESTORE_NAME = "longhorn-manager-test_vol-1.0-restore"
DEV_PATH = "/dev/longhorn/"

PORT = ":9500"

RETRY_COUNTS = 100
RETRY_ITERVAL = 0.1


@pytest.fixture
def clients(request):
    ips = get_mgr_ips()
    client = get_client(ips[0] + PORT)
    hosts = client.list_host()
    assert len(hosts) == len(ips)
    clis = get_clients(hosts)
    request.addfinalizer(lambda: cleanup_clients(clis))
    cleanup_clients(clis)
    return clis


def cleanup_clients(clis):
    client = clis.itervalues().next()
    volumes = client.list_volume()
    for v in volumes:
        client.delete(v)


def get_client(address):
    url = 'http://' + address + '/v1/schemas'
    c = cattle.from_env(url=url)
    return c


def get_mgr_ips():
    return string.split(os.environ[ENV_MANAGER_IPS], ",")


def get_backupstore_url():
    return os.environ[ENV_BACKUPSTORE_URL]


def get_clients(hosts):
    clients = {}
    for host in hosts:
        assert host["uuid"] is not None
        assert host["address"] is not None
        clients[host["uuid"]] = get_client(host["address"] + PORT)
    return clients


def wait_for_volume_state(client, name, state):
    for i in range(RETRY_COUNTS):
        volume = client.by_id_volume(name)
        if volume["state"] == state:
            break
        time.sleep(RETRY_ITERVAL)
    assert volume["state"] == state
    return volume


def wait_for_volume_delete(client, name):
    for i in range(RETRY_COUNTS):
        volumes = client.list_volume()
        found = False
        for volume in volumes:
            if volume["name"] == name:
                found = True
        if not found:
            break
        time.sleep(RETRY_ITERVAL)
    assert not found


def wait_for_snapshot_purge(volume, *snaps):
    for i in range(RETRY_COUNTS):
        snapshots = volume.snapshotList(volume=VOLUME_NAME)
        snapMap = {}
        for snap in snapshots:
            snapMap[snap["name"]] = snap
        found = False
        for snap in snaps:
            if snap in snapMap:
                found = True
                break
        if not found:
            break
        time.sleep(RETRY_ITERVAL)
    assert not found


def docker_stop(*containers):
    cmd = ["docker", "stop"]
    for c in containers:
        cmd.append(c)
    return subprocess.check_call(cmd)
