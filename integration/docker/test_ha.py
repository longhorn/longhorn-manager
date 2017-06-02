import common

from common import clients  # NOQA
from common import SIZE, VOLUME_NAME
from common import wait_for_volume_state, wait_for_volume_delete

def test_ha_simple_recovery(clients):  # NOQA
    # get a random client
    for host_id, client in clients.iteritems():
        break

    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)
    volume = wait_for_volume_state(client, VOLUME_NAME, "detached")
    assert volume["name"] == VOLUME_NAME
    assert volume["size"] == SIZE
    assert volume["numberOfReplicas"] == 2
    assert volume["state"] == "detached"
    assert volume["created"] != ""

    volume = volume.attach(hostId=host_id)
    volume = wait_for_volume_state(client, VOLUME_NAME, "healthy")

    volume = client.by_id_volume(VOLUME_NAME)
#    assert volume["endpoint"] == DEV_PATH + VOLUME_NAME

    assert len(volume["replicas"]) == 2
    replica0 = volume["replicas"][0]
    assert replica0["name"] != ""

    replica1 = volume["replicas"][1]
    assert replica1["name"] != ""

    volume = volume.replicaRemove(name=replica0["name"])
    assert len(volume["replicas"]) == 1
    volume = wait_for_volume_state(client, VOLUME_NAME, "degraded")

    volume = wait_for_volume_state(client, VOLUME_NAME, "healthy")

    volume = client.by_id_volume(VOLUME_NAME)
    assert volume["state"] == "healthy"
    assert len(volume["replicas"]) == 2

    new_replica0 = volume["replicas"][0]
    new_replica1 = volume["replicas"][1]

    assert (replica1["name"] == new_replica0["name"] or
            replica1["name"] == new_replica1["name"])

    volume = volume.detach()
    volume = wait_for_volume_state(client, VOLUME_NAME, "detached")

    client.delete(volume)
    wait_for_volume_delete(client, VOLUME_NAME)

    volumes = client.list_volume()
    assert len(volumes) == 0


def test_ha_salvage(clients):  # NOQA
    # get a random client
    for host_id, client in clients.iteritems():
        break

    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)
    volume = wait_for_volume_state(client, VOLUME_NAME, "detached")
    assert volume["name"] == VOLUME_NAME
    assert volume["size"] == SIZE
    assert volume["numberOfReplicas"] == 2
    assert volume["state"] == "detached"
    assert volume["created"] != ""

    volume = volume.attach(hostId=host_id)
    volume = wait_for_volume_state(client, VOLUME_NAME, "healthy")

    assert len(volume["replicas"]) == 2
    replica0_name = volume["replicas"][0]["name"]
    replica1_name = volume["replicas"][1]["name"]
    common.docker_stop(replica0_name, replica1_name)

    volume = wait_for_volume_state(client, VOLUME_NAME, "fault")
    assert len(volume["replicas"]) == 2
    assert volume["replicas"][0]["badTimestamp"] != ""
    assert volume["replicas"][1]["badTimestamp"] != ""

    volume.salvage(names=[replica0_name, replica1_name])

    volume = wait_for_volume_state(client, VOLUME_NAME, "detached")
    assert len(volume["replicas"]) == 2
    assert volume["replicas"][0]["badTimestamp"] == ""
    assert volume["replicas"][1]["badTimestamp"] == ""

    volume = volume.attach(hostId=host_id)
    volume = wait_for_volume_state(client, VOLUME_NAME, "healthy")

    volume = volume.detach()
    volume = wait_for_volume_state(client, VOLUME_NAME, "detached")

    client.delete(volume)
    wait_for_volume_delete(client, VOLUME_NAME)

    volumes = client.list_volume()
    assert len(volumes) == 0
