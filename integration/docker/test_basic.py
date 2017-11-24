import time
import common

from common import clients  # NOQA
from common import SIZE, VOLUME_NAME, VOLUME_RESTORE_NAME
from common import wait_for_volume_state, wait_for_volume_delete
from common import wait_for_snapshot_purge


def test_hosts_and_settings(clients):  # NOQA
    hosts = clients.itervalues().next().list_host()
    for host in hosts:
        assert host["uuid"] is not None
        assert host["address"] is not None

    host0_id = hosts[0]["uuid"]
    host1_id = hosts[1]["uuid"]
    host2_id = hosts[2]["uuid"]
    host0_from0 = clients[host0_id].by_id_host(host0_id)
    host0_from1 = clients[host1_id].by_id_host(host0_id)
    host0_from2 = clients[host2_id].by_id_host(host0_id)
    assert host0_from0["uuid"] == \
        host0_from1["uuid"] == \
        host0_from2["uuid"]
    assert host0_from0["address"] == \
        host0_from1["address"] == \
        host0_from2["address"]

    client = clients[host0_id]

    setting_names = ["backupTarget"]
    settings = client.list_setting()
    assert len(settings) == len(setting_names)

    settingMap = {}
    for setting in settings:
        settingMap[setting["name"]] = setting

    for name in setting_names:
        assert settingMap[name] is not None

    for name in setting_names:
        setting = client.by_id_setting(name)
        assert settingMap[name]["value"] == setting["value"]

        old_value = setting["value"]

        setting = client.update(setting, value="testvalue")
        assert setting["value"] == "testvalue"
        setting = client.by_id_setting(name)
        assert setting["value"] == "testvalue"

        setting = client.update(setting, value=old_value)
        assert setting["value"] == old_value


def test_volume_basic(clients):  # NOQA
    # get a random client
    for host_id, client in clients.iteritems():
        break

    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)
    assert volume["name"] == VOLUME_NAME
    assert volume["size"] == SIZE
    assert volume["numberOfReplicas"] == 2

    volume = wait_for_volume_state(client, VOLUME_NAME, "detached")
    # soft anti-affinity should work, and we have 3 nodes
    assert len(volume["replicas"]) == 2
    hosts = {}
    for replica in volume["replicas"]:
        id = replica["hostId"]
        assert id != ""
        assert id not in hosts
        hosts[id] = True
    assert len(hosts) == 2

    assert volume["state"] == "detached"
    assert volume["created"] != ""

    volumes = client.list_volume()
    assert len(volumes) == 1
    assert volumes[0]["name"] == volume["name"]
    assert volumes[0]["size"] == volume["size"]
    assert volumes[0]["numberOfReplicas"] == volume["numberOfReplicas"]
    assert volumes[0]["state"] == volume["state"]
    assert volumes[0]["created"] == volume["created"]

    volumeByName = client.by_id_volume(VOLUME_NAME)
    assert volumeByName["name"] == volume["name"]
    assert volumeByName["size"] == volume["size"]
    assert volumeByName["numberOfReplicas"] == volume["numberOfReplicas"]
    assert volumeByName["state"] == volume["state"]
    assert volumeByName["created"] == volume["created"]

    volume.attach(hostId=host_id)
    volume = wait_for_volume_state(client, VOLUME_NAME, "healthy")

    volumes = client.list_volume()
    assert len(volumes) == 1
    assert volumes[0]["name"] == volume["name"]
    assert volumes[0]["size"] == volume["size"]
    assert volumes[0]["numberOfReplicas"] == volume["numberOfReplicas"]
    assert volumes[0]["state"] == volume["state"]
    assert volumes[0]["created"] == volume["created"]
#    assert volumes[0]["endpoint"] == DEV_PATH + VOLUME_NAME

#    volume = client.by_id_volume(VOLUME_NAME)
#    assert volume["endpoint"] == DEV_PATH + VOLUME_NAME

    volume = volume.detach()

    wait_for_volume_state(client, VOLUME_NAME, "detached")

    client.delete(volume)

    wait_for_volume_delete(client, VOLUME_NAME)

    volumes = client.list_volume()
    assert len(volumes) == 0


def test_recurring_snapshot(clients):  # NOQA
    for host_id, client in clients.iteritems():
        break

    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)
    volume = wait_for_volume_state(client, VOLUME_NAME, "detached")

    snap2s = {"name": "snap2s", "cron": "@every 2s",
              "task": "snapshot", "retain": 3}
    snap3s = {"name": "snap3s", "cron": "@every 3s",
              "task": "snapshot", "retain": 2}
    volume.recurringUpdate(jobs=[snap2s, snap3s])

    time.sleep(0.1)
    volume = volume.attach(hostId=host_id)
    volume = wait_for_volume_state(client, VOLUME_NAME, "healthy")

    time.sleep(10)

    snapshots = volume.snapshotList()
    count = 0
    for snapshot in snapshots:
        if snapshot["removed"] is False:
            count += 1
    assert count == 5

    snap4s = {"name": "snap4s", "cron": "@every 4s",
              "task": "snapshot", "retain": 2}
    volume.recurringUpdate(jobs=[snap2s, snap4s])

    time.sleep(10)

    snapshots = volume.snapshotList()
    count = 0
    for snapshot in snapshots:
        if snapshot["removed"] is False:
            count += 1
    assert count == 7

    volume = volume.detach()

    wait_for_volume_state(client, VOLUME_NAME, "detached")

    client.delete(volume)

    wait_for_volume_delete(client, VOLUME_NAME)

    volumes = client.list_volume()
    assert len(volumes) == 0


def test_snapshot(clients):  # NOQA
    for host_id, client in clients.iteritems():
        break

    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)

    volume = wait_for_volume_state(client, VOLUME_NAME, "detached")
    assert volume["name"] == VOLUME_NAME
    assert volume["size"] == SIZE
    assert volume["numberOfReplicas"] == 2
    assert volume["state"] == "detached"

    volume = volume.attach(hostId=host_id)
    volume = wait_for_volume_state(client, VOLUME_NAME, "healthy")

    snapshot_test(client)
    volume = volume.detach()
    volume = wait_for_volume_state(client, VOLUME_NAME, "detached")

    client.delete(volume)
    volume = wait_for_volume_delete(client, VOLUME_NAME)

    volumes = client.list_volume()
    assert len(volumes) == 0


def snapshot_test(client):  # NOQA
    volume = client.by_id_volume(VOLUME_NAME)

    snap1 = volume.snapshotCreate()
    snap2 = volume.snapshotCreate()
    snap3 = volume.snapshotCreate()

    snapshots = volume.snapshotList()
    snapMap = {}
    for snap in snapshots:
        snapMap[snap["name"]] = snap

    assert snapMap[snap1["name"]]["name"] == snap1["name"]
    assert snapMap[snap1["name"]]["removed"] is False
    assert snapMap[snap2["name"]]["name"] == snap2["name"]
    assert snapMap[snap2["name"]]["parent"] == snap1["name"]
    assert snapMap[snap2["name"]]["removed"] is False
    assert snapMap[snap3["name"]]["name"] == snap3["name"]
    assert snapMap[snap3["name"]]["parent"] == snap2["name"]
    assert snapMap[snap3["name"]]["removed"] is False

    volume.snapshotDelete(name=snap3["name"])

    snapshots = volume.snapshotList(volume=VOLUME_NAME)
    snapMap = {}
    for snap in snapshots:
        snapMap[snap["name"]] = snap

    assert snapMap[snap1["name"]]["name"] == snap1["name"]
    assert snapMap[snap1["name"]]["removed"] is False
    assert snapMap[snap2["name"]]["name"] == snap2["name"]
    assert snapMap[snap2["name"]]["parent"] == snap1["name"]
    assert snapMap[snap2["name"]]["removed"] is False
    assert snapMap[snap3["name"]]["name"] == snap3["name"]
    assert snapMap[snap3["name"]]["parent"] == snap2["name"]
    assert snapMap[snap3["name"]]["children"] == ["volume-head"]
    assert snapMap[snap3["name"]]["removed"] is True

    snap = volume.snapshotGet(name=snap3["name"])
    assert snap["name"] == snap3["name"]
    assert snap["parent"] == snap3["parent"]
    assert snap["children"] == snap3["children"]
    assert snap["removed"] is True

    volume.snapshotRevert(name=snap2["name"])

    snapshots = volume.snapshotList(volume=VOLUME_NAME)
    snapMap = {}
    for snap in snapshots:
        snapMap[snap["name"]] = snap

    assert snapMap[snap1["name"]]["name"] == snap1["name"]
    assert snapMap[snap1["name"]]["removed"] is False
    assert snapMap[snap2["name"]]["name"] == snap2["name"]
    assert snapMap[snap2["name"]]["parent"] == snap1["name"]
    assert "volume-head" in snapMap[snap2["name"]]["children"]
    assert snap3["name"] in snapMap[snap2["name"]]["children"]
    assert snapMap[snap2["name"]]["removed"] is False
    assert snapMap[snap3["name"]]["name"] == snap3["name"]
    assert snapMap[snap3["name"]]["parent"] == snap2["name"]
    assert snapMap[snap3["name"]]["children"] == []
    assert snapMap[snap3["name"]]["removed"] is True

    volume.snapshotDelete(name=snap1["name"])
    volume.snapshotDelete(name=snap2["name"])

    volume.snapshotPurge()
    wait_for_snapshot_purge(volume, snap1["name"], snap3["name"])

    snapshots = volume.snapshotList(volume=VOLUME_NAME)
    snapMap = {}
    for snap in snapshots:
        snapMap[snap["name"]] = snap
    assert snap1["name"] not in snapMap
    assert snap3["name"] not in snapMap

    # it's the parent of volume-head, so it cannot be purged at this time
    assert snapMap[snap2["name"]]["name"] == snap2["name"]
    assert snapMap[snap2["name"]]["parent"] == ""
    assert "volume-head" in snapMap[snap2["name"]]["children"]
    assert snapMap[snap2["name"]]["removed"] is True


def test_backup(clients):  # NOQA
    for host_id, client in clients.iteritems():
        break

    volume = client.create_volume(name=VOLUME_NAME, size=SIZE,
                                  numberOfReplicas=2)
    volume = wait_for_volume_state(client, VOLUME_NAME, "detached")
    assert volume["name"] == VOLUME_NAME
    assert volume["size"] == SIZE
    assert volume["numberOfReplicas"] == 2
    assert volume["state"] == "detached"

    volume = volume.attach(hostId=host_id)
    volume = wait_for_volume_state(client, VOLUME_NAME, "healthy")

    backup_test(client, host_id)
    volume = volume.detach()
    volume = wait_for_volume_state(client, VOLUME_NAME, "detached")

    client.delete(volume)
    volume = wait_for_volume_delete(client, VOLUME_NAME)

    volumes = client.list_volume()
    assert len(volumes) == 0


def backup_test(client, host_id):  # NOQA
    volume = client.by_id_volume(VOLUME_NAME)

    setting = client.by_id_setting("backupTarget")
    setting = client.update(setting, value=common.get_backupstore_url())
    assert setting["value"] == common.get_backupstore_url()

    volume.snapshotCreate()
    snap2 = volume.snapshotCreate()
    volume.snapshotCreate()

    volume.snapshotBackup(name=snap2["name"])

    jobs = volume.jobList()
    found = False
    for job in jobs:
        if job["state"] == "ongoing" and job["jobType"] == "snapshot-backup":
            found = True
            break
    assert found

    found = False
    for i in range(100):
        bvs = client.list_backupVolume()
        for bv in bvs:
            if bv["name"] == VOLUME_NAME:
                found = True
                break
        if found:
            break
        time.sleep(1)
    assert found

    found = False
    for i in range(20):
        backups = bv.backupList()
        for b in backups:
            if b["snapshotName"] == snap2["name"]:
                found = True
                break
        if found:
            break
        time.sleep(1)
    assert found

    new_b = bv.backupGet(name=b["name"])
    assert new_b["name"] == b["name"]
    assert new_b["url"] == b["url"]
    assert new_b["snapshotName"] == b["snapshotName"]
    assert new_b["snapshotCreated"] == b["snapshotCreated"]
    assert new_b["created"] == b["created"]
    assert new_b["volumeName"] == b["volumeName"]
    assert new_b["volumeSize"] == b["volumeSize"]
    assert new_b["volumeCreated"] == b["volumeCreated"]

    # test restore
    volume = client.create_volume(name=VOLUME_RESTORE_NAME, size=SIZE,
                                  numberOfReplicas=2,
                                  fromBackup=b["url"])
    volume = wait_for_volume_state(client, VOLUME_RESTORE_NAME, "detached")
    assert volume["name"] == VOLUME_RESTORE_NAME
    assert volume["size"] == SIZE
    assert volume["numberOfReplicas"] == 2
    assert volume["state"] == "detached"
    volume = volume.attach(hostId=host_id)
    volume = wait_for_volume_state(client, VOLUME_RESTORE_NAME, "healthy")
    volume = volume.detach()
    volume = wait_for_volume_state(client, VOLUME_RESTORE_NAME, "detached")
    client.delete(volume)

    volume = wait_for_volume_delete(client, VOLUME_RESTORE_NAME)

    bv.backupDelete(name=b["name"])

    backups = bv.backupList()
    found = False
    for b in backups:
        if b["snapshotName"] == snap2["name"]:
            found = True
            break
    assert not found


def test_volume_multinode(clients):  # NOQA
    hosts = clients.keys()

    volume = clients[hosts[0]].create_volume(name=VOLUME_NAME, size=SIZE,
                                             numberOfReplicas=2)
    volume = wait_for_volume_state(clients[hosts[0]], VOLUME_NAME, "detached")

    for host_id in hosts:
        volume = volume.attach(hostId=host_id)
        volume = wait_for_volume_state(clients[hosts[1]],
                                       VOLUME_NAME, "healthy")
        assert volume["state"] == "healthy"
        assert volume["controller"]["hostId"] == host_id
        volume = volume.detach()
        volume = wait_for_volume_state(clients[hosts[2]],
                                       VOLUME_NAME, "detached")

    volume = volume.attach(hostId=hosts[0])
    volume = wait_for_volume_state(clients[hosts[1]], VOLUME_NAME, "healthy")
    assert volume["state"] == "healthy"
    assert volume["controller"]["hostId"] == hosts[0]

    snapshot_test(clients[hosts[1]])
    backup_test(clients[hosts[2]], hosts[2])

    clients[hosts[0]].delete(volume)
    wait_for_volume_delete(clients[hosts[1]], VOLUME_NAME)

    volumes = clients[hosts[2]].list_volume()
    assert len(volumes) == 0
