# Kata confidential direct-volume mode

This opt-in Longhorn V1 CSI node path passes a raw block device to a Kata
confidential guest without formatting, mounting, encrypting, or reading it on
the host. It is inactive unless the provisioned volume carries the exact
StorageClass parameter:

```yaml
allowVolumeExpansion: false
parameters:
  kataConfidentialDirectVolume: "true"
  dataEngine: v1
  encrypted: "false"
  frontend: blockdev
  migratable: "false"
  numberOfReplicas: "1"
```

All unmarked volumes keep the standard Longhorn CSI path. Resize is not part of
this profile, so its StorageClass must keep expansion disabled.

## Contract

The marked volume must be a filesystem-mode, `ReadWriteOncePod`, ext4 PVC with
one Longhorn V1 replica, the block-device frontend, and no Longhorn host
encryption or migration. The PVC must explicitly use `volumeMode: Filesystem`
and contain an immutable storage-manifest URI in this annotation:

```text
io.katacontainers.storage/confidential-manifest-uri
```

The URI must have the canonical `kbs:///repository/type/tag` form. Query
parameters, fragments, path traversal, and the legacy key/volume annotations
are rejected before lifecycle state is created.

The external provisioner runs with `--extra-create-metadata=true`, allowing the
node plugin to find the PVC without copying its annotation into Longhorn volume
parameters or lifecycle state.

`NodeStageVolume` validates the attached raw endpoint and persists only bounded
cleanup metadata. `NodePublishVolume` invokes the host's exact runtime-rs
`/opt/kata/bin/kata-ctl` through the existing namespace helper and registers
this typed mount object:

```json
{
  "volume-type": "directvol",
  "device": "/dev/longhorn/example-volume",
  "fstype": "confidential-storage",
  "confidential-storage": {
    "manifest-uri": "kbs:///tenant/storage-manifests/workspace-v1",
    "requested-access": "readWrite"
  }
}
```

Requested access is derived from the CSI `NodePublishVolume` request; v1
accepts only read-write publication. The outer fstype is a fail-closed protocol
discriminator. Kata validates the typed object, matches the manifest, access,
and container mount to measured Agent policy, and asks CDH to resolve and
enforce the manifest. Older Kata components that do not understand the object
fail on the unknown fstype rather than silently mounting a raw ext4 device.

No key, protection profile, filesystem parameters, or plaintext are present in
CSI requests, lifecycle state, mount metadata, command output, or logs.
Longhorn transports only the raw ciphertext device and non-secret activation
intent. CDH owns LUKS2, dm-integrity, and ext4 initialization or reopen; Kata
mounts only the mapper device returned by CDH.

Unpublish and unstage remove Kata registration and lifecycle state
idempotently. Volume statistics are requested from the guest through Kata.
`NodeExpandVolume` returns a stable `FailedPrecondition` without mutation.

## Runtime prerequisite

The CSI plugin must run on a node whose host root contains the matching
runtime-rs `/opt/kata/bin/kata-ctl`. The plugin uses `nsmounter --host-root` so the
runtime observes the host's direct-volume registry and shim sockets. Kata
command diagnostics are returned with an 8 KiB bound; the contract forbids
secret material in those commands. If this prerequisite or any validation
fails, the operation fails closed and never falls back to the host-mount path.
