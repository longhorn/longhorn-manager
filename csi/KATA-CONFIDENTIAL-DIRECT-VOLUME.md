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
and contain the non-secret KBS resource URI in this annotation:

```text
io.katacontainers.storage/confidential-key-uri
```

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
    "profile": "luks2-integrity-ext4",
    "volume-id": "example-volume",
    "key-uri": "kbs:///tenant/storage/key"
  }
}
```

The outer fstype is a fail-closed protocol discriminator. Kata validates the
typed object, matches its complete tuple to measured init-data, and asks CDH to
activate persistent journaled LUKS2 integrity and ext4. Older Kata components
that do not understand the object fail on the unknown fstype rather than
silently mounting a raw ext4 device.

No key bytes are present in CSI requests, lifecycle state, mount metadata,
command output, or logs. The volume ID and KBS URI are bounded non-secret
identifiers. Kata and the attested guest own LUKS2 initialization or reopen and
the ext4 mount.

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
