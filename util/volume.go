package util

// Use util/volume.go for volume related utility functions that would otherwise belong in controller/utils.go except
// that they are used by other components as well (e.g. manager).

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func IsVolumeMigrating(v *longhorn.Volume) bool {
	return v.Spec.MigrationNodeID != "" || v.Status.CurrentMigrationNodeID != ""
}

// IsMigratableVolume returns true if the volume is attached to a second node
// only temporarily, through a live migration. A shared block volume is attached
// to multiple nodes without migration, even if it is marked migratable.
func IsMigratableVolume(v *longhorn.Volume) bool {
	return v.Spec.Migratable && v.Spec.AccessMode == longhorn.AccessModeReadWriteMany && !v.Spec.SharedBlock
}

// IsSharedBlockVolume returns true if the volume is an RWX block volume attached
// to multiple nodes at the same time, with a single engine serving the engine
// frontends on all attached nodes.
func IsSharedBlockVolume(v *longhorn.Volume) bool {
	return v.Spec.SharedBlock && v.Spec.AccessMode == longhorn.AccessModeReadWriteMany
}

// IsRegularRWXVolume returns true if the volume is an RWX volume shared through
// a share manager (NFS).
func IsRegularRWXVolume(v *longhorn.Volume) bool {
	return v.Spec.AccessMode == longhorn.AccessModeReadWriteMany && !v.Spec.Migratable && !v.Spec.SharedBlock
}
