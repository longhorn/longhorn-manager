package volume

import (
	"testing"

	"github.com/stretchr/testify/assert"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestValidateTopologyZonePin(t *testing.T) {
	newVolume := func(terms []longhorn.VolumeTopologyTerm, affinity longhorn.ReplicaZoneSoftAntiAffinity) *longhorn.Volume {
		return &longhorn.Volume{
			Spec: longhorn.VolumeSpec{
				TopologyRequirement:         terms,
				ReplicaZoneSoftAntiAffinity: affinity,
			},
		}
	}
	pinned := []longhorn.VolumeTopologyTerm{{Zone: "zone-1", Region: "region-1"}}

	assert.NoError(t, validateTopologyZonePin(newVolume(pinned, longhorn.ReplicaZoneSoftAntiAffinityEnabled)))

	err := validateTopologyZonePin(newVolume(pinned, longhorn.ReplicaZoneSoftAntiAffinityDisabled))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "zone anti-affinity cannot be satisfied within one zone")

	// The mutator fills enabled before validation, so ignored on a pinned
	// volume only occurs when something bypasses it; the invariant still
	// rejects it.
	assert.Error(t, validateTopologyZonePin(newVolume(pinned, longhorn.ReplicaZoneSoftAntiAffinityDefault)))

	// Volumes that do not pin a single zone are free to use any value.
	regionOnly := []longhorn.VolumeTopologyTerm{{Region: "region-1"}}
	assert.NoError(t, validateTopologyZonePin(newVolume(regionOnly, longhorn.ReplicaZoneSoftAntiAffinityDisabled)))
	assert.NoError(t, validateTopologyZonePin(newVolume(nil, longhorn.ReplicaZoneSoftAntiAffinityDisabled)))
}
