package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ShardGroupState is the aggregate health state of an EC ShardGroup.
// +kubebuilder:validation:Enum=healthy;degraded;offline;rebuilding;growing;""
type ShardGroupState string

const (
	// ShardGroupStateHealthy means all k+m slots are in the normal state.
	ShardGroupStateHealthy = ShardGroupState("healthy")
	// ShardGroupStateDegraded means 1..m slots have failed; the volume is still serving I/O via
	// Reed-Solomon reconstruction.
	ShardGroupStateDegraded = ShardGroupState("degraded")
	// ShardGroupStateOffline means more than m slots have failed; the volume cannot serve I/O.
	ShardGroupStateOffline = ShardGroupState("offline")
	// ShardGroupStateRebuilding means a background shard rebuild is in progress.
	ShardGroupStateRebuilding = ShardGroupState("rebuilding")
	// ShardGroupStateGrowing means a capacity expansion (bdev_ec_resize) is in progress.
	ShardGroupStateGrowing = ShardGroupState("growing")
)

const (
	// ShardGroupConditionTypeDegradedRead is set while a degraded read has returned
	// EIO on a WIB-dirty stripe. It stays set as long as the SPDK DegradedReadEioDirty
	// counter is non-zero, which clears only when the ShardGroup process is recreated.
	ShardGroupConditionTypeDegradedRead = "DegradedRead"

	ShardGroupConditionReasonDegradedReadEIO = "DegradedReadEIO"
)

// ShardGroupSpec defines the desired state of the Longhorn ShardGroup
type ShardGroupSpec struct {
	// VolumeName is the name of the owning Volume CR. Immutable after creation.
	// +optional
	VolumeName string `json:"volumeName"`
	// DataChunks is the k parameter of the EC array. Immutable after creation.
	// +kubebuilder:validation:Minimum=1
	// +optional
	DataChunks int `json:"dataChunks"`
	// ParityChunks is the m parameter of the EC array. The ShardGroup tolerates up to m
	// simultaneous shard failures. Immutable after creation.
	// +kubebuilder:validation:Minimum=1
	// +optional
	ParityChunks int `json:"parityChunks"`
	// StripSizeKB is the EC chunk size in KiB. Must be a power of two in the range [4, 1024].
	// Immutable after creation.
	// +kubebuilder:validation:Minimum=4
	// +kubebuilder:validation:Maximum=1024
	// +optional
	StripSizeKB int `json:"stripSizeKB"`
	// NodeID identifies the node hosting the long-lived ShardGroup process that owns the
	// EC volume's bdev_ec, lvol store, head lvol, and NVMe-oF export. It is typically equal
	// to Engine.Spec.NodeID for engine-process co-location. The Volume controller is the
	// sole writer and sets this field at first attach. NodeID is NOT cleared on volume
	// detach (the ShardGroup process keeps running across detach to preserve the lvstore
	// and head lvol on the encoded shard blocks for fast re-attach); it only changes on
	// engine-node failover or volume deletion.
	// +optional
	NodeID string `json:"nodeID"`
}

// ShardGroupStatus defines the observed state of the Longhorn ShardGroup
type ShardGroupStatus struct {
	// OwnerID is the ID of the node that owns this ShardGroup.
	// +optional
	OwnerID string `json:"ownerID"`
	// State is the aggregate health state of the EC array.
	// +optional
	State ShardGroupState `json:"state"`
	// FailedCount is the number of slots currently in the failed state. Slots being replaced
	// (ShardStateReplacing) are not counted; an active rebuild is tracked separately via
	// RebuildInProgress.
	// +optional
	FailedCount int `json:"failedCount"`
	// ShardRefs is an ordered list of Shard CR names, where the list index equals the EC slot index.
	// +optional
	// +nullable
	ShardRefs []string `json:"shardRefs"`
	// WIBDirtyRegion is the number of dirty WIB regions reported by the EC bdev.
	// +optional
	WIBDirtyRegion int `json:"wibDirtyRegion"`
	// ScrubInProgress indicates whether a background scrub is currently running.
	// +optional
	ScrubInProgress bool `json:"scrubInProgress"`
	// RebuildInProgress indicates whether a background shard rebuild is currently running.
	// +optional
	RebuildInProgress bool `json:"rebuildInProgress"`
	// GrowInProgress indicates whether a capacity expansion is currently running.
	// +optional
	GrowInProgress bool `json:"growInProgress"`
	// EvictingSlots is the ordered list of slot indices currently in the eviction
	// pipeline (old Shard CR deleted, replacement not yet rebuilt). Tracked in
	// status so VolumeEvictionController can observe progress without annotation parsing.
	// +optional
	// +nullable
	EvictingSlots []int `json:"evictingSlots,omitempty"`
	// IntentionalDeleteSlots is the list of slot indices whose old Shard CR was
	// deleted intentionally (admin kubectl delete, eviction, drain). The replacement
	// Shard CR's failure-recovery debounce is bypassed for these slots so the
	// replace+rebuild sequence runs immediately rather than after the full
	// replica-replenishment-wait-interval. Cleared once the replacement reaches
	// ShardStateNormal with StorageIP set, and defensively cleared on ShardGroup
	// process re-bind.
	// +optional
	// +nullable
	IntentionalDeleteSlots []int `json:"intentionalDeleteSlots,omitempty"`
	// ECShardAddressMap maps shard slot index (as string) to the NVMe-oF address ("ip:port")
	// of each healthy shard instance (ShardStateNormal with a non-empty StorageIP and Port).
	// It is the base-bdev list for the ShardGroup process's EC array, and acts as the readiness
	// gate (together with every Shard CR being in ShardStateNormal) before the ShardGroup
	// process is provisioned.
	// +optional
	// +nullable
	ECShardAddressMap map[string]string `json:"ecShardAddressMap,omitempty"`
	// ProcessState is the runtime state of the ShardGroup process owned by this CR.
	// +optional
	ProcessState InstanceState `json:"processState,omitempty"`
	// StorageIP is the storage-network IP of the InstanceManager pod hosting the ShardGroup
	// process. Combined with Port and NQN, it forms the NVMe-oF endpoint that an EC volume's
	// engine attaches to.
	// +optional
	StorageIP string `json:"storageIP,omitempty"`
	// Port is the NVMe-oF port allocated for the ShardGroup process's exposed head lvol.
	// +optional
	Port int32 `json:"port,omitempty"`
	// NQN is the NVMe-oF subsystem NQN of the ShardGroup process's exposed head lvol.
	// +optional
	NQN string `json:"nqn,omitempty"`
	// LvstoreUUID is reserved for the UUID of the lvol store created on bdev_ec inside the
	// ShardGroup process. It is currently unpopulated: the ShardGroup instance does not surface
	// the lvstore UUID over the instance-manager proto yet. Kept for forward-compatible
	// observability; not on the engine data path.
	// +optional
	LvstoreUUID string `json:"lvstoreUUID,omitempty"`
	// HeadLvolUUID is the UUID of the head lvol on the ShardGroup-process-owned lvol
	// store. Surfaced for observability and debugging only.
	// +optional
	HeadLvolUUID string `json:"headLvolUUID,omitempty"`
	// InstanceManagerName is the InstanceManager currently hosting the ShardGroup process,
	// set during provisioning and cleared on teardown. During a re-bind to a new node it may
	// still reference the previous InstanceManager until teardown completes, so consumers must
	// validate it against Spec.NodeID before trusting the endpoint above.
	// +optional
	InstanceManagerName string `json:"instanceManagerName,omitempty"`
	// Conditions holds the latest observations of the ShardGroup's state, such as a
	// degraded read that returned EIO.
	// +optional
	// +nullable
	Conditions []Condition `json:"conditions"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhsg
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="Volume",type=string,JSONPath=`.spec.volumeName`,description="The volume that owns this ShardGroup"
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="The aggregate health state of the EC array"
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.spec.nodeID`,description="The node hosting the ShardGroup process (typically co-located with the engine)"
// +kubebuilder:printcolumn:name="Failed",type=integer,JSONPath=`.status.failedCount`,description="The number of failed slots"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// ShardGroup is where Longhorn stores ShardGroup object.
type ShardGroup struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ShardGroupSpec   `json:"spec,omitempty"`
	Status ShardGroupStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ShardGroupList is a list of ShardGroups.
type ShardGroupList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ShardGroup `json:"items"`
}
