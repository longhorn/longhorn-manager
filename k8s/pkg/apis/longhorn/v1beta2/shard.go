package v1beta2

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// ShardState is the health state of an individual EC shard slot.
// +kubebuilder:validation:Enum=normal;failed;replacing;""
type ShardState string

const (
	// ShardStateNormal means the shard is healthy and serving I/O normally.
	ShardStateNormal = ShardState("normal")
	// ShardStateFailed means the shard's base bdev has failed; the EC array is degraded.
	ShardStateFailed = ShardState("failed")
	// ShardStateReplacing means a replacement bdev has been hot-swapped in; a Reed-Solomon rebuild
	// may or may not be active. Whether a rebuild is running is indicated at the ShardGroup level
	// by ShardGroup.Status.RebuildInProgress.
	ShardStateReplacing = ShardState("replacing")
)

// ShardRole is the EC role of a shard slot. It is a pure function of (slotIndex, k) and is
// stored in status for informational purposes only.
// +kubebuilder:validation:Enum=data;parity;""
type ShardRole string

const (
	// ShardRoleData means this slot holds a data chunk (slot index < k).
	ShardRoleData = ShardRole("data")
	// ShardRoleParity means this slot holds a parity chunk (slot index >= k).
	ShardRoleParity = ShardRole("parity")
)

// ShardSpec defines the desired state of the Longhorn Shard
type ShardSpec struct {
	// ShardGroupName is the name of the owning ShardGroup CR. Immutable after creation.
	// +optional
	ShardGroupName string `json:"shardGroupName"`
	// SlotIndex is the zero-based position of this shard in the EC base-bdev array.
	// Determines the shard's role: indices 0..k-1 are DATA, k..k+m-1 are PARITY.
	// Immutable after creation.
	// +kubebuilder:validation:Minimum=0
	// +optional
	SlotIndex int `json:"slotIndex"`
	// Size is the shard lvol size in bytes. Set by the ShardGroup controller at creation time
	// and used for idempotent reconciliation.
	// +kubebuilder:validation:Type=string
	// +optional
	Size int64 `json:"size,string"`
	// NodeID is the node where this shard's lvol resides and its NVMe-oF target runs.
	// +optional
	NodeID string `json:"nodeID"`
	// DiskPath is the path of the disk that hosts the shard lvol.
	// +optional
	DiskPath string `json:"diskPath"`
	// DiskUUID is the UUID of the disk that hosts the shard lvol.
	// +optional
	DiskUUID string `json:"diskUUID"`
	// EvictionRequested indicates this shard should be relocated to a different node or disk.
	// Set by the node controller when the shard's node is being drained or its disk is evicted.
	// +optional
	EvictionRequested bool `json:"evictionRequested"`
}

// ShardStatus defines the observed state of the Longhorn Shard
type ShardStatus struct {
	// OwnerID is the ID of the node that owns this Shard.
	// +optional
	OwnerID string `json:"ownerID"`
	// State is the health state of this EC shard slot.
	// +optional
	State ShardState `json:"state"`
	// Role is the EC role of this slot (data or parity). Derived from SlotIndex and the parent
	// ShardGroup's DataChunks; stored here for informational purposes only.
	// +optional
	Role ShardRole `json:"role"`
	// StorageIP is the IP address of the NVMe-oF target exported by the shard's InstanceManager.
	// Populated after the shard instance is running.
	// +optional
	StorageIP string `json:"storageIP"`
	// Port is the NVMe-oF port of the shard's target export.
	// +optional
	Port int32 `json:"port"`
	// RebuildProgress is the rebuild completion percentage (0-100).
	// +optional
	RebuildProgress int `json:"rebuildProgress"`
	// LastFailureTimestamp is the RFC3339 timestamp of the most recent shard failure.
	// +optional
	LastFailureTimestamp string `json:"lastFailureTimestamp"`
	// ReplaceTriggered is set to true after shard replacement has been initiated, to prevent
	// re-issuing the replace command on subsequent cycles while SPDK advances the slot state.
	// Cleared when the slot state transitions away from Failed.
	// +optional
	ReplaceTriggered bool `json:"replaceTriggered"`
}

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +kubebuilder:resource:shortName=lhsd
// +kubebuilder:subresource:status
// +kubebuilder:storageversion
// +kubebuilder:printcolumn:name="ShardGroup",type=string,JSONPath=`.spec.shardGroupName`,description="The ShardGroup that owns this Shard"
// +kubebuilder:printcolumn:name="Slot",type=integer,JSONPath=`.spec.slotIndex`,description="The EC slot index of this Shard"
// +kubebuilder:printcolumn:name="Node",type=string,JSONPath=`.spec.nodeID`,description="The node that the shard instance runs on"
// +kubebuilder:printcolumn:name="State",type=string,JSONPath=`.status.state`,description="The health state of this shard slot"
// +kubebuilder:printcolumn:name="Role",type=string,JSONPath=`.status.role`,description="The EC role of this shard slot (data or parity)"
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// Shard is where Longhorn stores Shard object.
type Shard struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ShardSpec   `json:"spec,omitempty"`
	Status ShardStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// ShardList is a list of Shards.
type ShardList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Shard `json:"items"`
}
