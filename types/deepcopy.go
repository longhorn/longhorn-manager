package types

func (v *VolumeSpec) DeepCopyInto(to *VolumeSpec) {
	*to = *v
	if v.RecurringJobs == nil {
		return
	}
	to.RecurringJobs = make([]RecurringJob, len(v.RecurringJobs))
	for i := 0; i < len(v.RecurringJobs); i++ {
		to.RecurringJobs[i] = v.RecurringJobs[i]
	}
}

func (v *VolumeStatus) DeepCopyInto(to *VolumeStatus) {
	*to = *v
	if v.Conditions != nil {
		to.Conditions = make(map[VolumeConditionType]Condition)
		for key, value := range v.Conditions {
			to.Conditions[key] = value
		}
	}
}

func (e *EngineSpec) DeepCopyInto(to *EngineSpec) {
	*to = *e
	if e.ReplicaAddressMap != nil {
		to.ReplicaAddressMap = make(map[string]string)
		for key, value := range e.ReplicaAddressMap {
			to.ReplicaAddressMap[key] = value
		}
	}
	if e.UpgradedReplicaAddressMap != nil {
		to.UpgradedReplicaAddressMap = make(map[string]string)
		for key, value := range e.UpgradedReplicaAddressMap {
			to.UpgradedReplicaAddressMap[key] = value
		}
	}
}

func (e *EngineStatus) DeepCopyInto(to *EngineStatus) {
	*to = *e
	if e.ReplicaModeMap == nil {
		return
	}
	to.ReplicaModeMap = make(map[string]ReplicaMode)
	for key, value := range e.ReplicaModeMap {
		to.ReplicaModeMap[key] = value
	}
}

func (n *NodeSpec) DeepCopyInto(to *NodeSpec) {
	*to = *n
	if n.Disks == nil {
		return
	}
	to.Disks = make(map[string]DiskSpec)
	for key, value := range n.Disks {
		to.Disks[key] = value
	}
}

func (n *NodeStatus) DeepCopyInto(to *NodeStatus) {
	*to = *n
	if n.DiskStatus == nil {
		return
	}
	to.DiskStatus = make(map[string]DiskStatus)
	for key, value := range n.DiskStatus {
		to.DiskStatus[key] = value
	}
	if n.Conditions != nil {
		to.Conditions = make(map[NodeConditionType]Condition)
		for key, value := range n.Conditions {
			to.Conditions[key] = value
		}
	}
}

func (n *DiskStatus) DeepCopyInto(to *DiskStatus) {
	*to = *n
	if n.Conditions == nil {
		return
	}
	to.Conditions = make(map[DiskConditionType]Condition)
	for key, value := range n.Conditions {
		to.Conditions[key] = value
	}
}

func (n *InstanceManagerStatus) DeepCopyInto(to *InstanceManagerStatus) {
	*to = *n
	if n.Instances != nil {
		to.Instances = make(map[string]InstanceProcessStatus)
		for key, value := range n.Instances {
			to.Instances[key] = value
		}
	}
}
