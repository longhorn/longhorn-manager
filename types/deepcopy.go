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
