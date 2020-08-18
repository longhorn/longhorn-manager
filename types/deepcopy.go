package types

func (v *VolumeSpec) DeepCopyInto(to *VolumeSpec) {
	*to = *v
	if v.DiskSelector != nil {
		to.DiskSelector = make([]string, len(v.DiskSelector))
		for i := 0; i < len(v.DiskSelector); i++ {
			to.DiskSelector[i] = v.DiskSelector[i]
		}
	}
	if v.NodeSelector != nil {
		to.NodeSelector = make([]string, len(v.NodeSelector))
		for i := 0; i < len(v.NodeSelector); i++ {
			to.NodeSelector[i] = v.NodeSelector[i]
		}
	}
	if v.RecurringJobs != nil {
		to.RecurringJobs = make([]RecurringJob, len(v.RecurringJobs))
		for i := 0; i < len(v.RecurringJobs); i++ {
			toRecurringJob := v.RecurringJobs[i]
			if v.RecurringJobs[i].Labels != nil {
				toRecurringJob.Labels = make(map[string]string)
				for key, value := range v.RecurringJobs[i].Labels {
					toRecurringJob.Labels[key] = value
				}
			}
			to.RecurringJobs[i] = toRecurringJob
		}
	}
}

func (v *VolumeStatus) DeepCopyInto(to *VolumeStatus) {
	*to = *v
	if v.Conditions != nil {
		to.Conditions = make(map[string]Condition)
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
	if e.BackupStatus != nil {
		to.BackupStatus = make(map[string]*BackupStatus)
		for key, value := range e.BackupStatus {
			to.BackupStatus[key] = &BackupStatus{}
			*to.BackupStatus[key] = *value
		}
	}
	if e.ReplicaModeMap != nil {
		to.ReplicaModeMap = make(map[string]ReplicaMode)
		for key, value := range e.ReplicaModeMap {
			to.ReplicaModeMap[key] = value
		}
	}
	if e.RestoreStatus != nil {
		to.RestoreStatus = make(map[string]*RestoreStatus)
		for key, value := range e.RestoreStatus {
			to.RestoreStatus[key] = &RestoreStatus{}
			*to.RestoreStatus[key] = *value
		}
	}
	if e.PurgeStatus != nil {
		to.PurgeStatus = make(map[string]*PurgeStatus)
		for key, value := range e.PurgeStatus {
			to.PurgeStatus[key] = &PurgeStatus{}
			*to.PurgeStatus[key] = *value
		}
	}
	if e.RebuildStatus != nil {
		to.RebuildStatus = make(map[string]*RebuildStatus)
		for key, value := range e.RebuildStatus {
			to.RebuildStatus[key] = &RebuildStatus{}
			*to.RebuildStatus[key] = *value
		}
	}
	if e.Snapshots != nil {
		to.Snapshots = make(map[string]*Snapshot)
		for key, value := range e.Snapshots {
			to.Snapshots[key] = &Snapshot{}
			*to.Snapshots[key] = *value
		}
	}
}

func (n *NodeSpec) DeepCopyInto(to *NodeSpec) {
	*to = *n
	if n.Tags != nil {
		to.Tags = make([]string, len(n.Tags))
		for i := 0; i < len(n.Tags); i++ {
			to.Tags[i] = n.Tags[i]
		}
	}
	if n.DiskPathMap != nil {
		to.DiskPathMap = make(map[string]struct{})
		for key, value := range n.DiskPathMap {
			to.DiskPathMap[key] = value
		}
	}
}

func (n *NodeStatus) DeepCopyInto(to *NodeStatus) {
	*to = *n
	if n.Conditions != nil {
		to.Conditions = make(map[string]Condition)
		for key, value := range n.Conditions {
			to.Conditions[key] = value
		}
	}
	if n.DiskPathIDMap != nil {
		to.DiskPathIDMap = make(map[string]string)
		for key, value := range n.DiskPathIDMap {
			to.DiskPathIDMap[key] = value
		}
	}
}

func (d *DiskSpec) DeepCopyInto(to *DiskSpec) {
	*to = *d
	if d.Tags != nil {
		to.Tags = make([]string, len(d.Tags))
		for i := 0; i < len(d.Tags); i++ {
			to.Tags[i] = d.Tags[i]
		}
	}
}

func (d *DiskStatus) DeepCopyInto(to *DiskStatus) {
	*to = *d
	if d.Conditions != nil {
		to.Conditions = make(map[string]Condition)
		for key, value := range d.Conditions {
			to.Conditions[key] = value
		}
	}
	if d.ScheduledReplica != nil {
		to.ScheduledReplica = make(map[string]int64)
		for key, value := range d.ScheduledReplica {
			to.ScheduledReplica[key] = value
		}
	}
}

func (n *InstanceManagerStatus) DeepCopyInto(to *InstanceManagerStatus) {
	*to = *n
	if n.Instances != nil {
		to.Instances = make(map[string]InstanceProcess)
		for key, value := range n.Instances {
			to.Instances[key] = value
		}
	}
}

func (ei *EngineImageStatus) DeepCopyInto(to *EngineImageStatus) {
	*to = *ei
	if ei.Conditions != nil {
		to.Conditions = make(map[string]Condition)
		for key, value := range ei.Conditions {
			to.Conditions[key] = value
		}
	}
}
