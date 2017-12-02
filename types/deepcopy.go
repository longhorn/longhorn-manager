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
