package types

func (v *VolumeInfo) DeepCopyInto(to *VolumeInfo) {
	*to = *v
	to.RecurringJobs = make([]RecurringJob, len(v.RecurringJobs))
	for i := 0; i < len(v.RecurringJobs); i++ {
		to.RecurringJobs[i] = v.RecurringJobs[i]
	}
}
