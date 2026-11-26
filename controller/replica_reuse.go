package controller

import (
	"time"

	"github.com/cockroachdb/errors"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func calculateFailedReplicaReuseDelay(r *longhorn.Replica, now time.Time, waitInterval time.Duration) (time.Duration, error) {
	if r == nil || r.Spec.FailedAt == "" || r.Spec.HealthyAt == "" || waitInterval <= 0 {
		return 0, nil
	}

	failedAt, err := util.ParseTime(r.Spec.FailedAt)
	if err != nil {
		return 0, errors.Wrapf(err, "failed to parse failed timestamp %v of replica %v", r.Spec.FailedAt, r.Name)
	}

	reuseAt := failedAt.Add(waitInterval)
	if !now.Before(reuseAt) {
		return 0, nil
	}

	return reuseAt.Sub(now), nil
}

func (c *VolumeController) getFailedReplicaReuseDelay(r *longhorn.Replica) (time.Duration, error) {
	waitIntervalSeconds, err := c.ds.GetSettingAsInt(types.SettingNameReplicaReplenishmentWaitInterval)
	if err != nil {
		return 0, errors.Wrap(err, "failed to get replica replenishment wait interval")
	}

	now, err := util.ParseTime(c.nowHandler())
	if err != nil {
		return 0, errors.Wrap(err, "failed to parse current time")
	}

	return calculateFailedReplicaReuseDelay(r, now, time.Duration(waitIntervalSeconds)*time.Second)
}
