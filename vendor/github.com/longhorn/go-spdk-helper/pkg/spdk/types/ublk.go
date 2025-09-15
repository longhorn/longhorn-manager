package types

type UblkCreateTargetRequest struct {
	Cpumask         string `json:"cpumask,omitempty"`
	DisableUserCopy bool   `json:"disable_user_copy"`
}

type UblkGetDisksRequest struct {
	UblkId int32 `json:"ublk_id"`
}

type UblkDevice struct {
	BdevName   string `json:"bdev_name"`
	ID         int32  `json:"id"`
	NumQueues  int32  `json:"num_queues"`
	QueueDepth int32  `json:"queue_depth"`
	UblkDevice string `json:"ublk_device"`
}

type UblkStartDiskRequest struct {
	BdevName   string `json:"bdev_name"`
	UblkId     int32  `json:"ublk_id"`
	QueueDepth int32  `json:"queue_depth"`
	NumQueues  int32  `json:"num_queues"`
}

type UblkRecoverDiskRequest struct {
	BdevName string `json:"bdev_name"`
	UblkId   int32  `json:"ublk_id"`
}

type UblkStopDiskRequest struct {
	UblkId int32 `json:"ublk_id"`
}
