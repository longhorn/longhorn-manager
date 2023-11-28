package types

const (
	ObjectStoreContainerPort = 7480
	ObjectStoreServicePort   = 80
	ObjectStorePortName      = "s3"

	ObjectStoreStatusContainerPort = 9090
	ObjectStoreStatusServicePort   = 9090
	ObjectStoreStatusPortName      = "status"

	ObjectStoreUIContainerPort = 8080
	ObjectStoreUIServicePort   = 8080
	ObjectStoreUIPortName      = "ui"

	ObjectStoreLogLevel = "low"

	ObjectStoreStorageClassName = "longhorn-objectstorage-static"

	ObjectStoreContainerName   = "s3gw"
	ObjectStoreUIContainerName = "s3gw-ui"

	ObjectStorePodVolumeName = "data"
)
