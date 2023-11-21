package types

const (
	ObjectStoreContainerPort = 7480
	ObjectStoreServicePort   = 80

	ObjectStoreStatusContainerPort = 9090
	ObjectStoreStatusServicePort   = 9090

	ObjectStoreUIContainerPort = 8080
	ObjectStoreUIServicePort   = 8080

	ObjectStoreLogLevel = "low"

	ObjectStoreStorageClassName = "longhorn-objectstorage-static"

	ObjectStoreContainerName   = "s3gw"
	ObjectStoreUIContainerName = "s3gw-ui"
)
