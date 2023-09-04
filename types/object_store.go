package types

const (
	ObjectStoreLabelKeyApp = "app.longhorn.io/name"
	ObjectStoreLabelApp    = "object-store"

	ObjectStoreLabelKeyInstance = "app.longhorn.io/instance"

	ObjectStoreLabelKeyComponent = "app.longhorn.io/component"
	ObjectStoreLabelComponent    = "object-gateway"

	ObjectStoreLabelKeyManagedBy = "app.longhorn.io/managed-by"
	ObjectStoreLabelManagedBy    = "longhorn-manager"

	ObjectStoreContainerPort = 7480
	ObjectStoreServicePort   = 80

	ObjectStoreUIContainerPort = 8080
	ObjectStoreUIServicePort   = 8080
)
