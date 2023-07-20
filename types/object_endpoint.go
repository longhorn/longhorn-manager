package types

const (
	ObjectEndpointLabelKeyApp = "app.longhorn.io/name"
	ObjectEndpointLabelApp    = "object-endpoint"

	ObjectEndpointLabelKeyInstance = "app.longhorn.io/instance"

	ObjectEndpointLabelKeyComponent = "app.longhorn.io/component"
	ObjectEndpointLabelComponent    = "object-gateway"

	ObjectEndpointLabelKeyManagedBy = "app.longhorn.io/managed-by"
	ObjectEndpointLabelManagedBy    = "longhorn-manager"

	ObjectEndpointContainerPort = 7480
	ObjectEndpointServicePort   = 80
)
