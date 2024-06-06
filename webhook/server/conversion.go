package server

import (
	"net/http"

	"github.com/longhorn/longhorn-manager/webhook/conversion"
	"github.com/longhorn/longhorn-manager/webhook/resources/backingimage"
	"github.com/longhorn/longhorn-manager/webhook/resources/backuptarget"
	"github.com/longhorn/longhorn-manager/webhook/resources/engineimage"
	"github.com/longhorn/longhorn-manager/webhook/resources/image"
	"github.com/longhorn/longhorn-manager/webhook/resources/node"
	"github.com/longhorn/longhorn-manager/webhook/resources/volume"
)

func Conversion() (http.Handler, []string, error) {
	conversions := []string{
		backingimage.NewConversion(),
		backuptarget.NewConversion(),
		engineimage.NewConversion(),
		image.NewConversion(),
		node.NewConversion(),
		volume.NewConversion(),
	}

	router, err := conversion.NewHandler()
	if err != nil {
		return nil, nil, err
	}
	return router, conversions, nil
}
