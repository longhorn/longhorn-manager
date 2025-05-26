package server

import (
	"net/http"

	"github.com/longhorn/longhorn-manager/webhook/conversion"
)

func Conversion() (http.Handler, []string, error) {
	conversions := []string{
		// TODO: add Conversions for Longhorn APIs
	}

	router, err := conversion.NewHandler()
	if err != nil {
		return nil, nil, err
	}
	return router, conversions, nil
}
