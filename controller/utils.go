package controller

import (
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func handleReconcileErrorLogging(logger logrus.FieldLogger, err error, mesg string) {
	if types.ErrorIsInvalidState(err) {
		return
	}

	if apierrors.IsConflict(err) {
		logger.WithError(err).Debug(mesg)
	} else {
		logger.WithError(err).Error(mesg)
	}
}
