package controller

import (
	"github.com/longhorn/longhorn-manager/types"
	"github.com/sirupsen/logrus"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

func handleReconcileErrorLogging(logger logrus.FieldLogger, err error, mesg string) {
	if types.ErrorIsInvalidState(err) {
		logger.WithError(err).Trace(mesg)
		return
	}

	if apierrors.IsConflict(err) {
		logger.WithError(err).Debug(mesg)
	} else {
		logger.WithError(err).Error(mesg)
	}
}
