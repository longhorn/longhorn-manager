package engineapi

import (
	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// EngineFrontendClientProxy extends EngineFrontendClient with resource cleanup.
type EngineFrontendClientProxy interface {
	EngineFrontendClient
	Close()
}

// EngineFrontendProxy wraps an InstanceManagerClient to provide
// EngineFrontendClientProxy operations for the v2 initiator.
type EngineFrontendProxy struct {
	logger   logrus.FieldLogger
	imClient *InstanceManagerClient
}

// NewEngineFrontendClientProxy creates a new EngineFrontendClientProxy
// using the given InstanceManager to communicate with the running initiator.
func NewEngineFrontendClientProxy(im *longhorn.InstanceManager, logger logrus.FieldLogger) (EngineFrontendClientProxy, error) {
	if im.Status.CurrentState != longhorn.InstanceManagerStateRunning {
		return nil, errors.Errorf("%v instance manager is in %v, not running state", im.Name, im.Status.CurrentState)
	}

	c, err := NewInstanceManagerClient(im, false)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create engine frontend client proxy")
	}

	return &EngineFrontendProxy{
		logger:   logger,
		imClient: c,
	}, nil
}

func (p *EngineFrontendProxy) Close() {
	if p.imClient == nil {
		p.logger.Warn("Engine frontend proxy: IM client not set")
		return
	}
	if err := p.imClient.Close(); err != nil {
		p.logger.WithError(err).Warn("Failed to close engine frontend client proxy")
	}
}

// EngineFrontendGet retrieves the current state of the engine frontend instance
func (p *EngineFrontendProxy) EngineFrontendGet(ef *longhorn.EngineFrontend) (*longhorn.InstanceProcess, error) {
	return p.imClient.InstanceGet(ef.Spec.DataEngine, ef.Name, string(longhorn.InstanceTypeEngineFrontend))
}

// EngineFrontendSwitchOverTarget switches the initiator to a new engine target.
// switchoverPhase controls phased switchover: "preparing", "switching", "promoting", or "" for atomic.
func (p *EngineFrontendProxy) EngineFrontendSwitchOverTarget(ef *longhorn.EngineFrontend, targetAddress, switchoverPhase string) error {
	return p.imClient.EngineFrontendSwitchOverTarget(ef.Spec.DataEngine, ef.Name, targetAddress, ef.Spec.EngineName, switchoverPhase)
}

// EngineFrontendSuspend suspends the engine frontend instance
func (p *EngineFrontendProxy) EngineFrontendSuspend(ef *longhorn.EngineFrontend) error {
	return p.imClient.EngineFrontendSuspend(ef.Spec.DataEngine, ef.Name)
}

// EngineFrontendResume resumes the engine frontend instance
func (p *EngineFrontendProxy) EngineFrontendResume(ef *longhorn.EngineFrontend) error {
	return p.imClient.EngineFrontendResume(ef.Spec.DataEngine, ef.Name)
}
