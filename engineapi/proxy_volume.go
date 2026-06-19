package engineapi

import (
	"fmt"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (p *Proxy) VolumeGet(e *longhorn.Engine) (volume *Volume, err error) {
	recv, err := p.grpcClient.VolumeGet(string(e.Spec.DataEngine), e.Name, "", e.Spec.VolumeName, p.DirectToURL(e))
	if err != nil {
		return nil, err
	}

	return (*Volume)(recv), nil
}

func getVolumeFrontendServiceObject(e *longhorn.Engine, ef *longhorn.EngineFrontend) DataEngineObject {
	if e != nil && ef != nil && types.IsDataEngineV2(e.Spec.DataEngine) {
		return ef
	}
	return e
}

// VolumeFrontendGet fetches volume info scoped to the given EngineFrontend.
// For v2, the instance-manager proxy overlays the EngineFrontend's size,
// endpoint, frontend and expansion state on top of the engine view, which is
// what the EngineFrontend controller needs to observe the frontend device.
// For v1 (or when ef is nil) it is equivalent to VolumeGet.
func (p *Proxy) VolumeFrontendGet(e *longhorn.Engine, ef *longhorn.EngineFrontend) (volume *Volume, err error) {
	engineFrontendName := ""
	if ef != nil {
		engineFrontendName = ef.Name
	}
	serviceObj := getVolumeFrontendServiceObject(e, ef)

	recv, err := p.grpcClient.VolumeGet(string(e.Spec.DataEngine), e.Name, engineFrontendName, e.Spec.VolumeName, p.DirectToURL(serviceObj))
	if err != nil {
		return nil, err
	}

	return (*Volume)(recv), nil
}

func (p *Proxy) VolumeExpand(obj DataEngineObject, size int64) (err error) {
	return p.grpcClient.VolumeExpand(obj.GetDataEngine(), obj.GetEngineName(), obj.GetEngineFrontendName(), obj.GetVolumeName(), p.DirectToURL(obj), size)
}

func (p *Proxy) VolumeFrontendStart(e *longhorn.Engine) (err error) {
	frontendName, err := GetEngineInstanceFrontend(e.Spec.DataEngine, e.Spec.Frontend)
	if err != nil {
		return err
	}

	if frontendName == "" {
		return fmt.Errorf("cannot start empty frontend")
	}

	return p.grpcClient.VolumeFrontendStart(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e), frontendName)
}

func (p *Proxy) VolumeFrontendShutdown(e *longhorn.Engine) (err error) {
	return p.grpcClient.VolumeFrontendShutdown(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e))
}

func (p *Proxy) VolumeUnmapMarkSnapChainRemovedSet(e *longhorn.Engine) error {
	return p.grpcClient.VolumeUnmapMarkSnapChainRemovedSet(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e), e.Spec.UnmapMarkSnapChainRemovedEnabled)
}

func (p *Proxy) VolumeSnapshotMaxCountSet(e *longhorn.Engine) error {
	return p.grpcClient.VolumeSnapshotMaxCountSet(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e), e.Spec.SnapshotMaxCount)
}

func (p *Proxy) VolumeSnapshotMaxSizeSet(e *longhorn.Engine) error {
	return p.grpcClient.VolumeSnapshotMaxSizeSet(string(e.Spec.DataEngine), e.Name, e.Spec.VolumeName,
		p.DirectToURL(e), e.Spec.SnapshotMaxSize)
}

func (p *Proxy) RemountReadOnlyVolume(e *longhorn.Engine) error {
	return p.grpcClient.RemountReadOnlyVolume(e.Spec.VolumeName)
}
