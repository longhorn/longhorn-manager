package engineapi

import (
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (p *Proxy) VolumeGet(e *longhorn.Engine) (volume *Volume, err error) {
	recv, err := p.grpcClient.VolumeGet(p.DirectToURL(e))
	if err != nil {
		return nil, err
	}

	return (*Volume)(recv), nil
}

func (p *Proxy) VolumeExpand(e *longhorn.Engine) (err error) {
	return p.grpcClient.VolumeExpand(p.DirectToURL(e), e.Spec.VolumeSize)
}
