package controller

import (
	"fmt"
	"strconv"

	"github.com/Sirupsen/logrus"
	pvController "github.com/kubernetes-incubator/external-storage/lib/controller"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"

	longhornclient "github.com/rancher/longhorn-manager/client"
	"github.com/rancher/longhorn-manager/types"
)

const (
	LonghornProvisionerName = "rancher.io/longhorn"
	LonghornStorageClass    = "longhorn"
	LonghornDriver          = "rancher.io/longhorn"
)

type Provisioner struct {
	apiClient *longhornclient.RancherClient
}

func NewProvisioner(apiClient *longhornclient.RancherClient) pvController.Provisioner {
	return &Provisioner{apiClient}
}

func (p *Provisioner) Provision(opts pvController.VolumeOptions) (*v1.PersistentVolume, error) {
	pvc := opts.PVC
	if pvc.Spec.Selector != nil {
		return nil, fmt.Errorf("claim.Spec.Selector is not supported")
	}
	rwRequired := false
	for _, accessMode := range pvc.Spec.AccessModes {
		if accessMode == v1.ReadWriteMany {
			rwRequired = true
			break
		}
	}
	if rwRequired {
		return nil, fmt.Errorf("ReadWriteMany access mode is not supported")
	}
	resourceStorage := pvc.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	numberOfReplicasParam := types.DefaultNumberOfReplicas
	if _, ok := opts.Parameters[types.OptionNumberOfReplicas]; ok {
		numberOfReplicasParam = opts.Parameters[types.OptionNumberOfReplicas]
	}
	numberOfReplicas, err := strconv.Atoi(numberOfReplicasParam)
	if err != nil {
		return nil, err
	}

	optionStaleReplicaTimeoutParam := types.DefaultStaleReplicaTimeout
	if _, ok := opts.Parameters[types.OptionStaleReplicaTimeout]; ok {
		optionStaleReplicaTimeoutParam = opts.Parameters[types.OptionStaleReplicaTimeout]
	}
	staleReplicaTimeout, err := strconv.Atoi(optionStaleReplicaTimeoutParam)
	if err != nil {
		return nil, err
	}
	frontend := types.VolumeFrontend(opts.Parameters[types.OptionFrontend])
	if frontend == "" {
		frontend = types.VolumeFrontendBlockDev
	}
	sizeGiB := volumeutil.RoundUpToGiB(resourceStorage)
	volReq := &longhornclient.Volume{
		Name:                opts.PVName,
		Size:                fmt.Sprintf("%dGi", sizeGiB),
		Frontend:            string(frontend),
		FromBackup:          opts.Parameters[types.OptionFromBackup],
		NumberOfReplicas:    int64(numberOfReplicas),
		StaleReplicaTimeout: int64(staleReplicaTimeout),
	}
	v, err := p.apiClient.Volume.Create(volReq)
	if err != nil {
		return nil, err
	}
	logrus.Infof("provisioner: created volume %v, size: %dGi", v.Name, sizeGiB)
	quantity := resource.NewQuantity(sizeGiB*volumeutil.GIB, resource.BinarySI)

	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: v.Name,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: opts.PersistentVolumeReclaimPolicy,
			AccessModes:                   pvc.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): *quantity,
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				FlexVolume: &v1.FlexPersistentVolumeSource{
					Driver: LonghornDriver,
					FSType: opts.Parameters["fsType"],
					Options: map[string]string{
						types.OptionFromBackup:          v.FromBackup,
						types.OptionNumberOfReplicas:    strconv.FormatInt(v.NumberOfReplicas, 10),
						types.OptionStaleReplicaTimeout: strconv.FormatInt(v.StaleReplicaTimeout, 10),
					},
				},
			},
		},
	}, nil
}

func (p *Provisioner) Delete(pv *v1.PersistentVolume) error {
	volume, err := p.apiClient.Volume.ById(pv.Name)
	if err != nil {
		return err
	}
	if volume == nil {
		return nil
	}

	if pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain {
		logrus.Infof("provisioner: delete volume %v", volume.Name)
		return p.apiClient.Volume.Delete(volume)
	}
	logrus.Infof("provisioner: detach volume %v", volume.Name)
	_, err = p.apiClient.Volume.ActionDetach(volume)
	return err
}
