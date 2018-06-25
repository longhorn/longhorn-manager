package controller

import (
	"fmt"
	"strconv"

	"github.com/Sirupsen/logrus"

	pvController "github.com/kubernetes-incubator/external-storage/lib/controller"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/longhorn-manager/manager"
	"github.com/rancher/longhorn-manager/types"
)

const (
	LonghornProvisionerName = "rancher.io/longhorn"
	LonghornStorageClass    = "longhorn"
	LonghornDriver          = "rancher.io/longhorn"
)

type Provisioner struct {
	m *manager.VolumeManager
}

func NewProvisioner(m *manager.VolumeManager) pvController.Provisioner {
	return &Provisioner{
		m: m,
	}
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
	resourceStorage := opts.PVC.Spec.Resources.Requests[v1.ResourceName(v1.ResourceStorage)]
	size := resourceStorage.Value()
	numberOfReplicas, err := strconv.Atoi(opts.Parameters[types.OptionNumberOfReplica])
	if err != nil {
		return nil, err
	}
	staleReplicaTimeout, err := strconv.Atoi(opts.Parameters[types.OptionStaleReplicaTimeout])
	if err != nil {
		return nil, err
	}
	frontend := types.VolumeFrontend(opts.Parameters[types.OptionFrontend])
	if frontend == "" {
		frontend = types.VolumeFrontendBlockDev
	}
	spec := &types.VolumeSpec{
		Size:                size,
		Frontend:            frontend,
		FromBackup:          opts.Parameters[types.OptionFromBackup],
		NumberOfReplicas:    numberOfReplicas,
		StaleReplicaTimeout: staleReplicaTimeout,
	}
	v, err := p.m.Create(opts.PVName, spec)
	if err != nil {
		return nil, err
	}
	quantity := resource.NewQuantity(v.Spec.Size, resource.BinarySI)
	logrus.Info("provisioner: created volume %v", v.Name)
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
						types.OptionFromBackup:          v.Spec.FromBackup,
						types.OptionNumberOfReplica:     strconv.Itoa(v.Spec.NumberOfReplicas),
						types.OptionStaleReplicaTimeout: strconv.Itoa(v.Spec.StaleReplicaTimeout),
					},
				},
			},
		},
	}, nil
}

func (p *Provisioner) Delete(pv *v1.PersistentVolume) error {
	volume, err := p.m.Get(pv.Name)
	if err != nil {
		return err
	}
	if volume == nil {
		return nil
	}
	if volume.Spec.OwnerID != p.m.GetCurrentNodeID() {
		return &pvController.IgnoredError{"Not owned by current node"}
	}
	if pv.Spec.PersistentVolumeReclaimPolicy != v1.PersistentVolumeReclaimRetain {
		return p.m.Delete(pv.Name)
	}
	_, err = p.m.Detach(pv.Name)
	return err
}
