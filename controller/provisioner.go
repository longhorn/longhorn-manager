package controller

import (
	"fmt"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

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
	spec := &types.VolumeSpec{
		Size:                strconv.FormatInt(size, 10),
		FromBackup:          opts.Parameters["fromBackup"],
		NumberOfReplicas:    3,  //opts.Parameters["numberOfReplicas"],
		StaleReplicaTimeout: 60, //opts.Parameters["StaleReplicaTimeout"],
	}
	v, err := p.m.Create(opts.PVName, spec)
	if err != nil {
		return nil, err
	}
	quantity, err := resource.ParseQuantity(v.Spec.Size)
	if err != nil {
		return nil, errors.Wrapf(err, "BUG: cannot parse %v", v.Spec.Size)
	}
	logrus.Info("provisioner: created volume %v", v.Name)
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: v.Name,
		},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeReclaimPolicy: opts.PersistentVolumeReclaimPolicy,
			AccessModes:                   pvc.Spec.AccessModes,
			Capacity: v1.ResourceList{
				v1.ResourceName(v1.ResourceStorage): quantity,
			},
			PersistentVolumeSource: v1.PersistentVolumeSource{
				FlexVolume: &v1.FlexVolumeSource{
					Driver: LonghornDriver,
					FSType: opts.Parameters["fsType"],
					Options: map[string]string{
						"fromBackup":          v.Spec.FromBackup,
						"numberOfReplicas":    strconv.Itoa(v.Spec.NumberOfReplicas),
						"staleReplicaTimeout": strconv.Itoa(v.Spec.StaleReplicaTimeout),
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
