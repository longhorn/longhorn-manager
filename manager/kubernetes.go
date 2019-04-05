package manager

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

var (
	pvVolumeMode = apiv1.PersistentVolumeFilesystem
)

func (m *VolumeManager) PVCreate(name, pvName string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to create PV for volume %v", name)
	}()

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}

	if v.Status.KubernetesStatus.PVName != "" {
		return v, fmt.Errorf("volume already had PV %v", v.Status.KubernetesStatus.PVName)
	}

	if pvName == "" {
		pvName = v.Name
	}

	pv := NewPVManifest(v, pvName)
	pv, err = m.ds.CreatePersisentVolume(pv)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("Created PV for volume %v: %+v", v.Name, v.Spec)
	return v, nil
}

func NewPVManifest(v *longhorn.Volume, pvName string) *apiv1.PersistentVolume {
	return &apiv1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: pvName,
		},
		Spec: apiv1.PersistentVolumeSpec{
			Capacity: apiv1.ResourceList{
				apiv1.ResourceStorage: *resource.NewQuantity(v.Spec.Size, resource.BinarySI),
			},
			AccessModes: []apiv1.PersistentVolumeAccessMode{
				apiv1.ReadWriteOnce,
			},

			PersistentVolumeReclaimPolicy: apiv1.PersistentVolumeReclaimDelete,

			VolumeMode: &pvVolumeMode,

			PersistentVolumeSource: apiv1.PersistentVolumeSource{
				CSI: &apiv1.CSIPersistentVolumeSource{
					Driver: "io.rancher.longhorn",
					FSType: "ext4",
					VolumeAttributes: map[string]string{
						"numberOfReplicas":    string(v.Spec.NumberOfReplicas),
						"staleReplicaTimeout": string(v.Spec.StaleReplicaTimeout),
					},
					VolumeHandle: v.Name,
				},
			},
		},
	}
}

func (m *VolumeManager) PVCCreate(name, namespace, pvcName string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to create PVC for volume %v", name)
	}()

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}

	ks := v.Status.KubernetesStatus

	if ks.LastPVCRefAt == "" && ks.PVCName != "" {
		return v, fmt.Errorf("volume already had PVC %v", ks.PVCName)
	}

	if pvcName == "" {
		pvcName = v.Name
	}

	if ks.PVName == "" {
		return nil, fmt.Errorf("connot find existing PV for volume %v in PVCCreate", name)
	}

	if ks.PVStatus != string(apiv1.VolumeAvailable) && ks.PVStatus != string(apiv1.VolumeReleased) {
		return nil, fmt.Errorf("cannot create PVC for PV %v since the PV status is %v", ks.PVName, ks.PVStatus)
	}

	pvc := NewPVCManifest(v, ks.PVName, namespace, pvcName)
	pvc, err = m.ds.CreatePersisentVolumeClaim(namespace, pvc)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("Created PVC for volume %v: %+v", v.Name, v.Spec)
	return v, nil
}

func NewPVCManifest(v *longhorn.Volume, pvName, ns, pvcName string) *apiv1.PersistentVolumeClaim {
	return &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      pvcName,
			Namespace: ns,
		},
		Spec: apiv1.PersistentVolumeClaimSpec{
			AccessModes: []apiv1.PersistentVolumeAccessMode{
				apiv1.ReadWriteOnce,
			},
			Resources: apiv1.ResourceRequirements{
				Requests: apiv1.ResourceList{
					apiv1.ResourceStorage: *resource.NewQuantity(v.Spec.Size, resource.BinarySI),
				},
			},
			VolumeName: pvName,
		},
	}
}
