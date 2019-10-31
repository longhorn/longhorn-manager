package manager

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

var (
	pvVolumeMode = apiv1.PersistentVolumeFilesystem
)

const (
	KubeStatusPollConut    = 5
	KubeStatusPollInterval = 1 * time.Second
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

	storageClassName, err := m.ds.GetSettingValueExisted(types.SettingNameDefaultLonghornStaticStorageClass)
	if err != nil {
		return nil, fmt.Errorf("failed to get longhorn static storage class name for PV %v creation: %v", pvName, err)
	}

	pv := NewPVManifest(v, pvName, storageClassName)
	pv, err = m.ds.CreatePersisentVolume(pv)
	if err != nil {
		return nil, err
	}
	created := false
	for i := 0; i < KubeStatusPollConut; i++ {
		v, err = m.ds.GetVolume(name)
		if err != nil {
			return nil, err
		}
		pvStatus := v.Status.KubernetesStatus.PVStatus
		if pvStatus != "" && pvStatus != string(apiv1.VolumePending) {
			created = true
			break
		}
		time.Sleep(KubeStatusPollInterval)
	}

	if !created {
		return v, fmt.Errorf("created PV %v is not in 'Available' status", pvName)
	}

	logrus.Debugf("Created PV for volume %v: %+v", v.Name, v.Spec)
	return v, nil
}

func NewPVManifest(v *longhorn.Volume, pvName, storageClassName string) *apiv1.PersistentVolume {
	diskSelector := strings.Join(v.Spec.DiskSelector, ",")
	nodeSelector := strings.Join(v.Spec.NodeSelector, ",")

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

			PersistentVolumeReclaimPolicy: apiv1.PersistentVolumeReclaimRetain,

			VolumeMode: &pvVolumeMode,

			StorageClassName: storageClassName,

			PersistentVolumeSource: apiv1.PersistentVolumeSource{
				CSI: &apiv1.CSIPersistentVolumeSource{
					Driver: types.LonghornDriverName,
					FSType: "ext4",
					VolumeAttributes: map[string]string{
						"diskSelector":        diskSelector,
						"nodeSelector":        nodeSelector,
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
		return nil, fmt.Errorf("no PVName set in volume %v kubernetes status %v", name, ks)
	}

	if ks.PVStatus != string(apiv1.VolumeAvailable) && ks.PVStatus != string(apiv1.VolumeReleased) {
		return nil, fmt.Errorf("cannot create PVC for PV %v since the PV status is %v", ks.PVName, ks.PVStatus)
	}

	pv, err := m.ds.GetPersisentVolume(ks.PVName)
	if err != nil {
		return nil, err
	}

	// cleanup ClaimRef of PV. Otherwise the existing PV cannot be reused.
	if pv.Spec.ClaimRef != nil {
		pv.Spec.ClaimRef = nil
		pv, err = m.ds.UpdatePersisentVolume(pv)
		if err != nil {
			return nil, err
		}
	}

	pvc := NewPVCManifest(v, ks.PVName, namespace, pvcName, pv.Spec.StorageClassName)
	pvc, err = m.ds.CreatePersisentVolumeClaim(namespace, pvc)
	if err != nil {
		return nil, err
	}

	created := false
	for i := 0; i < KubeStatusPollConut; i++ {
		v, err = m.ds.GetVolume(name)
		if err != nil {
			return nil, err
		}
		pvStatus := v.Status.KubernetesStatus.PVStatus
		if pvStatus == string(apiv1.VolumeBound) {
			created = true
			break
		}
		time.Sleep(KubeStatusPollInterval)
	}

	if !created {
		return v, fmt.Errorf("created PVC %v doesn't bound PV, PV status is %v", pvcName, v.Status.KubernetesStatus.PVStatus)
	}

	logrus.Debugf("Created PVC for volume %v: %+v", v.Name, v.Spec)
	return v, nil
}

func NewPVCManifest(v *longhorn.Volume, pvName, ns, pvcName, storageClassName string) *apiv1.PersistentVolumeClaim {
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
			StorageClassName: &storageClassName,
			VolumeName:       pvName,
		},
	}
}
