package app

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/urfave/cli"

	"k8s.io/client-go/rest"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeclientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const (
	FlagAll = "all"
)

func MigrateForPre070VolumesCmd() cli.Command {
	return cli.Command{
		Name: "migrate-for-pre-070-volumes",
		Flags: []cli.Flag{
			cli.BoolFlag{
				Name:  FlagAll,
				Usage: "Check and migrate PVs and PVCs for all pre v0.7.0 volumes",
			},
		},
		Action: func(c *cli.Context) {
			if err := migrateForPre070Volumes(c); err != nil {
				logrus.Fatalf("Error migrate PVs and PVCs for the volumes: %v", err)
			}
		},
	}
}

func migrateForPre070Volumes(c *cli.Context) error {
	var err error
	migrateAllVolumes := c.Bool(FlagAll)

	lhNamespace := os.Getenv(types.EnvPodNamespace)
	if lhNamespace == "" {
		return fmt.Errorf("failed to detect pod namespace, environment variable %v is missing", types.EnvPodNamespace)
	}

	config, err := rest.InClusterConfig()
	if err != nil {
		return errors.Wrap(err, "failed to get client config")
	}
	kubeClient, err := kubeclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "failed to get k8s client")
	}
	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return errors.Wrap(err, "failed to get clientset")
	}

	if migrateAllVolumes {
		vs, err := lhClient.LonghornV1beta2().Volumes(lhNamespace).List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			return err
		}
		for _, v := range vs.Items {
			if err = migratePVAndPVCForPre070Volume(kubeClient, lhClient, lhNamespace, v.Name); err != nil {
				return err
			}
		}
	} else {
		if c.NArg() == 0 {
			return errors.New("volume name or the flag '--all' is required")
		}
		if err = migratePVAndPVCForPre070Volume(kubeClient, lhClient, lhNamespace, c.Args()[0]); err != nil {
			return err
		}
	}

	return nil
}

func migratePVAndPVCForPre070Volume(kubeClient *kubeclientset.Clientset, lhClient *lhclientset.Clientset, lhNamespace, volumeName string) (err error) {
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "Failed to migrate PV and PVC for the volume %v", volumeName)
		}
	}()

	v, err := lhClient.LonghornV1beta2().Volumes(lhNamespace).Get(context.TODO(), volumeName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	ks := v.Status.KubernetesStatus

	if v.Status.State != longhorn.VolumeStateDetached {
		logrus.Infof("Invalid state %v for migrating volume %v", v.Status.State, volumeName)
		return nil
	}
	if ks.PVName == "" {
		logrus.Infof("There is no need to do migration for volume %v: no related PV", volumeName)
		return nil
	}
	if len(ks.WorkloadsStatus) != 0 && ks.LastPodRefAt == "" {
		logrus.Infof("There are still running workloads using the volume %v", volumeName)
		return nil
	}

	oldPV, err := kubeClient.CoreV1().PersistentVolumes().Get(context.TODO(), ks.PVName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if oldPV.Spec.CSI == nil || oldPV.Spec.CSI.Driver != types.DepracatedDriverName {
		logrus.Infof(" There is no need to migrate PV and PVC for volume %v: the PV is not created by the old Longhorn", volumeName)
		return nil
	}

	// Recreate PVC and PV without using CSI/StorageClass can make things easier:
	// 1) The goal of the volume migration is deprecating the old CSI components in Longhorn system.
	// As long as the data is retained and the volume can be controlled by the new CSI components,
	// the inconsistency of some PVC/PV fields do not matter.
	// 2) The StorageClass is used for volume initialization and setting some immutable fields in PV/PVC.
	// But currently the volume already exists and the parameters may be different from those immutable field.
	// Hence we don't need to make sure that those fields in the new PV/PVC are the same as the old ones.
	// pv.Spec.CSI.VolumeAttributes
	// 3) If we choose to use the StorageClass, we need to make sure:
	//     1) the StorageClass used by the old PV and PVC still exists;
	//     2) its parameters are the same as oldPV.Spec.CSI.VolumeAttributes.
	staticStorageClass, err := lhClient.LonghornV1beta2().Settings(lhNamespace).Get(
		context.TODO(),
		string(types.SettingNameDefaultLonghornStaticStorageClass), metav1.GetOptions{})
	if err != nil {
		return err
	}
	if staticStorageClass.Value == "" {
		return fmt.Errorf("empty static StorageClass for new PV/PVC creation")
	}

	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "failed to delete then recreate PV/PVC, users need to manually check the current PVC/PV then recreate them if needed")
		}
	}()

	if oldPV.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimRetain {
		oldPV.Spec.PersistentVolumeReclaimPolicy = corev1.PersistentVolumeReclaimRetain
		if oldPV, err = kubeClient.CoreV1().PersistentVolumes().Update(context.TODO(), oldPV, metav1.UpdateOptions{}); err != nil {
			return err
		}
	}

	pvcRecreationRequired := false
	var pvcName, namespace string
	if ks.PVCName != "" && ks.LastPVCRefAt == "" {
		pvcRecreationRequired = true
		pvcName = ks.PVCName
		namespace = ks.Namespace
		if err = kubeClient.CoreV1().PersistentVolumeClaims(namespace).Delete(context.TODO(), pvcName, metav1.DeleteOptions{}); err != nil {
			return err
		}
	}

	if err = kubeClient.CoreV1().PersistentVolumes().Delete(context.TODO(), ks.PVName, metav1.DeleteOptions{}); err != nil {
		return err
	}

	pvDeleted := false
	for i := 0; i < datastore.KubeStatusPollCount; i++ {
		v, err = lhClient.LonghornV1beta2().Volumes(lhNamespace).Get(context.TODO(), volumeName, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if v.Status.KubernetesStatus.PVName == "" {
			pvDeleted = true
			break
		}
		time.Sleep(datastore.KubeStatusPollInterval)
	}
	if !pvDeleted {
		return fmt.Errorf("failed to wait for the old PV deletion complete")
	}

	newPV := datastore.NewPVManifestForVolume(v, oldPV.Name, staticStorageClass.Value, oldPV.Spec.CSI.FSType)
	if _, err = kubeClient.CoreV1().PersistentVolumes().Create(context.TODO(), newPV, metav1.CreateOptions{}); err != nil {
		return err
	}

	if pvcRecreationRequired {
		pvc := datastore.NewPVCManifestForVolume(v, oldPV.Name, namespace, pvcName, staticStorageClass.Value)
		if _, err = kubeClient.CoreV1().PersistentVolumeClaims(namespace).Create(context.TODO(), pvc, metav1.CreateOptions{}); err != nil {
			return err
		}
	}

	logrus.Infof("Successfully migrated PV and PVC for volume %v", volumeName)

	return nil
}
