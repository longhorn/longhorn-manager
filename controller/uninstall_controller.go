package controller

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	v1 "k8s.io/api/core/v1"
	apiextension "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsv1 "k8s.io/client-go/informers/apps/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

const (
	CRDEngineName          = "engines.longhorn.io"
	CRDReplicaName         = "replicas.longhorn.io"
	CRDVolumeName          = "volumes.longhorn.io"
	CRDEngineImageName     = "engineimages.longhorn.io"
	CRDNodeName            = "nodes.longhorn.io"
	CRDInstanceManagerName = "instancemanagers.longhorn.io"
)

var (
	gracePeriod = 90 * time.Second
)

type UninstallController struct {
	*baseController
	namespace string
	force     bool
	ds        *datastore.DataStore
	stopCh    chan struct{}

	cacheSyncs []cache.InformerSynced
}

func NewUninstallController(
	logger logrus.FieldLogger,
	namespace string,
	force bool,
	ds *datastore.DataStore,
	stopCh chan struct{},
	extensionsClient apiextension.Interface,
	volumeInformer lhinformers.VolumeInformer,
	engineInformer lhinformers.EngineInformer,
	replicaInformer lhinformers.ReplicaInformer,
	engineImageInformer lhinformers.EngineImageInformer,
	nodeInformer lhinformers.NodeInformer,
	imInformer lhinformers.InstanceManagerInformer,
	daemonSetInformer appsv1.DaemonSetInformer,
) *UninstallController {
	c := &UninstallController{
		baseController: newBaseControllerWithQueue("longhorn-uninstall", logger,
			workqueue.NewNamedRateLimitingQueue(workqueue.NewMaxOfRateLimiter(
				workqueue.NewItemExponentialFailureRateLimiter(500*time.Millisecond, 5*time.Second),
				&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
			), "longhorn-uninstall"),
		),
		namespace: namespace,
		force:     force,
		ds:        ds,
		stopCh:    stopCh,
	}

	cacheSyncs := []cache.InformerSynced{}
	cacheSyncs = append(cacheSyncs, daemonSetInformer.Informer().HasSynced)

	if _, err := extensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(CRDEngineName, metav1.GetOptions{}); err == nil {
		engineInformer.Informer().AddEventHandler(c.controlleeHandler())
		cacheSyncs = append(cacheSyncs, engineInformer.Informer().HasSynced)
	}
	if _, err := extensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(CRDReplicaName, metav1.GetOptions{}); err == nil {
		replicaInformer.Informer().AddEventHandler(c.controlleeHandler())
		cacheSyncs = append(cacheSyncs, replicaInformer.Informer().HasSynced)
	}
	if _, err := extensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(CRDVolumeName, metav1.GetOptions{}); err == nil {
		volumeInformer.Informer().AddEventHandler(c.controlleeHandler())
		cacheSyncs = append(cacheSyncs, volumeInformer.Informer().HasSynced)
	}
	if _, err := extensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(CRDEngineImageName, metav1.GetOptions{}); err == nil {
		engineImageInformer.Informer().AddEventHandler(c.controlleeHandler())
		cacheSyncs = append(cacheSyncs, engineImageInformer.Informer().HasSynced)
	}
	if _, err := extensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(CRDNodeName, metav1.GetOptions{}); err == nil {
		nodeInformer.Informer().AddEventHandler(c.controlleeHandler())
		cacheSyncs = append(cacheSyncs, nodeInformer.Informer().HasSynced)
	}

	if _, err := extensionsClient.ApiextensionsV1beta1().CustomResourceDefinitions().Get(CRDInstanceManagerName, metav1.GetOptions{}); err == nil {
		imInformer.Informer().AddEventHandler(c.controlleeHandler())
		cacheSyncs = append(cacheSyncs, imInformer.Informer().HasSynced)
	}

	daemonSetInformer.Informer().AddEventHandler(c.controlleeHandler())

	c.cacheSyncs = cacheSyncs

	return c
}

func (c *UninstallController) controlleeHandler() cache.ResourceEventHandler {
	return cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { c.enqueueControlleeChange() },
		UpdateFunc: func(old, cur interface{}) { c.enqueueControlleeChange() },
		DeleteFunc: func(obj interface{}) { c.enqueueControlleeChange() },
	}
}

func (c *UninstallController) enqueueControlleeChange() {
	c.queue.AddRateLimited("uninstall")
}

func (c *UninstallController) Run() error {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	if !controller.WaitForCacheSync("longhorn uninstall", c.stopCh, c.cacheSyncs...) {
		return fmt.Errorf("Failed to sync informers")
	}

	if err := c.checkPreconditions(); err != nil {
		close(c.stopCh)
		return err
	}

	startTime := time.Now()
	logrus.Infof("Uninstalling...")
	defer func() {
		fields := logrus.Fields{"runtime": time.Now().Sub(startTime)}
		logrus.WithFields(fields).Infof("Uninstallation completed.")
	}()
	go wait.Until(c.worker, time.Second, c.stopCh)

	<-c.stopCh
	return nil
}

func (c *UninstallController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *UninstallController) processNextWorkItem() bool {
	key, quit := c.queue.Get()

	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.uninstall()
	c.handleErr(err, key)

	return true
}

func (c *UninstallController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	logrus.Warn(errors.Wrapf(err, "worker error"))
	c.queue.AddRateLimited(key)
}

func (c *UninstallController) uninstall() error {
	if ready, err := c.managerReady(); err != nil {
		return err
	} else if ready {
		if waitForUpdate, err := c.deleteCRDs(); err != nil || waitForUpdate {
			return err
		}
	}

	// A race condition exists where manager may attempt to recreate certain
	// CRDs after their deletion. We must delete manager and do a final check
	// for CRDs, just in case.
	if waitForUpdate, err := c.deleteManager(); err != nil || waitForUpdate {
		return err
	}

	if waitForUpdate, err := c.deleteDriver(); err != nil || waitForUpdate {
		return err
	}

	// We set gracePeriod=0s because there is no possibility of graceful
	// cleanup without a running manager.
	gracePeriod = 0 * time.Second
	if waitForUpdate, err := c.deleteCRDs(); err != nil || waitForUpdate {
		return err
	}

	// Success
	close(c.stopCh)
	return nil
}

func (c *UninstallController) checkPreconditions() error {
	if ready, err := c.managerReady(); err != nil {
		return err
	} else if !ready {
		if c.force {
			logrus.Warnf("Manager not ready, this may leave data behind.")
			gracePeriod = 0 * time.Second
		} else {
			return fmt.Errorf("manager not ready, set --force to continue")
		}
	}

	if vols, err := c.ds.ListVolumesRO(); err != nil {
		return err
	} else if len(vols) > 0 {
		volumesInUse := false
		for _, vol := range vols {
			logFields := logrus.Fields{
				"type": "volume",
			}
			if vol.Status.State == types.VolumeStateAttaching ||
				vol.Status.State == types.VolumeStateAttached {

				logFields["name"] = vol.Name
				logFields["state"] = vol.Status.State
				logrus.WithFields(logFields).Warnf("Volume in use")
				volumesInUse = true
			}
		}
		if volumesInUse && !c.force {
			return fmt.Errorf("volume(s) in use, set --force to continue")
		}
	}

	return nil
}

func (c *UninstallController) deleteCRDs() (bool, error) {
	if volumes, err := c.ds.ListVolumes(); err != nil {
		return true, err
	} else if len(volumes) > 0 {
		logrus.Infof("%d volumes remaining", len(volumes))
		return true, c.deleteVolumes(volumes)
	}

	if engines, err := c.ds.ListEngines(); err != nil {
		return true, err
	} else if len(engines) > 0 {
		logrus.Infof("%d engines remaining", len(engines))
		return true, c.deleteEngines(engines)
	}

	if replicas, err := c.ds.ListReplicas(); err != nil {
		return true, err
	} else if len(replicas) > 0 {
		logrus.Infof("%d replicas remaining", len(replicas))
		return true, c.deleteReplicas(replicas)
	}

	if engineImages, err := c.ds.ListEngineImages(); err != nil {
		return true, err
	} else if len(engineImages) > 0 {
		logrus.Infof("%d engineimages remaining", len(engineImages))
		return true, c.deleteEngineImages(engineImages)
	}

	if nodes, err := c.ds.ListNodes(); err != nil {
		return true, err
	} else if len(nodes) > 0 {
		logrus.Infof("%d nodes remaining", len(nodes))
		return true, c.deleteNodes(nodes)
	}

	if instanceManagers, err := c.ds.ListInstanceManagers(); err != nil {
		return true, err
	} else if len(instanceManagers) > 0 {
		logrus.Infof("%d instance managers remaining", len(instanceManagers))
		return true, c.deleteInstanceManagers(instanceManagers)
	}

	return false, nil
}

func (c *UninstallController) deleteVolumes(vols map[string]*longhorn.Volume) (err error) {
	defer func() {
		err = errors.Wrapf(err, "Failed to delete volumes")
	}()
	for _, vol := range vols {
		logFields := logrus.Fields{
			"name": vol.Name,
			"type": "volume",
		}
		timeout := metav1.NewTime(time.Now().Add(-gracePeriod))
		if vol.DeletionTimestamp == nil {
			if err = c.ds.DeleteVolume(vol.Name); err != nil {
				err = errors.Wrapf(err, "Failed to mark for deletion")
				return
			}
			logrus.WithFields(logFields).Info("Marked for deletion")
		} else if vol.DeletionTimestamp.Before(&timeout) {
			if err = c.ds.RemoveFinalizerForVolume(vol); err != nil {
				err = errors.Wrapf(err, "Failed to remove finalizer")
				return
			}
			logrus.WithFields(logFields).Info("Removed finalizer")
		}
	}
	return
}

func (c *UninstallController) deleteEngines(engines map[string]*longhorn.Engine) (err error) {
	defer func() {
		err = errors.Wrapf(err, "Failed to delete engines")
	}()
	for _, engine := range engines {
		logFields := logrus.Fields{
			"name": engine.Name,
			"type": "engine",
		}
		timeout := metav1.NewTime(time.Now().Add(-gracePeriod))
		if engine.DeletionTimestamp == nil {
			if err = c.ds.DeleteEngine(engine.Name); err != nil {
				err = errors.Wrapf(err, "Failed to mark for deletion")
				return
			}
			logrus.WithFields(logFields).Infof("Marked for deletion")
		} else if engine.DeletionTimestamp.Before(&timeout) {
			if err = c.ds.RemoveFinalizerForEngine(engine); err != nil {
				err = errors.Wrapf(err, "Failed to remove finalizer")
				return
			}
			logrus.WithFields(logFields).Infof("Removed finalizer")
		}
	}
	return
}

func (c *UninstallController) deleteReplicas(replicas map[string]*longhorn.Replica) (err error) {
	defer func() {
		err = errors.Wrapf(err, "Failed to delete replicas")
	}()
	for _, replica := range replicas {
		logFields := logrus.Fields{
			"name": replica.Name,
			"type": "replica",
		}
		timeout := metav1.NewTime(time.Now().Add(-gracePeriod))
		if replica.DeletionTimestamp == nil {
			if err = c.ds.DeleteReplica(replica.Name); err != nil {
				err = errors.Wrapf(err, "Failed to mark for deletion")
				return
			}
			logrus.WithFields(logFields).Infof("Marked for deletion")
		} else if replica.DeletionTimestamp.Before(&timeout) {
			if err = c.ds.RemoveFinalizerForReplica(replica); err != nil {
				err = errors.Wrapf(err, "Failed to remove finalizer")
				return
			}
			logrus.WithFields(logFields).Infof("Removed finalizer")
		}
	}
	return
}

func (c *UninstallController) deleteEngineImages(engineImages map[string]*longhorn.EngineImage) (err error) {
	defer func() {
		err = errors.Wrapf(err, "Failed to delete engine images")
	}()
	for _, ei := range engineImages {
		logFields := logrus.Fields{
			"type": "engineImage",
			"name": ei.Name,
		}
		timeout := metav1.NewTime(time.Now().Add(-gracePeriod))
		if ei.DeletionTimestamp == nil {
			if err = c.ds.DeleteEngineImage(ei.Name); err != nil {
				err = errors.Wrapf(err, "Failed to mark for deletion")
				return
			}
			logrus.WithFields(logFields).Infof("Marked for deletion")
		} else if ei.DeletionTimestamp.Before(&timeout) {
			dsName := types.GetDaemonSetNameFromEngineImageName(ei.Name)
			if err = c.ds.DeleteDaemonSet(dsName); err != nil {
				if !apierrors.IsNotFound(err) {
					err = errors.Wrapf(err, "Failed to remove daemon set")
					return
				}
				logrus.WithFields(logFields).Infof("Removed daemon set")
				err = nil
			}
			if err = c.ds.RemoveFinalizerForEngineImage(ei); err != nil {
				err = errors.Wrapf(err, "Failed to remove finalizer")
				return
			}
			logrus.WithFields(logFields).Infof("Removed finalizer")
		}
	}
	return
}

func (c *UninstallController) deleteNodes(nodes map[string]*longhorn.Node) (err error) {
	defer func() {
		err = errors.Wrapf(err, "Failed to delete nodes")
	}()
	for _, node := range nodes {
		logFields := logrus.Fields{
			"type": "node",
			"name": node.Name,
		}
		if node.DeletionTimestamp == nil {
			if err = c.ds.DeleteNode(node.Name); err != nil {
				err = errors.Wrapf(err, "Failed to mark for deletion")
				return
			}
			logrus.WithFields(logFields).Infof("Marked for deletion")
		} else {
			if err = c.ds.RemoveFinalizerForNode(node); err != nil {
				err = errors.Wrapf(err, "Failed to remove finalizer")
				return
			}
			logrus.WithFields(logFields).Infof("Removed finalizer")
		}
	}
	return
}

func (c *UninstallController) deleteInstanceManagers(instanceManagers map[string]*longhorn.InstanceManager) (err error) {
	defer func() {
		err = errors.Wrapf(err, "Failed to delete instance managers")
	}()
	for _, im := range instanceManagers {
		logFields := logrus.Fields{
			"type": "instanceManager",
			"name": im.Name,
		}
		timeout := metav1.NewTime(time.Now().Add(-gracePeriod))
		if im.DeletionTimestamp == nil {
			if err = c.ds.DeleteInstanceManager(im.Name); err != nil {
				err = errors.Wrapf(err, "Failed to mark for deletion")
				return
			}
			logrus.WithFields(logFields).Infof("Marked for deletion")
		} else if im.DeletionTimestamp.Before(&timeout) {
			var pod *v1.Pod
			pod, err = c.ds.GetInstanceManagerPod(im.Name)
			if err != nil {
				err = errors.Wrapf(err, "Could not get pod for instance manager")
			}
			if pod != nil {
				if err = c.ds.DeletePod(pod.Name); err != nil {
					return
				}
				logrus.WithFields(logFields).Infof("Removed instance manager")
			}

			if err = c.ds.RemoveFinalizerForInstanceManager(im); err != nil {
				err = errors.Wrapf(err, "Failed to remove finalizer")
				return
			}
			logrus.WithFields(logFields).Infof("Removed finalizer")
		}
	}
	return
}

func (c *UninstallController) deleteManager() (bool, error) {
	if ds, err := c.ds.GetDaemonSet(types.LonghornManagerDaemonSetName); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return true, err
	} else if ds.DeletionTimestamp == nil {
		if err := c.ds.DeleteDaemonSet(types.LonghornManagerDaemonSetName); err != nil {
			logrus.Warn("failed to mark manager for deletion")
			return true, err
		}
		logrus.Info("marked manager for deletion")
		return true, nil
	}
	logrus.Info("manager already marked for deletion")
	return true, nil
}

func (c *UninstallController) managerReady() (bool, error) {
	if ds, err := c.ds.GetDaemonSet(types.LonghornManagerDaemonSetName); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, err
	} else if ds.DeletionTimestamp != nil {
		logrus.Warn("Manager marked for deletion")
		return false, nil
	} else if ds.Status.NumberReady < ds.Status.DesiredNumberScheduled-1 {
		// During upgrade, there may be at most one pod missing, so we
		// will allow that to support uninstallation during upgrade
		logrus.Warnf("Manager not enough ready pods (%d/%d)",
			ds.Status.NumberReady, ds.Status.DesiredNumberScheduled)
		return false, nil
	}
	return true, nil
}

func (c *UninstallController) deleteDriver() (bool, error) {
	deploymentsToClean := []string{
		types.DriverDeployerName,
		types.CSIAttacherName,
		types.CSIProvisionerName,
		types.CSIResizerName,
	}
	wait := false
	for _, name := range deploymentsToClean {
		if driver, err := c.ds.GetDeployment(name); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			logrus.Warnf("failed to get %v for deletion: %v", name, err)
			wait = true
			continue
		} else if driver.DeletionTimestamp == nil {
			if err := c.ds.DeleteDeployment(name); err != nil {
				logrus.Warnf("failed to mark %v for deletion: %v", name, err)
				wait = true
				continue
			}
			logrus.Infof("marked %v for deletion", name)
			wait = true
			continue
		}
		logrus.Infof("%v already marked for deletion", name)
		wait = true
	}
	daemonSetsToClean := []string{
		types.CSIPluginName,
	}
	for _, name := range daemonSetsToClean {
		if driver, err := c.ds.GetDaemonSet(name); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			logrus.Warnf("failed to get %v for deletion: %v", name, err)
			wait = true
			continue
		} else if driver.DeletionTimestamp == nil {
			if err := c.ds.DeleteDaemonSet(name); err != nil {
				logrus.Warnf("failed to mark %v for deletion: %v", name, err)
				wait = true
				continue
			}
			logrus.Infof("marked %v for deletion", name)
			wait = true
			continue
		}
		logrus.Infof("%v already marked for deletion", name)
		wait = true
	}

	if err := c.ds.DeleteCSIDriver(types.LonghornDriverName); err != nil {
		if !apierrors.IsNotFound(err) {
			logrus.Warnf("failed to delete CSIDriver object %v: %v", types.LonghornDriverName, err)
			wait = true
		}
	}

	return wait, nil
}
