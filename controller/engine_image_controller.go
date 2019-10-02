package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	appsinformers "k8s.io/client-go/informers/apps/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
)

var (
	ownerKindEngineImage = longhorn.SchemeGroupVersion.WithKind("EngineImage").String()

	ExpiredEngineImageTimeout = 60 * time.Minute
)

type EngineImageController struct {
	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the engine image
	controllerID   string
	serviceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	iStoreSynced  cache.InformerSynced
	vStoreSynced  cache.InformerSynced
	dsStoreSynced cache.InformerSynced
	nStoreSynced  cache.InformerSynced
	imStoreSynced cache.InformerSynced

	queue workqueue.RateLimitingInterface
}

func NewEngineImageController(
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	engineImageInformer lhinformers.EngineImageInformer,
	volumeInformer lhinformers.VolumeInformer,
	dsInformer appsinformers.DaemonSetInformer,
	nodeInformer lhinformers.NodeInformer,
	imInformer lhinformers.InstanceManagerInformer,
	kubeClient clientset.Interface,
	namespace string, controllerID, serviceAccount string) *EngineImageController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	ic := &EngineImageController{
		namespace:      namespace,
		controllerID:   controllerID,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-engine-image-controller"}),

		ds: ds,

		iStoreSynced:  engineImageInformer.Informer().HasSynced,
		vStoreSynced:  volumeInformer.Informer().HasSynced,
		dsStoreSynced: dsInformer.Informer().HasSynced,
		nStoreSynced:  nodeInformer.Informer().HasSynced,
		imStoreSynced: imInformer.Informer().HasSynced,

		queue: workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "longhorn-engine-image"),
	}

	engineImageInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			img := obj.(*longhorn.EngineImage)
			ic.enqueueEngineImage(img)
		},
		UpdateFunc: func(old, cur interface{}) {
			curImg := cur.(*longhorn.EngineImage)
			ic.enqueueEngineImage(curImg)
		},
		DeleteFunc: func(obj interface{}) {
			img := obj.(*longhorn.EngineImage)
			ic.enqueueEngineImage(img)
		},
	})

	volumeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			v := obj.(*longhorn.Volume)
			ic.enqueueVolumes(v)
		},
		UpdateFunc: func(old, cur interface{}) {
			oldV := old.(*longhorn.Volume)
			curV := cur.(*longhorn.Volume)
			ic.enqueueVolumes(oldV, curV)
		},
		DeleteFunc: func(obj interface{}) {
			v := obj.(*longhorn.Volume)
			ic.enqueueVolumes(v)
		},
	})

	dsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ic.enqueueControlleeChange(obj)
		},
		UpdateFunc: func(old, cur interface{}) {
			ic.enqueueControlleeChange(cur)
		},
		DeleteFunc: func(obj interface{}) {
			ic.enqueueControlleeChange(obj)
		},
	})

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ic.enqueueAllEngineImagesForNodeChange()
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			ic.enqueueAllEngineImagesForNodeChange()
		},
		DeleteFunc: func(obj interface{}) {
			ic.enqueueAllEngineImagesForNodeChange()
		},
	})

	imInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			im := obj.(*longhorn.InstanceManager)
			ic.enqueueInstanceManager(im)
		},
		UpdateFunc: func(old, cur interface{}) {
			curIM := cur.(*longhorn.InstanceManager)
			ic.enqueueInstanceManager(curIM)
		},
		DeleteFunc: func(obj interface{}) {
			im := obj.(*longhorn.InstanceManager)
			ic.enqueueInstanceManager(im)
		},
	})

	return ic
}

func (ic *EngineImageController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer ic.queue.ShutDown()

	logrus.Infof("Start Longhorn Engine Image controller")
	defer logrus.Infof("Shutting down Longhorn Engine Image controller")

	if !controller.WaitForCacheSync("longhorn engine images", stopCh, ic.iStoreSynced, ic.vStoreSynced, ic.dsStoreSynced, ic.nStoreSynced, ic.imStoreSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(ic.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (ic *EngineImageController) worker() {
	for ic.processNextWorkItem() {
	}
}

func (ic *EngineImageController) processNextWorkItem() bool {
	key, quit := ic.queue.Get()

	if quit {
		return false
	}
	defer ic.queue.Done(key)

	err := ic.syncEngineImage(key.(string))
	ic.handleErr(err, key)

	return true
}

func (ic *EngineImageController) handleErr(err error, key interface{}) {
	if err == nil {
		ic.queue.Forget(key)
		return
	}

	if ic.queue.NumRequeues(key) < maxRetries {
		logrus.Warnf("Error syncing Longhorn engine image %v: %v", key, err)
		ic.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	logrus.Warnf("Dropping Longhorn engine image %v out of the queue: %v", key, err)
	ic.queue.Forget(key)
}

func (ic *EngineImageController) syncEngineImage(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync engine image for %v", key)
	}()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != ic.namespace {
		// Not ours, don't do anything
		return nil
	}

	engineImage, err := ic.ds.GetEngineImage(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			logrus.Infof("Longhorn engine image %v has been deleted", key)
			return nil
		}
		return err
	}

	nodeStatusDown := false
	if engineImage.Spec.OwnerID != "" {
		nodeStatusDown, err = ic.ds.IsNodeDownOrDeleted(engineImage.Spec.OwnerID)
		if err != nil {
			logrus.Warnf("Found error while checking ownerID is down or deleted:%v", err)
		}
	}

	if engineImage.Spec.OwnerID == "" || nodeStatusDown {
		// Claim it
		engineImage.Spec.OwnerID = ic.controllerID
		engineImage, err = ic.ds.UpdateEngineImage(engineImage)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		logrus.Debugf("Engine Image Controller %v picked up %v (%v)", ic.controllerID, engineImage.Name, engineImage.Spec.Image)
	} else if engineImage.Spec.OwnerID != ic.controllerID {
		// Not ours
		return nil
	}

	checksumName := types.GetEngineImageChecksumName(engineImage.Spec.Image)
	if engineImage.Name != checksumName {
		return fmt.Errorf("Image %v checksum %v doesn't match engine image name %v", engineImage.Spec.Image, checksumName, engineImage.Name)
	}

	dsName := types.GetDaemonSetNameFromEngineImageName(engineImage.Name)
	if engineImage.DeletionTimestamp != nil {
		if err := ic.ds.DeleteDaemonSet(dsName); err != nil && !datastore.ErrorIsNotFound(err) {
			return errors.Wrapf(err, "cannot cleanup DaemonSet of engine image %v", engineImage.Name)
		}
		logrus.Infof("Removed DaemonSet %v for engine image %v (%v)", dsName, engineImage.Name, engineImage.Spec.Image)
		if err := ic.ds.DeleteInstanceManagersForEngineImage(engineImage.Name); err != nil {
			return errors.Wrapf(err, "cannot cleanup instance managers of engine image %v", engineImage.Name)
		}
		logrus.Infof("Removing related instance managers for engine image %v (%v)", engineImage.Name, engineImage.Spec.Image)
		return ic.ds.RemoveFinalizerForEngineImage(engineImage)
	}

	oldImageState := engineImage.Status.State
	existingEngineImage := engineImage.DeepCopy()
	defer func() {
		if err == nil && !reflect.DeepEqual(existingEngineImage, engineImage) {
			_, err = ic.ds.UpdateEngineImage(engineImage)
		}
		if apierrors.IsConflict(errors.Cause(err)) {
			logrus.Debugf("Requeue %v due to conflict", key)
			ic.enqueueEngineImage(engineImage)
			err = nil
		}
	}()

	ds, err := ic.ds.GetEngineImageDaemonSet(dsName)
	if err != nil {
		return errors.Wrapf(err, "cannot get daemonset for engine image %v", engineImage.Name)
	}
	if ds == nil {
		setting, err := ic.ds.GetSetting(types.SettingNameTaintToleration)
		if err != nil {
			return errors.Wrapf(err, "failed to get taint toleration setting before creating engine image daemonset")
		}
		tolerations, err := types.UnmarshalTolerations(setting.Value)
		if err != nil {
			return errors.Wrapf(err, "failed to unmarshal taint toleration setting before creating engine image daemonset")
		}

		dsSpec := ic.createEngineImageDaemonSetSpec(engineImage, tolerations)
		if err = ic.ds.CreateEngineImageDaemonSet(dsSpec); err != nil {
			return errors.Wrapf(err, "fail to create daemonset for engine image %v", engineImage.Name)
		}
		logrus.Infof("Created daemon set %v for engine image %v (%v)", dsSpec.Name, engineImage.Name, engineImage.Spec.Image)
		engineImage.Status.State = types.EngineImageStateDeploying
		return nil
	}

	if ds.Status.DesiredNumberScheduled == 0 {
		engineImage.Status.State = types.EngineImageStateDeploying
		return nil
	}

	nodes, err := ic.ds.ListNodes()
	if err != nil {
		return err
	}
	readyNodeCount := int32(0)
	readyNodeList := []*longhorn.Node{}
	for _, node := range nodes {
		condition := types.GetNodeConditionFromStatus(node.Status, types.NodeConditionTypeReady)
		if condition.Status == types.ConditionStatusTrue {
			readyNodeCount++
			readyNodeList = append(readyNodeList, node)
		}
	}
	if ds.Status.NumberAvailable < readyNodeCount {
		engineImage.Status.State = types.EngineImageStateDeploying
		return nil
	}

	if !types.EngineBinaryExistOnHostForImage(engineImage.Spec.Image) {
		engineImage.Status.State = types.EngineImageStateDeploying
		return nil
	}

	// will only become ready for the first time if all the following functions succeed
	engineImage.Status.State = types.EngineImageStateReady

	if err := ic.updateEngineImageVersion(engineImage); err != nil {
		return err
	}

	if err := engineapi.CheckCLICompatibilty(engineImage.Status.CLIAPIVersion, engineImage.Status.CLIAPIMinVersion); err != nil {
		engineImage.Status.State = types.EngineImageStateIncompatible
		// Allow update reference count and clean up even it's incompatible since engines may have been upgraded
	}

	if engineImage.Status.State != types.EngineImageStateIncompatible && !reflect.DeepEqual(types.GetEngineImageLabels(engineImage.Name), ds.Labels) {
		engineImage.Status.State = types.EngineImageStateDeploying
		// Cannot use update to correct the labels. The label update for the DaemonSet won’t be applied to the its pods.
		// The related issue: https://github.com/longhorn/longhorn/issues/769
		if err := ic.ds.DeleteDaemonSet(dsName); err != nil && !datastore.ErrorIsNotFound(err) {
			return errors.Wrapf(err, "cannot delete the DaemonSet with mismatching labels for engine image %v", engineImage.Name)
		}
		logrus.Infof("Removed DaemonSet %v with mismatching labels for engine image %v (%v). The DaemonSet with correct labels will be recreated automatically after deletion",
			dsName, engineImage.Name, engineImage.Spec.Image)
		return nil
	}

	if err := ic.syncInstanceManagers(engineImage, readyNodeList); err != nil {
		return err
	}

	if err := ic.updateEngineImageRefCount(engineImage); err != nil {
		return errors.Wrapf(err, "failed to update RefCount for engine image %v(%v)", engineImage.Name, engineImage.Spec.Image)
	}

	if err := ic.cleanupExpiredEngineImage(engineImage); err != nil {
		return err
	}

	if oldImageState != types.EngineImageStateReady && engineImage.Status.State == types.EngineImageStateReady &&
		engineImage.DeletionTimestamp == nil {
		logrus.Infof("Engine image %v (%v) become ready", engineImage.Name, engineImage.Spec.Image)
	}

	return nil
}

func (ic *EngineImageController) updateEngineImageVersion(ei *longhorn.EngineImage) error {
	engineCollection := &engineapi.EngineCollection{}
	// we're getting local longhorn engine version, don't need volume etc
	client, err := engineCollection.NewEngineClient(&engineapi.EngineClientRequest{
		EngineImage: ei.Spec.Image,
		VolumeName:  "",
		IP:          "",
		Port:        0,
	})
	if err != nil {
		return errors.Wrapf(err, "cannot get engine client to check engine version")
	}
	version, err := client.Version(true)
	if err != nil {
		logrus.Warnf("cannot get engine version for %v (%v): %v", ei.Name, ei.Spec.Image, err)
		version = &engineapi.EngineVersion{
			ClientVersion: &types.EngineVersionDetails{},
			ServerVersion: nil,
		}
		version.ClientVersion.Version = "ei.Spec.Image"
		version.ClientVersion.GitCommit = "unknown"
		version.ClientVersion.BuildDate = "unknown"
		version.ClientVersion.CLIAPIVersion = types.InvalidEngineVersion
		version.ClientVersion.CLIAPIMinVersion = types.InvalidEngineVersion
		version.ClientVersion.ControllerAPIVersion = types.InvalidEngineVersion
		version.ClientVersion.DataFormatVersion = types.InvalidEngineVersion
		version.ClientVersion.DataFormatMinVersion = types.InvalidEngineVersion
	}

	ei.Status.EngineVersionDetails = *version.ClientVersion
	return nil
}

func (ic *EngineImageController) updateEngineImageRefCount(ei *longhorn.EngineImage) error {
	volumes, err := ic.ds.ListVolumes()
	if err != nil {
		return errors.Wrap(err, "cannot list volumes when updateEngineImageRefCount")
	}
	image := ei.Spec.Image
	refCount := 0
	for _, v := range volumes {
		if v.Spec.EngineImage == image || v.Status.CurrentImage == image {
			refCount++
		}

		// Volume engine is still using the engine manager of this old engine image after live upgrading to other engine image.
		if v.Status.CurrentImage != image && v.Status.State == types.VolumeStateAttached {
			// For old version engine image, there is no related instance manager
			isCLIAPIVersionOne, err := ic.ds.IsEngineImageCLIAPIVersionOne(v.Status.CurrentImage)
			if err != nil {
				return err
			}
			if isCLIAPIVersionOne {
				continue
			}

			es, err := ic.ds.ListVolumeEngines(v.Name)
			if err != nil {
				return err
			}
			for _, e := range es {
				im, err := ic.ds.GetInstanceManager(e.Status.InstanceManagerName)
				if err != nil {
					return err
				}
				if ei.Name == im.Spec.EngineImage {
					refCount++
				}
			}
		}
	}
	ei.Status.RefCount = refCount
	if ei.Status.RefCount == 0 {
		if ei.Status.NoRefSince == "" {
			ei.Status.NoRefSince = util.Now()
		}
	} else {
		ei.Status.NoRefSince = ""
	}
	return nil

}

func (ic *EngineImageController) cleanupExpiredEngineImage(ei *longhorn.EngineImage) (err error) {
	defer func() {
		err = errors.Wrapf(err, "cannot cleanup engine image %v (%v)", ei.Name, ei.Spec.Image)
	}()

	if ei.Status.RefCount != 0 {
		return nil
	}
	if ei.Status.NoRefSince == "" {
		return nil
	}
	if util.TimestampAfterTimeout(ei.Status.NoRefSince, ExpiredEngineImageTimeout) {
		defaultEngineImage, err := ic.ds.GetSetting(types.SettingNameDefaultEngineImage)
		if err != nil {
			return err
		}
		if defaultEngineImage.Value == "" {
			return fmt.Errorf("default engine image not set")
		}
		// Don't delete the default image
		if ei.Spec.Image == defaultEngineImage.Value {
			return nil
		}

		logrus.Infof("Engine image %v (%v) expired, clean it up", ei.Name, ei.Spec.Image)
		if err := ic.ds.DeleteEngineImage(ei.Name); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (ic *EngineImageController) enqueueEngineImage(engineImage *longhorn.EngineImage) {
	key, err := controller.KeyFunc(engineImage)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", engineImage, err))
		return
	}

	ic.queue.AddRateLimited(key)
}

func (ic *EngineImageController) enqueueVolumes(volumes ...*longhorn.Volume) {
	images := map[string]struct{}{}
	for _, v := range volumes {
		if _, ok := images[v.Spec.EngineImage]; !ok {
			images[v.Spec.EngineImage] = struct{}{}
		}
		if v.Status.CurrentImage != "" {
			if _, ok := images[v.Status.CurrentImage]; !ok {
				images[v.Status.CurrentImage] = struct{}{}
			}
		}
	}

	for img := range images {
		engineImage, err := ic.ds.GetEngineImage(types.GetEngineImageChecksumName(img))
		if err != nil {
			continue
		}
		// Not ours
		if engineImage.Spec.OwnerID != ic.controllerID {
			continue
		}
		ic.enqueueEngineImage(engineImage)
	}
}

func (ic *EngineImageController) enqueueControlleeChange(obj interface{}) {
	metaObj, err := meta.Accessor(obj)
	if err != nil {
		logrus.Warnf("BUG: %v cannot be convert to metav1.Object: %v", obj, err)
		return
	}
	ownerRefs := metaObj.GetOwnerReferences()
	for _, ref := range ownerRefs {
		if ref.Kind != ownerKindEngineImage {
			continue
		}
		namespace := metaObj.GetNamespace()
		ic.ResolveRefAndEnqueue(namespace, &ref)
		return
	}
}

func (ic *EngineImageController) enqueueAllEngineImagesForNodeChange() {
	engineImages, err := ic.ds.ListEngineImages()
	if err != nil {
		logrus.Warnf("Failed to list engine images: %v", err)
		return
	}

	for _, ei := range engineImages {
		ic.enqueueEngineImage(ei)
	}
	return
}

func (ic *EngineImageController) enqueueInstanceManager(im *longhorn.InstanceManager) {
	ei, err := ic.ds.GetEngineImage(im.Spec.EngineImage)
	if err != nil {
		logrus.Errorf("failed to get the engine image %v when enqueuing instance manager change: %v", im.Spec.EngineImage, err)
		return
	}

	ic.enqueueEngineImage(ei)
	return
}

func (ic *EngineImageController) ResolveRefAndEnqueue(namespace string, ref *metav1.OwnerReference) {
	if ref.Kind != ownerKindEngineImage {
		return
	}
	engineImage, err := ic.ds.GetEngineImage(ref.Name)
	if err != nil {
		return
	}
	if engineImage.UID != ref.UID {
		// The controller we found with this Name is not the same one that the
		// OwnerRef points to.
		return
	}
	// Not ours
	if engineImage.Spec.OwnerID != ic.controllerID {
		return
	}
	ic.enqueueEngineImage(engineImage)
}

func (ic *EngineImageController) createEngineImageDaemonSetSpec(ei *longhorn.EngineImage, tolerations []v1.Toleration) *appsv1.DaemonSet {
	dsName := types.GetDaemonSetNameFromEngineImageName(ei.Name)
	image := ei.Spec.Image
	cmd := []string{
		"/bin/bash",
	}
	args := []string{
		"-c",
		"cp /usr/local/bin/longhorn* /data/ && echo installed && trap 'rm /data/longhorn* && echo cleaned up' EXIT && sleep infinity",
	}
	maxUnavailable := intstr.FromString(`100%`)
	d := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name: dsName,
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: types.GetEngineImageLabels(ei.Name),
			},
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
				Type: appsv1.RollingUpdateDaemonSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDaemonSet{
					MaxUnavailable: &maxUnavailable,
				},
			},
			Template: v1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   dsName,
					Labels: types.GetEngineImageLabels(ei.Name),
				},
				Spec: v1.PodSpec{
					ServiceAccountName: ic.serviceAccount,
					Tolerations:        tolerations,
					Containers: []v1.Container{
						{
							Name:            dsName,
							Image:           image,
							Command:         cmd,
							Args:            args,
							ImagePullPolicy: v1.PullAlways,
							VolumeMounts: []v1.VolumeMount{
								{
									Name:      "data",
									MountPath: "/data/",
								},
							},
							ReadinessProbe: &v1.Probe{
								Handler: v1.Handler{
									Exec: &v1.ExecAction{
										Command: []string{
											"ls", "/data/longhorn",
										},
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       5,
							},
						},
					},
					Volumes: []v1.Volume{
						{
							Name: "data",
							VolumeSource: v1.VolumeSource{
								HostPath: &v1.HostPathVolumeSource{
									Path: types.GetEngineBinaryDirectoryOnHostForImage(image),
								},
							},
						},
					},
				},
			},
		},
	}
	return d
}

func (ic *EngineImageController) syncInstanceManagers(ei *longhorn.EngineImage, readyNodeList []*longhorn.Node) error {
	for _, node := range readyNodeList {
		// Get the Instance Managers if they do exist, so we can handle them once we get the Instance Manager state.
		engineIM, err := ic.ds.GetInstanceManagerBySelector(node.Name, ei.Name, types.InstanceManagerTypeEngine)
		if err != nil {
			if !types.ErrorIsNotFound(err) {
				return errors.Wrapf(err, "cannot get engine instance manager for node %v, engine image %v", node.Name, ei.Name)

			}
			engineIM = nil
		}

		replicaIM, err := ic.ds.GetInstanceManagerBySelector(node.Name, ei.Name, types.InstanceManagerTypeReplica)
		if err != nil {
			if !types.ErrorIsNotFound(err) {
				return errors.Wrapf(err, "cannot get replica instance manager for node %v, engine image %v", node.Name, ei.Name)
			}
			replicaIM = nil
		}

		switch ei.Status.State {
		case types.EngineImageStateIncompatible:
			if engineIM != nil {
				if err := ic.ds.DeleteInstanceManager(engineIM.Name); err != nil {
					return errors.Wrapf(err, "cannot cleanup engine instance manager of node %v, engine image %v", node.Name, ei.Name)
				}
			}

			if replicaIM != nil {
				if err := ic.ds.DeleteInstanceManager(replicaIM.Name); err != nil {
					return errors.Wrapf(err, "cannot cleanup replica instance manager of node %v, engine image %v", node.Name, ei.Name)
				}
			}
		case types.EngineImageStateDeploying:
			fallthrough
		case types.EngineImageStateReady:
			if engineIM == nil {
				if _, err := ic.createInstanceManager(ei, types.InstanceManagerTypeEngine, node); err != nil {
					return errors.Wrapf(err, "failed to create engine instance manager for node %v, engine image %v", node.Name, ei.Name)
				}
				ei.Status.State = types.EngineImageStateDeploying
				logrus.Infof("Created engine instance manager for node %v, engine image %v (%v)", node.Name, ei.Name, ei.Spec.Image)
			} else {
				if engineIM.Status.CurrentState != types.InstanceManagerStateRunning {
					ei.Status.State = types.EngineImageStateDeploying
				}
			}

			if replicaIM == nil {
				if len(node.Spec.Disks) > 0 {
					if _, err := ic.createInstanceManager(ei, types.InstanceManagerTypeReplica, node); err != nil {
						return errors.Wrapf(err, "failed to create replica instance manager for node %v, engine image %v", node.Name, ei.Name)
					}
					ei.Status.State = types.EngineImageStateDeploying
					logrus.Infof("Created replica instance manager for node %v, engine image %v (%v)", node.Name, ei.Name, ei.Spec.Image)
				}
			} else {
				if len(node.Spec.Disks) == 0 {
					// If the Node no longer has any Disks on it, it's safe to delete the Replica Instance Manager since
					// its no longer needed.
					if err := ic.ds.DeleteInstanceManager(replicaIM.Name); err != nil {
						return errors.Wrapf(err, "cannot cleanup replica instance manager of node %v, engine image %v", node.Name, ei.Name)
					}
				} else {
					if replicaIM.Status.CurrentState != types.InstanceManagerStateRunning {
						ei.Status.State = types.EngineImageStateDeploying
					}
				}
			}
		default:
			return fmt.Errorf("BUG: engine image %v had unexpected state %v", ei.Name, ei.Status.State)
		}
	}

	return nil
}

func (ic *EngineImageController) createInstanceManager(ei *longhorn.EngineImage, imType types.InstanceManagerType,
	node *longhorn.Node) (*longhorn.InstanceManager, error) {

	var generateName string
	switch imType {
	case types.InstanceManagerTypeEngine:
		generateName = "instance-manager-e-"
	case types.InstanceManagerTypeReplica:
		generateName = "instance-manager-r-"
	}

	blockOwnerDeletion := true
	instanceManager := &longhorn.InstanceManager{
		ObjectMeta: metav1.ObjectMeta{
			// Even though the labels duplicate information already in the spec, spec cannot be used for
			// Field Selectors in CustomResourceDefinitions:
			// https://github.com/kubernetes/kubernetes/issues/53459
			Labels: types.GetInstanceManagerLabels(node.Name, ei.Name, imType),
			Name:   generateName + util.RandomID(),
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         longhorn.SchemeGroupVersion.String(),
					Kind:               longhorn.SchemeGroupVersion.WithKind("EngineImage").String(),
					Name:               ei.Name,
					UID:                ei.UID,
					BlockOwnerDeletion: &blockOwnerDeletion,
				},
			},
		},
		Spec: types.InstanceManagerSpec{
			EngineImage: ei.Name,
			NodeID:      node.Name,
			Type:        imType,
		},
		Status: types.InstanceManagerStatus{
			CurrentState: types.InstanceManagerStateStopped,
			Instances:    map[string]types.InstanceProcess{},
		},
	}

	return ic.ds.CreateInstanceManager(instanceManager)
}
