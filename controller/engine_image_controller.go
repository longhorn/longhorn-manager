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
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

var (
	ownerKindEngineImage = longhorn.SchemeGroupVersion.WithKind("EngineImage").String()

	ExpiredEngineImageTimeout = 60 * time.Minute
)

type EngineImageController struct {
	*baseController

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

	// for unit test
	nowHandler                func() string
	engineBinaryChecker       func(string) bool
	engineImageVersionUpdater func(*longhorn.EngineImage) error
}

func NewEngineImageController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	engineImageInformer lhinformers.EngineImageInformer,
	volumeInformer lhinformers.VolumeInformer,
	dsInformer appsinformers.DaemonSetInformer,
	kubeClient clientset.Interface,
	namespace string, controllerID, serviceAccount string) *EngineImageController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	ic := &EngineImageController{
		baseController: newBaseController("longhorn-engine-image", logger),

		namespace:      namespace,
		controllerID:   controllerID,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-engine-image-controller"}),

		ds: ds,

		iStoreSynced:  engineImageInformer.Informer().HasSynced,
		vStoreSynced:  volumeInformer.Informer().HasSynced,
		dsStoreSynced: dsInformer.Informer().HasSynced,

		nowHandler:                util.Now,
		engineBinaryChecker:       types.EngineBinaryExistOnHostForImage,
		engineImageVersionUpdater: updateEngineImageVersion,
	}

	engineImageInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ic.enqueueEngineImage,
		UpdateFunc: func(old, cur interface{}) { ic.enqueueEngineImage(cur) },
		DeleteFunc: ic.enqueueEngineImage,
	})

	volumeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) { ic.enqueueVolumes(obj) },
		UpdateFunc: func(old, cur interface{}) { ic.enqueueVolumes(old, cur) },
		DeleteFunc: func(obj interface{}) { ic.enqueueVolumes(obj) },
	})

	dsInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ic.enqueueControlleeChange,
		UpdateFunc: func(old, cur interface{}) { ic.enqueueControlleeChange(cur) },
		DeleteFunc: ic.enqueueControlleeChange,
	})

	return ic
}

func (ic *EngineImageController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer ic.queue.ShutDown()

	logrus.Infof("Start Longhorn Engine Image controller")
	defer logrus.Infof("Shutting down Longhorn Engine Image controller")

	if !controller.WaitForCacheSync("longhorn engine images", stopCh, ic.iStoreSynced, ic.vStoreSynced, ic.dsStoreSynced) {
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
	if engineImage.Status.OwnerID != "" {
		nodeStatusDown, err = ic.ds.IsNodeDownOrDeleted(engineImage.Status.OwnerID)
		if err != nil {
			logrus.Warnf("Found error while checking ownerID is down or deleted:%v", err)
		}
	}

	if engineImage.Status.OwnerID == "" || nodeStatusDown {
		// Claim it
		engineImage.Status.OwnerID = ic.controllerID
		engineImage, err = ic.ds.UpdateEngineImageStatus(engineImage)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		logrus.Debugf("Engine Image Controller %v picked up %v (%v)", ic.controllerID, engineImage.Name, engineImage.Spec.Image)
	} else if engineImage.Status.OwnerID != ic.controllerID {
		// Not ours
		return nil
	}

	checksumName := types.GetEngineImageChecksumName(engineImage.Spec.Image)
	if engineImage.Name != checksumName {
		return fmt.Errorf("Image %v checksum %v doesn't match engine image name %v", engineImage.Spec.Image, checksumName, engineImage.Name)
	}

	dsName := types.GetDaemonSetNameFromEngineImageName(engineImage.Name)
	if engineImage.DeletionTimestamp != nil {
		// Will use the foreground deletion to implicitly clean up the related DaemonSet.
		logrus.Infof("Removing engine image %v (%v)", engineImage.Name, engineImage.Spec.Image)
		return ic.ds.RemoveFinalizerForEngineImage(engineImage)
	}

	existingEngineImage := engineImage.DeepCopy()
	defer func() {
		if err == nil && !reflect.DeepEqual(existingEngineImage.Status, engineImage.Status) {
			_, err = ic.ds.UpdateEngineImageStatus(engineImage)
		}
		if apierrors.IsConflict(errors.Cause(err)) {
			logrus.Debugf("Requeue %v due to conflict: %v", key, err)
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

		priorityClassSetting, err := ic.ds.GetSetting(types.SettingNamePriorityClass)
		if err != nil {
			return errors.Wrapf(err, "failed to get priority class setting before creating engine image daemonset")
		}
		priorityClass := priorityClassSetting.Value

		registrySecretSetting, err := ic.ds.GetSetting(types.SettingNameRegistrySecret)
		if err != nil {
			return errors.Wrapf(err, "failed to get registry secret setting before creating engine image daemonset")
		}
		registrySecret := registrySecretSetting.Value

		imagePullPolicy, err := ic.ds.GetSettingImagePullPolicy()
		if err != nil {
			return errors.Wrapf(err, "failed to get system pods image pull policy before creating engine image daemonset")
		}

		dsSpec := ic.createEngineImageDaemonSetSpec(engineImage, tolerations, priorityClass, registrySecret, imagePullPolicy)

		if err = ic.ds.CreateEngineImageDaemonSet(dsSpec); err != nil {
			return errors.Wrapf(err, "fail to create daemonset for engine image %v", engineImage.Name)
		}
		logrus.Infof("Created daemon set %v for engine image %v (%v)", dsSpec.Name, engineImage.Name, engineImage.Spec.Image)
		engineImage.Status.Conditions = types.SetCondition(engineImage.Status.Conditions,
			types.EngineImageConditionTypeReady, types.ConditionStatusFalse,
			types.EngineImageConditionTypeReadyReasonDaemonSet, fmt.Sprintf("creating daemon set %v for %v", dsSpec.Name, engineImage.Spec.Image))
		engineImage.Status.State = types.EngineImageStateDeploying
		return nil
	}

	// TODO: Will remove this reference kind correcting after all Longhorn components having used the new kinds
	if len(ds.OwnerReferences) < 1 || ds.OwnerReferences[0].Kind != types.LonghornKindEngineImage {
		ds.OwnerReferences = datastore.GetOwnerReferencesForEngineImage(engineImage)
		ds, err = ic.kubeClient.AppsV1().DaemonSets(ic.namespace).Update(ds)
		if err != nil {
			return err
		}
	}

	if ds.Status.DesiredNumberScheduled == 0 {
		engineImage.Status.Conditions = types.SetCondition(engineImage.Status.Conditions,
			types.EngineImageConditionTypeReady, types.ConditionStatusFalse,
			types.EngineImageConditionTypeReadyReasonDaemonSet, "no pod scheduled")
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
		condition := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeReady)
		if condition.Status == types.ConditionStatusTrue {
			readyNodeCount++
			readyNodeList = append(readyNodeList, node)
		}
	}
	if ds.Status.NumberAvailable < readyNodeCount {
		engineImage.Status.Conditions = types.SetCondition(engineImage.Status.Conditions, types.EngineImageConditionTypeReady, types.ConditionStatusFalse,
			types.EngineImageConditionTypeReadyReasonDaemonSet, fmt.Sprintf("no enough pods are ready: %v vs %v", ds.Status.NumberAvailable, readyNodeCount))
		engineImage.Status.State = types.EngineImageStateDeploying
		return nil
	}

	if !ic.engineBinaryChecker(engineImage.Spec.Image) {
		engineImage.Status.Conditions = types.SetCondition(engineImage.Status.Conditions, types.EngineImageConditionTypeReady, types.ConditionStatusFalse, types.EngineImageConditionTypeReadyReasonDaemonSet, "engine binary check failed")
		engineImage.Status.State = types.EngineImageStateDeploying
		return nil
	}

	if err := ic.engineImageVersionUpdater(engineImage); err != nil {
		return err
	}

	// will only become ready for the first time if all the following functions succeed
	engineImage.Status.State = types.EngineImageStateReady

	if err := engineapi.CheckCLICompatibilty(engineImage.Status.CLIAPIVersion, engineImage.Status.CLIAPIMinVersion); err != nil {
		engineImage.Status.Conditions = types.SetCondition(engineImage.Status.Conditions, types.EngineImageConditionTypeReady, types.ConditionStatusFalse, types.EngineImageConditionTypeReadyReasonBinary, "incompatible")
		engineImage.Status.State = types.EngineImageStateIncompatible
		// Allow update reference count and clean up even it's incompatible since engines may have been upgraded
	}

	if err := ic.updateEngineImageRefCount(engineImage); err != nil {
		return errors.Wrapf(err, "failed to update RefCount for engine image %v(%v)", engineImage.Name, engineImage.Spec.Image)
	}

	if err := ic.cleanupExpiredEngineImage(engineImage); err != nil {
		return err
	}

	if engineImage.Status.State != types.EngineImageStateIncompatible && !reflect.DeepEqual(types.GetEngineImageLabels(engineImage.Name), ds.Labels) {
		engineImage.Status.State = types.EngineImageStateDeploying
		engineImage.Status.Conditions = types.SetCondition(engineImage.Status.Conditions,
			types.EngineImageConditionTypeReady, types.ConditionStatusFalse,
			types.EngineImageConditionTypeReadyReasonDaemonSet, fmt.Sprintf("Correcting the DaemonSet pod label for Engine image %v (%v)", engineImage.Name, engineImage.Spec.Image))
		// Cannot use update to correct the labels. The label update for the DaemonSet won’t be applied to the its pods.
		// The related issue: https://github.com/longhorn/longhorn/issues/769
		if err := ic.ds.DeleteDaemonSet(dsName); err != nil && !datastore.ErrorIsNotFound(err) {
			return errors.Wrapf(err, "cannot delete the DaemonSet with mismatching labels for engine image %v", engineImage.Name)
		}
		logrus.Infof("Removed DaemonSet %v with mismatching labels for engine image %v (%v). The DaemonSet with correct labels will be recreated automatically after deletion",
			dsName, engineImage.Name, engineImage.Spec.Image)
	}

	if engineImage.Status.State == types.EngineImageStateReady {
		engineImage.Status.Conditions = types.SetConditionAndRecord(engineImage.Status.Conditions,
			types.EngineImageConditionTypeReady, types.ConditionStatusTrue,
			"", fmt.Sprintf("Engine image %v (%v) become ready", engineImage.Name, engineImage.Spec.Image),
			ic.eventRecorder, engineImage, v1.EventTypeNormal)
	}

	return nil
}

func updateEngineImageVersion(ei *longhorn.EngineImage) error {
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
		return errors.Wrapf(err, "cannot get engine version for %v (%v)", ei.Name, ei.Spec.Image)
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
	}
	ei.Status.RefCount = refCount
	if ei.Status.RefCount == 0 {
		if ei.Status.NoRefSince == "" {
			ei.Status.NoRefSince = ic.nowHandler()
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
		// TODO: Need to consider if the engine image can be removed in engine image controller
		if err := ic.ds.DeleteEngineImage(ei.Name); err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (ic *EngineImageController) enqueueEngineImage(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	ic.queue.AddRateLimited(key)
}

func (ic *EngineImageController) enqueueVolumes(volumes ...interface{}) {
	images := map[string]struct{}{}
	for _, obj := range volumes {
		v, isVolume := obj.(*longhorn.Volume)
		if !isVolume {
			deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
			if !ok {
				utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
				continue
			}

			// use the last known state, to enqueue, dependent objects
			v, ok = deletedState.Obj.(*longhorn.Volume)
			if !ok {
				utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
				continue
			}
		}

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
		if engineImage.Status.OwnerID != ic.controllerID {
			continue
		}
		ic.enqueueEngineImage(engineImage)
	}
}

func (ic *EngineImageController) enqueueControlleeChange(obj interface{}) {
	if deletedState, ok := obj.(*cache.DeletedFinalStateUnknown); ok {
		obj = deletedState.Obj
	}

	metaObj, err := meta.Accessor(obj)
	if err != nil {
		logrus.Warnf("BUG: %v cannot be convert to metav1.Object: %v", obj, err)
		return
	}
	ownerRefs := metaObj.GetOwnerReferences()
	for _, ref := range ownerRefs {
		namespace := metaObj.GetNamespace()
		ic.ResolveRefAndEnqueue(namespace, &ref)
		return
	}
}

func (ic *EngineImageController) ResolveRefAndEnqueue(namespace string, ref *metav1.OwnerReference) {
	if ref.Kind != types.LonghornKindEngineImage {
		// TODO: Will stop checking this wrong reference kind after all Longhorn components having used the new kinds
		if ref.Kind != ownerKindEngineImage {
			return
		}
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
	if engineImage.Status.OwnerID != ic.controllerID {
		return
	}
	ic.enqueueEngineImage(engineImage)
}

func (ic *EngineImageController) createEngineImageDaemonSetSpec(ei *longhorn.EngineImage, tolerations []v1.Toleration,
	priorityClass, registrySecret string, imagePullPolicy v1.PullPolicy) *appsv1.DaemonSet {

	dsName := types.GetDaemonSetNameFromEngineImageName(ei.Name)
	image := ei.Spec.Image
	cmd := []string{
		"/bin/bash",
	}
	args := []string{
		"-c",
		"diff /usr/local/bin/longhorn /data/longhorn > /dev/null 2>&1; " +
			"if [ $? -ne 0 ]; then cp -p /usr/local/bin/longhorn /data/ && echo installed; fi && " +
			"trap 'rm /data/longhorn* && echo cleaned up' EXIT && sleep infinity",
	}
	maxUnavailable := intstr.FromString(`100%`)
	privileged := true
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
					Name:            dsName,
					Labels:          types.GetEngineImageLabels(ei.Name),
					OwnerReferences: datastore.GetOwnerReferencesForEngineImage(ei),
				},
				Spec: v1.PodSpec{
					ServiceAccountName: ic.serviceAccount,
					Tolerations:        tolerations,
					PriorityClassName:  priorityClass,
					Containers: []v1.Container{
						{
							Name:            dsName,
							Image:           image,
							Command:         cmd,
							Args:            args,
							ImagePullPolicy: imagePullPolicy,
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
											"sh", "-c",
											"ls /data/longhorn && /data/longhorn version --client-only",
										},
									},
								},
								InitialDelaySeconds: 5,
								PeriodSeconds:       5,
							},
							SecurityContext: &v1.SecurityContext{
								Privileged: &privileged,
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

	if registrySecret != "" {
		d.Spec.Template.Spec.ImagePullSecrets = []v1.LocalObjectReference{
			{
				Name: registrySecret,
			},
		}
	}

	return d
}
