package controller

import (
	"encoding/json"
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type ImageController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the image
	controllerID   string
	serviceAccount string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced
}

func NewImageController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	namespace, controllerID, serviceAccount string) (*ImageController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	ic := &ImageController{
		baseController: newBaseController("longhorn-image", logger),

		namespace:      namespace,
		controllerID:   controllerID,
		serviceAccount: serviceAccount,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-image-controller"}),

		ds: ds,
	}

	var err error
	if _, err = ds.ImageInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    ic.enqueueImage,
		UpdateFunc: func(old, cur interface{}) { ic.enqueueImage(cur) },
		DeleteFunc: ic.enqueueImage,
	}, 0); err != nil {
		return nil, err
	}
	ic.cacheSyncs = append(ic.cacheSyncs, ds.ImageInformer.HasSynced)

	return ic, nil
}

func (ic *ImageController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer ic.queue.ShutDown()

	ic.logger.Info("Starting Longhorn Image controller")
	defer ic.logger.Info("Shut down Longhorn Image controller")

	if !cache.WaitForNamedCacheSync("longhorn images", stopCh, ic.cacheSyncs...) {
		return
	}

	for i := 0; i < workers; i++ {
		go wait.Until(ic.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (ic *ImageController) worker() {
	for ic.processNextWorkItem() {
	}
}

func (ic *ImageController) processNextWorkItem() bool {
	key, quit := ic.queue.Get()

	if quit {
		return false
	}
	defer ic.queue.Done(key)

	err := ic.syncImage(key.(string))
	ic.handleErr(err, key)

	return true
}

func (ic *ImageController) handleErr(err error, key interface{}) {
	if err == nil {
		ic.queue.Forget(key)
		return
	}

	log := ic.logger.WithField("image", key)
	if ic.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn image")
		ic.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn image out of the queue")
	ic.queue.Forget(key)
}

func getLoggerForImage(logger logrus.FieldLogger, i *longhorn.Image) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"name": i.Name,
		},
	)
}

func (ic *ImageController) syncImage(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync engine image for %v", key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != ic.namespace {
		return nil
	}

	image, err := ic.ds.GetImage(name)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			return nil
		}
		return errors.Wrapf(err, "failed to get engine image")
	}
	log := getLoggerForImage(ic.logger, image)

	// check isResponsibleFor here
	isResponsible, err := ic.isResponsibleFor(image)
	if err != nil {
		return err
	}
	if !isResponsible {
		return nil
	}

	if image.Status.OwnerID != ic.controllerID {
		image.Status.OwnerID = ic.controllerID
		image, err = ic.ds.UpdateImageStatus(image)
		if err != nil {
			// we don't mind others coming first
			if apierrors.IsConflict(errors.Cause(err)) {
				return nil
			}
			return err
		}
		log.Infof("Image got new owner %v", ic.controllerID)
	}

	if image.DeletionTimestamp != nil {
		return ic.ds.RemoveFinalizerForImage(image)
	}

	existingImage := image.DeepCopy()
	defer func() {
		if err == nil && !reflect.DeepEqual(existingImage.Status, image.Status) {
			_, err = ic.ds.UpdateImageStatus(image)
		}
		if apierrors.IsConflict(errors.Cause(err)) {
			log.WithError(err).Debugf("Requeue image %v due to conflict", key)
			ic.enqueueImage(image)
			err = nil
		}
		if image.Status.State == longhorn.ImageStateDeploying {
			log.WithError(err).Debugf("Requeue image %v due to synchronize the deploy state", key)
			if ic.queue.NumRequeues(key) < maxRetries {
				ic.queue.AddRateLimited(key)
			} else {
				ic.enqueueImage(image)
			}
		}
	}()

	ds, err := ic.ds.GetImageDaemonSet(types.LonghornPrePullManagerImageDaemonSetName)
	if err != nil {
		return errors.Wrapf(err, "cannot get daemonset %v for image %v", types.LonghornPrePullManagerImageDaemonSetName, image.Name)
	}

	imagePullPolicy, err := ic.ds.GetSettingImagePullPolicy()
	if err != nil {
		return errors.Wrapf(err, "failed to get system pods image pull policy before creating/updating image daemonset")
	}

	if ds == nil {
		tolerations, err := ic.ds.GetSettingTaintToleration()
		if err != nil {
			return errors.Wrapf(err, "failed to get taint toleration setting before creating image daemonset")
		}

		nodeSelector, err := ic.ds.GetSettingSystemManagedComponentsNodeSelector()
		if err != nil {
			return err
		}

		priorityClassSetting, err := ic.ds.GetSettingWithAutoFillingRO(types.SettingNamePriorityClass)
		if err != nil {
			return errors.Wrapf(err, "failed to get priority class setting before creating image daemonset")
		}
		priorityClass := priorityClassSetting.Value

		registrySecretSetting, err := ic.ds.GetSettingWithAutoFillingRO(types.SettingNameRegistrySecret)
		if err != nil {
			return errors.Wrapf(err, "failed to get registry secret setting before creating image daemonset")
		}
		registrySecret := registrySecretSetting.Value

		dsSpec, err := ic.createImageDaemonSetSpec(tolerations, priorityClass, registrySecret, imagePullPolicy, nodeSelector)
		if err != nil {
			return errors.Wrapf(err, "failed to create daemonset spec for images")
		}

		log.Infof("Creating daemon set %v for images when reconciling image %v(%v)", dsSpec.Name, image.Name, image.Spec.ImageURL)
		if err = ic.ds.CreateImageDaemonSet(dsSpec); err != nil {
			return errors.Wrapf(err, "failed to create daemonset %v for images", dsSpec.Name)
		}
		image.Status.State = longhorn.ImageStateDeploying

		return nil
	}

	// update the image containers in the daemonset if needed
	needUpdate := true
	for _, container := range ds.Spec.Template.Spec.Containers {
		if container.Name == image.Name && container.Image == image.Spec.ImageURL {
			needUpdate = false
			break
		}
	}

	if needUpdate {
		newContainersSpec, err := ic.createImageDaemonSetContainersSpec(imagePullPolicy)
		if err != nil {
			return err
		}
		ds.Spec.Template.Spec.Containers = newContainersSpec
		if _, err = ic.ds.UpdateDaemonSet(ds); err != nil {
			return errors.Wrapf(err, "failed to update daemonset for image %v", image.Name)
		}
		image.Status.State = longhorn.ImageStateDeploying
		return nil
	}

	if err := ic.syncNodeDeploymentMap(image); err != nil {
		return err
	}

	deployedNodeCount := 0
	for _, isDeployed := range image.Status.NodeDeploymentMap {
		if isDeployed {
			deployedNodeCount++
		}
	}

	readyNodes, err := ic.ds.ListReadyNodesRO()
	if err != nil {
		return err
	}

	if deployedNodeCount < len(readyNodes) {
		image.Status.State = longhorn.ImageStateDeploying
	} else {
		image.Status.State = longhorn.ImageStateDeployed
	}

	return nil
}

func (ic *ImageController) createImageDaemonSetSpec(tolerations []corev1.Toleration, priorityClass, registrySecret string,
	imagePullPolicy corev1.PullPolicy, nodeSelector map[string]string) (*appsv1.DaemonSet, error) {

	maxUnavailable := intstr.FromString(`100%`)
	tolerationsByte, err := json.Marshal(tolerations)
	if err != nil {
		return nil, err
	}

	containersSpec, err := ic.createImageDaemonSetContainersSpec(imagePullPolicy)
	if err != nil {
		return nil, err
	}

	d := &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:        types.LonghornPrePullManagerImageDaemonSetName,
			Annotations: map[string]string{types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix): string(tolerationsByte)},
			Labels:      types.GetImageDaemonSetLabelSelector(),
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: types.GetImageDaemonSetLabelSelector(),
			},
			UpdateStrategy: appsv1.DaemonSetUpdateStrategy{
				Type: appsv1.RollingUpdateDaemonSetStrategyType,
				RollingUpdate: &appsv1.RollingUpdateDaemonSet{
					MaxUnavailable: &maxUnavailable,
				},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   types.LonghornPrePullManagerImageDaemonSetName,
					Labels: types.GetImageDaemonSetLabelSelector(),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: ic.serviceAccount,
					Tolerations:        tolerations,
					NodeSelector:       nodeSelector,
					PriorityClassName:  priorityClass,
					Containers:         containersSpec,
				},
			},
		},
	}

	if registrySecret != "" {
		d.Spec.Template.Spec.ImagePullSecrets = []corev1.LocalObjectReference{
			{
				Name: registrySecret,
			},
		}
	}
	types.AddGoCoverDirToDaemonSet(d)

	return d, nil
}

func (ic *ImageController) createImageDaemonSetContainersSpec(imagePullPolicy corev1.PullPolicy) ([]corev1.Container, error) {
	var allImageContainers []corev1.Container
	cmd := []string{
		"/bin/bash",
	}
	privileged := true

	images, err := ic.ds.ListImagesRO()
	if err != nil {
		return nil, err
	}

	for _, image := range images {
		imageContainer := corev1.Container{
			Name:            image.Name,
			Image:           image.Spec.ImageURL,
			Command:         cmd,
			Args:            []string{"-c", "echo " + image.Spec.ImageURL + " image pulled && sleep infinity"},
			ImagePullPolicy: imagePullPolicy,
			SecurityContext: &corev1.SecurityContext{
				Privileged: &privileged,
			},
		}
		allImageContainers = append(allImageContainers, imageContainer)
	}
	return allImageContainers, nil
}

func (ic *ImageController) syncNodeDeploymentMap(image *longhorn.Image) (err error) {
	defer func() {
		err = errors.Wrapf(err, "cannot sync NodeDeploymenMap for image %v", image.Name)
	}()

	// initialize deployment map for all known nodes
	nodeDeploymentMap := map[string]bool{}
	nodes, err := ic.ds.ListNodesRO()
	if err != nil {
		return err
	}
	for _, node := range nodes {
		nodeDeploymentMap[node.Name] = false
	}

	imageDaemonSetPods, err := ic.ds.ListImageDaemonSetPods()
	if err != nil {
		return err
	}
	for _, pod := range imageDaemonSetPods {
		allContainerReady := true
		for _, containerStatus := range pod.Status.ContainerStatuses {
			if containerStatus.Name == image.Name {
				allContainerReady = allContainerReady && containerStatus.Ready
			}
		}
		nodeDeploymentMap[pod.Spec.NodeName] = allContainerReady
	}

	image.Status.NodeDeploymentMap = nodeDeploymentMap
	return nil
}

func (ic *ImageController) enqueueImage(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	ic.queue.Add(key)
}

func (ic *ImageController) isResponsibleFor(image *longhorn.Image) (bool, error) {
	var err error
	defer func() {
		err = errors.Wrap(err, "error while checking isResponsibleFor")
	}()

	readyNodesWithImage, err := ic.ds.ListReadyNodesContainingImageRO(image.Name)
	if err != nil {
		return false, err
	}
	isResponsible := isControllerResponsibleFor(ic.controllerID, ic.ds, image.Name, "", image.Status.OwnerID)

	if len(readyNodesWithImage) == 0 {
		return isResponsible, nil
	}

	currentOwnerAvailable, err := ic.ds.CheckImageReadiness(image.Name, image.Status.OwnerID)
	if err != nil {
		return false, err
	}
	currentNodeAvailable, err := ic.ds.CheckImageReadiness(image.Name, ic.controllerID)
	if err != nil {
		return false, err
	}

	isPreferredOwner := currentNodeAvailable && isResponsible
	continueToBeOwner := currentNodeAvailable && ic.controllerID == image.Status.OwnerID
	requiresNewOwner := currentNodeAvailable && !currentOwnerAvailable
	return isPreferredOwner || continueToBeOwner || requiresNewOwner, nil
}
