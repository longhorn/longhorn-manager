package controller

import (
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

type SupportBundleController struct {
	*baseController

	// which namespace controller is running with
	namespace string
	// use as the OwnerID of the controller
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	sStoreSynced cache.InformerSynced

	SupportBundleInitialized util.Cond

	ServiceAccount string
}

func NewSupportBundleController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	supportBundleInformer lhinformers.SupportBundleInformer,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
	serviceAccount string) *SupportBundleController {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	sbc := &SupportBundleController{
		baseController: newBaseController("longhorn-support-bundle", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-support-bundle-controller"}),

		sStoreSynced:             supportBundleInformer.Informer().HasSynced,
		SupportBundleInitialized: "Initialized",
		ServiceAccount:           serviceAccount,
	}

	supportBundleInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sbc.enqueueSupportBundle,
		UpdateFunc: func(old, cur interface{}) { sbc.enqueueSupportBundle(cur) },
		DeleteFunc: sbc.enqueueSupportBundle,
	})

	return sbc
}

func (sbc *SupportBundleController) enqueueSupportBundle(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	sbc.queue.AddRateLimited(key)
}

func (sbc *SupportBundleController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer sbc.queue.ShutDown()

	sbc.logger.Infof("Start Longhorn Support Bundle controller")
	defer sbc.logger.Infof("Shutting down Longhorn Support Bundle controller")

	if !cache.WaitForNamedCacheSync(sbc.name, stopCh, sbc.sStoreSynced) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(sbc.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (sbc *SupportBundleController) worker() {
	for sbc.processNextWorkItem() {
	}
}

func (sbc *SupportBundleController) processNextWorkItem() bool {
	key, quit := sbc.queue.Get()
	if quit {
		return false
	}
	defer sbc.queue.Done(key)
	err := sbc.syncSupportBundle(key.(string))
	sbc.handleErr(err, key)
	return true
}

func (sbc *SupportBundleController) handleErr(err error, key interface{}) {
	if err == nil {
		sbc.queue.Forget(key)
		return
	}

	if sbc.queue.NumRequeues(key) < maxRetries {
		sbc.logger.WithError(err).Warnf("Error syncing Longhorn Support Bundle %v", key)
		sbc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	sbc.logger.WithError(err).Warnf("Dropping Longhorn Support Bundle %v out of the queue", key)
	sbc.queue.Forget(key)
}

func (sbc *SupportBundleController) syncSupportBundle(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: fail to sync support-bundle %v", sbc.name, key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != sbc.namespace {
		return nil
	}

	supportBundle, err := sbc.ds.GetSupportBundle(name)

	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			sbc.logger.WithField("supportbundle", name).Debug("Support bundle has been deleted")
		}
		return err
	}

	log := getLoggerForSupportBundle(sbc.logger, supportBundle)
	if supportBundle.DeletionTimestamp != nil {
		if supportBundle.Status.State != types.SupportBundleStateDeleting {
			supportBundle.Status.State = types.SupportBundleStateDeleting
			_, err := sbc.ds.UpdateSupportBundleStatus(supportBundle)
			if err != nil {
				return err
			}
			return sbc.ds.DeleteSupportBundle(name)
		}
	}

	existingSupportBundle := supportBundle.DeepCopy()

	defer func() {
		if !reflect.DeepEqual(supportBundle.Status, existingSupportBundle.Status) {
			_, err = sbc.ds.UpdateSupportBundle(supportBundle)
		}

		if apierrors.IsConflict(errors.Cause(err)) {
			log.Debugf("Requeue support bundle due to error %v", err)
			sbc.enqueueSupportBundle(supportBundle)
			err = nil
		}
	}()

	if err := sbc.reconcile(supportBundle); err != nil {
		return err
	}

	return nil
}

func getLoggerForSupportBundle(logger logrus.FieldLogger, supportBundle *longhorn.SupportBundle) *logrus.Entry {
	return logger.WithFields(
		logrus.Fields{
			"supportbundle": supportBundle.Name,
		},
	)
}

func (sbc *SupportBundleController) reconcile(sb *longhorn.SupportBundle) (err error) {

	log := getLoggerForSupportBundle(sbc.logger, sb)

	switch sb.Status.State {
	case types.SupportBundleStateNone:
		log.Debugf("[%s] generating a support bundle", sb.Name)

		supportBundleImage, err := sbc.ds.GetSetting(types.SettingNameSupportBundleImage)
		if err != nil {
			return err
		}

		err = sbc.Create(sb, sbc.getManagerName(sb), supportBundleImage.Value)
		if err != nil {
			sb.Status.State = types.SupportBundleStateError
			sb.Status.ErrorMessage = types.SupportBundleErrorCreateDeployment
			return err
		}

		sb.Status.State = types.SupportBundleStateInProgress
		return nil
	case types.SupportBundleStateInProgress:
		logrus.Debugf("[%s] support bundle is being generated", sb.Name)
		err = sbc.checkManagerStatus(sb)
		return err
	default:
		log.Debugf("[%s] noop for state %s", sb.Name, sb.Status.State)
		return nil
	}
}

func (sbc *SupportBundleController) getManagerName(supportBundle *longhorn.SupportBundle) string {
	return fmt.Sprintf("supportbundle-manager-%s", supportBundle.Name)
}

func (sbc *SupportBundleController) getImagePullPolicy() corev1.PullPolicy {

	imagePullPolicy, err := sbc.ds.GetSetting(types.SettingNameSystemManagedPodsImagePullPolicy)
	if err != nil {
		return corev1.PullIfNotPresent
	}

	switch strings.ToLower(imagePullPolicy.Value) {
	case "always":
		return corev1.PullAlways
	case "if-not-present":
		return corev1.PullIfNotPresent
	case "never":
		return corev1.PullNever
	default:
		return corev1.PullIfNotPresent
	}
}

func (sbc *SupportBundleController) checkManagerStatus(sb *longhorn.SupportBundle) error {
	if time.Now().After(sb.CreationTimestamp.Add(types.SupportBundleCreationTimeout)) {
		sb.Status.State = types.SupportBundleStateError
		sb.Status.ErrorMessage = types.SupportBundleErrorTimeout
		return nil
	}

	managerStatus, err := sbc.GetStatus(sb)
	if err != nil {
		logrus.Debugf("[%s] manager pod is not ready: %s", sb.Name, err)
		sb.Status.State = types.SupportBundleStateError
		sb.Status.ErrorMessage = managerStatus.ErrorMessage
		sbc.enqueueSupportBundle(sb)
		return err
	}

	switch managerStatus.Phase {
	case types.SupportBundleStateReadyForDownload:
		sb.Status.State = types.SupportBundleStateReadyForDownload
		sb.Status.FileSize = managerStatus.FileSize
		sb.Status.Progress = managerStatus.Progress
		sb.Status.FileName = managerStatus.FileName
		return nil

	default:
		if sb.Status.Progress == managerStatus.Progress {
			sbc.enqueueSupportBundle(sb)
			return nil
		}
		sb.Status.Progress = managerStatus.Progress
		return nil
	}
}

func (sbc *SupportBundleController) Create(sb *longhorn.SupportBundle, deployName, image string) error {

	log := getLoggerForSupportBundle(sbc.logger, sb)
	log.Debugf("creating deployment %s with image %s", deployName, image)

	pullPolicy := sbc.getImagePullPolicy()

	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      deployName,
			Namespace: sb.Namespace,
			Labels: map[string]string{
				"app":                       types.SupportBundleManager,
				types.SupportBundleLabelKey: sb.Name,
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					Name:       sb.Name,
					Kind:       "SupportBundle",
					UID:        sb.UID,
					APIVersion: "v1beta1",
				},
			},
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": types.SupportBundleManager},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{
						"app":                       types.SupportBundleManager,
						types.SupportBundleLabelKey: sb.Name,
					},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:            "manager",
							Image:           image,
							Args:            []string{"/usr/bin/support-bundle-kit", "manager"},
							ImagePullPolicy: pullPolicy,
							Env: []corev1.EnvVar{
								{
									// support bundle kit need to change the environment variable name it accepts
									Name:  "SUPPORT_BUNDLE_NAMESPACE",
									Value: sb.Namespace,
								},
								{
									Name:  "SUPPORT_BUNDLE_VERSION",
									Value: manager.FriendlyVersion(),
								},
								{
									Name:  "SUPPORT_BUNDLE_NAME",
									Value: sb.Name,
								},
								{
									Name:  "SUPPORT_BUNDLE_DEBUG",
									Value: "true",
								},
								{
									Name: "SUPPORT_BUNDLE_MANAGER_POD_IP",
									ValueFrom: &corev1.EnvVarSource{
										FieldRef: &corev1.ObjectFieldSelector{
											FieldPath: "status.podIP",
										},
									},
								},
								{
									Name:  "SUPPORT_BUNDLE_IMAGE",
									Value: image,
								},
								{
									Name:  "SUPPORT_BUNDLE_IMAGE_PULL_POLICY",
									Value: string(pullPolicy),
								},
							},
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: 8080,
								},
							},
						},
					},
					ServiceAccountName: sbc.ServiceAccount,
				},
			},
		},
	}

	_, err := sbc.ds.CreateDeployment(deployment)
	return err
}

func (sbc *SupportBundleController) GetStatus(sb *longhorn.SupportBundle) (*ManagerStatus, error) {
	podIP, err := manager.GetManagerPodIP(sbc.ds)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("http://%s:8080/status", podIP)
	httpClient := http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := httpClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	s := &ManagerStatus{}
	err = json.NewDecoder(resp.Body).Decode(s)
	if err != nil {
		return nil, err
	}

	return s, nil
}

type ManagerStatus struct {
	Phase types.SuppportBundleState

	ErrorMessage types.SupportBundleError

	Progress int

	FileName string

	FileSize int64
}
