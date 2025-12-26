package controller

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	metricsclientset "k8s.io/metrics/pkg/client/clientset/versioned"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	lhns "github.com/longhorn/go-common-libs/ns"

	"github.com/longhorn/longhorn-manager/constant"
	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	VersionTagLatest = "latest"
	VersionTagStable = "stable"
)

var (
	upgradeCheckInterval          = time.Hour
	settingControllerResyncPeriod = time.Hour
)

type SettingController struct {
	*baseController

	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	metricsClient metricsclientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	cacheSyncs []cache.InformerSynced

	// upgrade checker
	lastUpgradeCheckedTimestamp time.Time
	version                     string
}

type Version struct {
	Name        string // must be in semantic versioning
	ReleaseDate string
	Tags        []string
}

type CheckUpgradeRequest struct {
	AppVersion string `json:"appVersion"`

	ExtraTagInfo   CheckUpgradeExtraInfo `json:"extraTagInfo"`
	ExtraFieldInfo CheckUpgradeExtraInfo `json:"extraFieldInfo"`
}

type CheckUpgradeExtraInfo interface {
}

type CheckUpgradeResponse struct {
	Versions []Version `json:"versions"`
}

func NewSettingController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	metricsClient metricsclientset.Interface,
	namespace, controllerID, version string) (*SettingController, error) {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	sc := &SettingController{
		baseController: newBaseController("longhorn-setting", logger),

		namespace:    namespace,
		controllerID: controllerID,

		kubeClient:    kubeClient,
		metricsClient: metricsClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, corev1.EventSource{Component: "longhorn-setting-controller"}),

		ds: ds,

		version: version,
	}

	var err error
	if _, err = ds.SettingInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.enqueueSetting,
		UpdateFunc: func(old, cur interface{}) { sc.enqueueSetting(cur) },
		DeleteFunc: sc.enqueueSetting,
	}, settingControllerResyncPeriod); err != nil {
		return nil, err
	}
	sc.cacheSyncs = append(sc.cacheSyncs, ds.SettingInformer.HasSynced)

	if _, err = ds.NodeInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.enqueueSettingForNode,
		UpdateFunc: func(old, cur interface{}) { sc.enqueueSettingForNode(cur) },
		DeleteFunc: sc.enqueueSettingForNode,
	}, 0); err != nil {
		return nil, err
	}
	sc.cacheSyncs = append(sc.cacheSyncs, ds.NodeInformer.HasSynced)

	return sc, nil
}

func (sc *SettingController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer sc.queue.ShutDown()

	sc.logger.Info("Starting Longhorn Setting controller")
	defer sc.logger.Info("Shut down Longhorn Setting controller")

	if !cache.WaitForNamedCacheSync("longhorn settings", stopCh, sc.cacheSyncs...) {
		return
	}

	// must remain single threaded since backup store timer is not thread-safe now
	go wait.Until(sc.worker, time.Second, stopCh)

	<-stopCh
}

func (sc *SettingController) worker() {
	for sc.processNextWorkItem() {
	}
}

func (sc *SettingController) processNextWorkItem() bool {
	key, quit := sc.queue.Get()

	if quit {
		return false
	}
	defer sc.queue.Done(key)

	err := sc.syncSetting(key.(string))
	sc.handleErr(err, key)

	return true
}

func (sc *SettingController) handleErr(err error, key interface{}) {
	if err == nil {
		sc.queue.Forget(key)
		return
	}

	log := sc.logger.WithField("Setting", key)
	if sc.queue.NumRequeues(key) < maxRetries {
		handleReconcileErrorLogging(log, err, "Failed to sync Longhorn setting")
		sc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	handleReconcileErrorLogging(log, err, "Dropping Longhorn setting out of the queue")
	sc.queue.Forget(key)
}

func (sc *SettingController) syncSetting(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync setting for %v", key)
	}()

	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	defer func() {
		setting, dsErr := sc.ds.GetSettingExact(types.SettingName(name))
		if dsErr != nil {
			sc.logger.WithError(dsErr).Warnf("Failed to get setting: %v", name)
			return
		}
		existingApplied := setting.Status.Applied
		if !setting.Status.Applied && err == nil {
			setting.Status.Applied = true
		} else if err != nil {
			setting.Status.Applied = false
		}
		if setting.Status.Applied != existingApplied {
			if _, dsErr := sc.ds.UpdateSettingStatus(setting); dsErr != nil {
				sc.logger.WithError(dsErr).Warnf("Failed to update setting: %v", name)
			}
		}
	}()

	if err := sc.syncNonDangerZoneSettingsForManagedComponents(types.SettingName(name)); err != nil {
		return err
	}

	return sc.syncDangerZoneSettingsForManagedComponents(types.SettingName(name))
}

func (sc *SettingController) syncNonDangerZoneSettingsForManagedComponents(settingName types.SettingName) error {
	switch settingName {
	case types.SettingNameUpgradeChecker:
		if err := sc.syncUpgradeChecker(); err != nil {
			return err
		}
	case types.SettingNameKubernetesClusterAutoscalerEnabled:
		if err := sc.updateKubernetesClusterAutoscalerEnabled(); err != nil {
			return err
		}
	case types.SettingNameSupportBundleFailedHistoryLimit:
		if err := sc.cleanupFailedSupportBundles(); err != nil {
			return err
		}
	case types.SettingNameLogLevel:
		if err := sc.updateLogLevel(settingName); err != nil {
			return err
		}
	case types.SettingNameDefaultLonghornStaticStorageClass:
		if err := sc.syncDefaultLonghornStaticStorageClass(); err != nil {
			return err
		}
	}

	return nil
}

func (sc *SettingController) isDangerZoneSetting(settingName types.SettingName) bool {
	settingDefinition, exists := types.GetSettingDefinition(settingName)
	if !exists {
		sc.logger.Debugf("Setting %s does not exist", settingName)
		return false
	}

	return settingDefinition.Category == types.SettingCategoryDangerZone
}

func (sc *SettingController) syncDangerZoneSettingsForManagedComponents(settingName types.SettingName) error {
	if !sc.isDangerZoneSetting(settingName) {
		return nil
	}

	dangerSettingsRequiringAllVolumesDetached := []types.SettingName{
		types.SettingNameTaintToleration,
		types.SettingNameSystemManagedComponentsNodeSelector,
		types.SettingNamePriorityClass,
		types.SettingNameStorageNetwork,
	}

	if slices.Contains(dangerSettingsRequiringAllVolumesDetached, settingName) {
		switch settingName {
		case types.SettingNameTaintToleration:
			if err := sc.updateTaintToleration(); err != nil {
				return err
			}
		case types.SettingNameSystemManagedComponentsNodeSelector:
			if err := sc.updateNodeSelector(); err != nil {
				return err
			}
		case types.SettingNamePriorityClass:
			if err := sc.updatePriorityClass(); err != nil {
				return err
			}
		case types.SettingNameStorageNetwork:
			funcPreupdate := func() error {
				detached, err := sc.ds.AreAllVolumesDetachedState()
				if err != nil {
					return errors.Wrapf(err, "failed to check volume detachment for %v setting update", types.SettingNameStorageNetwork)
				}

				if !detached {
					return &types.ErrorInvalidState{Reason: fmt.Sprintf("failed to apply %v setting to Longhorn components when there are attached volumes. It will be eventually applied", types.SettingNameStorageNetwork)}
				}

				return nil
			}

			if err := sc.updateCNI(funcPreupdate); err != nil {
				return err
			}
		}

		return nil
	}

	dangerSettingRequiringRWXVolumesDetached := []types.SettingName{
		types.SettingNameEndpointNetworkForRWXVolume,
	}
	if slices.Contains(dangerSettingRequiringRWXVolumesDetached, settingName) {
		switch settingName {
		case types.SettingNameEndpointNetworkForRWXVolume:
			funcPreupdate := func() error {
				detached, err := sc.ds.AreAllRWXVolumesDetached()
				if err != nil {
					return errors.Wrapf(err, "failed to check volume detachment for %v setting update", types.SettingNameEndpointNetworkForRWXVolume)
				}
				if !detached {
					return &types.ErrorInvalidState{Reason: fmt.Sprintf("failed to apply %v setting to Longhorn components when there are attached volumes. It will be eventually applied", types.SettingNameEndpointNetworkForRWXVolume)}
				}

				return nil
			}

			if err := sc.updateCNI(funcPreupdate); err != nil {
				return err
			}
		}
		return nil
	}

	// Other danger-zone settings not requiring volume detachment
	switch settingName {
	case types.SettingNameSystemManagedCSIComponentsResourceLimits:
		return sc.updateSystemManagedCSIComponentsResourceLimits()
	}

	// These settings are also protected by webhook validators, when there are new updates.
	// Updating them is only allowed when all volumes of the data engine are detached.
	dangerSettingsRequiringSpecificDataEngineVolumesDetached := []types.SettingName{
		types.SettingNameV1DataEngine,
		types.SettingNameV2DataEngine,
		types.SettingNameGuaranteedInstanceManagerCPU,
	}

	if slices.Contains(dangerSettingsRequiringSpecificDataEngineVolumesDetached, settingName) {
		switch settingName {
		case types.SettingNameV1DataEngine, types.SettingNameV2DataEngine:
			if err := sc.updateDataEngine(settingName); err != nil {
				return errors.Wrapf(err, "failed to apply %v setting to Longhorn instance managers when there are attached volumes. "+
					"It will be eventually applied", settingName)
			}
		case types.SettingNameGuaranteedInstanceManagerCPU:
			for _, dataEngine := range []longhorn.DataEngineType{longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2} {
				if err := sc.updateInstanceManagerCPURequest(dataEngine); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// getResponsibleNodeID returns which node need to run
func getResponsibleNodeID(ds *datastore.DataStore) (string, error) {
	readyNodes, err := ds.ListReadyNodesRO()
	if err != nil {
		return "", err
	}
	if len(readyNodes) == 0 {
		return "", fmt.Errorf("no ready nodes available")
	}

	// We use the first node as the responsible node
	// If we pick a random node, there is probability
	// more than one node be responsible node at the same time
	var responsibleNodes []string
	for node := range readyNodes {
		responsibleNodes = append(responsibleNodes, node)
	}
	sort.Strings(responsibleNodes)
	return responsibleNodes[0], nil
}

// updateTaintToleration deletes all user-deployed and system-managed components immediately with the updated taint toleration.
func (sc *SettingController) updateTaintToleration() error {
	setting, err := sc.ds.GetSettingWithAutoFillingRO(types.SettingNameTaintToleration)
	if err != nil {
		return err
	}
	newTolerations := setting.Value
	newTolerationsList, err := types.UnmarshalTolerations(newTolerations)
	if err != nil {
		return err
	}
	newTolerationsMap := util.TolerationListToMap(newTolerationsList)

	updatingRuntimeObjects, err := sc.collectRuntimeObjects()
	if err != nil {
		return errors.Wrap(err, "failed to collect runtime objects for toleration update")
	}
	notUpdatedTolerationObjs, err := getNotUpdatedTolerationList(newTolerationsMap, updatingRuntimeObjects...)
	if err != nil {
		return err
	}
	if len(notUpdatedTolerationObjs) == 0 {
		return nil
	}

	detached, err := sc.ds.AreAllVolumesDetachedState()
	if err != nil {
		return errors.Wrapf(err, "failed to check volume detachment for %v setting update", types.SettingNameTaintToleration)
	}
	if !detached {
		return &types.ErrorInvalidState{Reason: fmt.Sprintf("failed to apply %v setting to Longhorn components when there are attached volumes. It will be eventually applied", types.SettingNameTaintToleration)}
	}

	for _, obj := range notUpdatedTolerationObjs {
		lastAppliedTolerationsList, err := getLastAppliedTolerationsList(obj)
		if err != nil {
			return err
		}
		switch objType := obj.(type) {
		case *appsv1.DaemonSet:
			ds := obj.(*appsv1.DaemonSet)
			sc.logger.Infof("Deleting daemonset %v to update tolerations from %v to %v", ds.Name, util.TolerationListToMap(lastAppliedTolerationsList), newTolerationsMap)
			if err := sc.updateTolerationForDaemonset(ds, lastAppliedTolerationsList, newTolerationsList); err != nil {
				return err
			}
		case *appsv1.Deployment:
			dp := obj.(*appsv1.Deployment)
			sc.logger.Infof("Updating deployment %v to update tolerations from %v to %v", dp.Name, util.TolerationListToMap(lastAppliedTolerationsList), newTolerationsMap)
			if err := sc.updateTolerationForDeployment(dp, lastAppliedTolerationsList, newTolerationsList); err != nil {
				return err
			}
		case *corev1.Pod:
			pod := obj.(*corev1.Pod)
			sc.logger.Infof("Deleting pod %v to update tolerations from %v to %v", pod.Name, util.TolerationListToMap(lastAppliedTolerationsList), newTolerationsMap)
			if err := sc.ds.DeletePod(pod.Name); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown object type %v when updating %v setting", objType, types.SettingNameTaintToleration)
		}
	}

	return nil
}

func (sc *SettingController) collectRuntimeObjects() (returnCollectRuntimeObjects []runtime.Object, err error) {
	dsList, err := sc.ds.ListDeploymentWithLabels(types.GetBaseLabelsForSystemManagedComponent())
	if err != nil {
		return nil, errors.Wrap(err, "failed to list Longhorn deployments for collecting runtime objects")
	}

	dpList, err := sc.ds.ListDaemonSetWithLabels(types.GetBaseLabelsForSystemManagedComponent())
	if err != nil {
		return nil, errors.Wrap(err, "failed to list Longhorn daemonsets for collecting runtime objects")
	}

	imPodList, err := sc.ds.ListInstanceManagerPods()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list instance manager pods for collecting runtime objects")
	}
	smPodList, err := sc.ds.ListShareManagerPods()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list share manager pods for collecting runtime objects")
	}
	bimPodList, err := sc.ds.ListBackingImageManagerPods()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list backing image manager pods for collecting runtime objects")
	}
	podList := append(imPodList, smPodList...)
	podList = append(podList, bimPodList...)

	for _, ds := range dsList {
		returnCollectRuntimeObjects = append(returnCollectRuntimeObjects, runtime.Object(ds))
	}
	for _, dp := range dpList {
		returnCollectRuntimeObjects = append(returnCollectRuntimeObjects, runtime.Object(dp))
	}
	for _, pod := range podList {
		returnCollectRuntimeObjects = append(returnCollectRuntimeObjects, runtime.Object(pod))
	}

	return returnCollectRuntimeObjects, nil
}

func getNotUpdatedTolerationList(newTolerationsMap map[string]corev1.Toleration, objs ...runtime.Object) ([]runtime.Object, error) {
	notUpdatedObjsList := []runtime.Object{}
	for _, obj := range objs {
		lastAppliedTolerationsList, err := getLastAppliedTolerationsList(obj)
		if err != nil {
			return notUpdatedObjsList, err
		}
		if !reflect.DeepEqual(util.TolerationListToMap(lastAppliedTolerationsList), newTolerationsMap) {
			notUpdatedObjsList = append(notUpdatedObjsList, obj)
		}
	}

	return notUpdatedObjsList, nil
}

func (sc *SettingController) updateTolerationForDeployment(dp *appsv1.Deployment, lastAppliedTolerations, newTolerations []corev1.Toleration) error {
	existingTolerationsMap := util.TolerationListToMap(dp.Spec.Template.Spec.Tolerations)
	lastAppliedTolerationsMap := util.TolerationListToMap(lastAppliedTolerations)
	newTolerationsMap := util.TolerationListToMap(newTolerations)
	dp.Spec.Template.Spec.Tolerations = getFinalTolerations(existingTolerationsMap, lastAppliedTolerationsMap, newTolerationsMap)
	newTolerationsByte, err := json.Marshal(newTolerations)
	if err != nil {
		return err
	}
	if err := util.SetAnnotation(dp, types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix), string(newTolerationsByte)); err != nil {
		return err
	}
	sc.logger.Infof("Updating tolerations from %v to %v for %v", existingTolerationsMap, dp.Spec.Template.Spec.Tolerations, dp.Name)
	if _, err := sc.ds.UpdateDeployment(dp); err != nil {
		return err
	}
	return nil
}

func (sc *SettingController) updateTolerationForDaemonset(ds *appsv1.DaemonSet, lastAppliedTolerations, newTolerations []corev1.Toleration) error {
	existingTolerationsMap := util.TolerationListToMap(ds.Spec.Template.Spec.Tolerations)
	lastAppliedTolerationsMap := util.TolerationListToMap(lastAppliedTolerations)
	newTolerationsMap := util.TolerationListToMap(newTolerations)
	ds.Spec.Template.Spec.Tolerations = getFinalTolerations(existingTolerationsMap, lastAppliedTolerationsMap, newTolerationsMap)
	newTolerationsByte, err := json.Marshal(newTolerations)
	if err != nil {
		return err
	}
	if err := util.SetAnnotation(ds, types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix), string(newTolerationsByte)); err != nil {
		return err
	}
	sc.logger.Infof("Updating tolerations from %v to %v for %v", existingTolerationsMap, ds.Spec.Template.Spec.Tolerations, ds.Name)
	if _, err := sc.ds.UpdateDaemonSet(ds); err != nil {
		return err
	}
	return nil
}

// getLastAppliedTolerationsList returns last applied tolerations list.
func getLastAppliedTolerationsList(obj runtime.Object) ([]corev1.Toleration, error) {
	lastAppliedTolerations, err := util.GetAnnotation(obj, types.GetLonghornLabelKey(types.LastAppliedTolerationAnnotationKeySuffix))
	if err != nil {
		return nil, err
	}

	if lastAppliedTolerations == "" {
		lastAppliedTolerations = "[]"
	}

	var lastAppliedTolerationsList []corev1.Toleration
	if err := json.Unmarshal([]byte(lastAppliedTolerations), &lastAppliedTolerationsList); err != nil {
		return nil, err
	}

	return lastAppliedTolerationsList, nil
}

// updatePriorityClass deletes all user-deployed and system-managed components immediately with the updated priority class.
func (sc *SettingController) updatePriorityClass() error {
	setting, err := sc.ds.GetSettingWithAutoFillingRO(types.SettingNamePriorityClass)
	if err != nil {
		return err
	}
	newPriorityClass := setting.Value

	updatingRuntimeObjects, err := sc.collectRuntimeObjects()
	if err != nil {
		return errors.Wrap(err, "failed to collect runtime objects for priority class update")
	}
	notUpdatedPriorityClassObjs, err := getNotUpdatedPriorityClassList(newPriorityClass, updatingRuntimeObjects...)
	if err != nil {
		return err
	}
	if len(notUpdatedPriorityClassObjs) == 0 {
		return nil
	}

	detached, err := sc.ds.AreAllVolumesDetachedState()
	if err != nil {
		return errors.Wrapf(err, "failed to check volume detachment for %v setting update", types.SettingNamePriorityClass)
	}
	if !detached {
		return &types.ErrorInvalidState{Reason: fmt.Sprintf("failed to apply %v setting to Longhorn components when there are attached volumes. It will be eventually applied", types.SettingNamePriorityClass)}
	}

	for _, obj := range notUpdatedPriorityClassObjs {
		switch objType := obj.(type) {
		case *appsv1.DaemonSet:
			ds := obj.(*appsv1.DaemonSet)
			sc.logger.Infof("Updating the priority class from %v to %v for %v", ds.Spec.Template.Spec.PriorityClassName, newPriorityClass, ds.Name)
			ds.Spec.Template.Spec.PriorityClassName = newPriorityClass
			if _, err := sc.ds.UpdateDaemonSet(ds); err != nil {
				return err
			}
		case *appsv1.Deployment:
			dp := obj.(*appsv1.Deployment)
			sc.logger.Infof("Updating the priority class from %v to %v for %v", dp.Spec.Template.Spec.PriorityClassName, newPriorityClass, dp.Name)
			dp.Spec.Template.Spec.PriorityClassName = newPriorityClass
			if _, err := sc.ds.UpdateDeployment(dp); err != nil {
				return err
			}
		case *corev1.Pod:
			pod := obj.(*corev1.Pod)
			sc.logger.Infof("Deleting pod %v to update the priority class from %v to %v", pod.Name, pod.Spec.PriorityClassName, newPriorityClass)
			if err := sc.ds.DeletePod(pod.Name); err != nil {
				return err
			}
		default:
			return fmt.Errorf("unknown object type %v when updating %v setting", objType, types.SettingNamePriorityClass)
		}
	}

	return nil
}

func getNotUpdatedPriorityClassList(newPriorityClassName string, objs ...runtime.Object) ([]runtime.Object, error) {
	notUpdatedObjsList := []runtime.Object{}
	oldPriorityClassName := ""
	for _, obj := range objs {
		switch objType := obj.(type) {
		case *appsv1.DaemonSet:
			ds := obj.(*appsv1.DaemonSet)
			oldPriorityClassName = ds.Spec.Template.Spec.PriorityClassName
		case *appsv1.Deployment:
			dp := obj.(*appsv1.Deployment)
			oldPriorityClassName = dp.Spec.Template.Spec.PriorityClassName
		case *corev1.Pod:
			pod := obj.(*corev1.Pod)
			oldPriorityClassName = pod.Spec.PriorityClassName
		default:
			return nil, fmt.Errorf("unknown object type %v when updating %v setting", objType, types.SettingNamePriorityClass)
		}
		if oldPriorityClassName == newPriorityClassName {
			continue
		}

		notUpdatedObjsList = append(notUpdatedObjsList, obj)
	}

	return notUpdatedObjsList, nil
}

func (sc *SettingController) updateKubernetesClusterAutoscalerEnabled() error {
	// IM pods annotation will be handled in the instance manager controller

	clusterAutoscalerEnabled, err := sc.ds.GetSettingAsBool(types.SettingNameKubernetesClusterAutoscalerEnabled)
	if err != nil {
		return err
	}

	evictKey := types.KubernetesClusterAutoscalerSafeToEvictKey

	deploymentList, err := sc.ds.ListDeploymentWithLabels(types.GetBaseLabelsForSystemManagedComponent())
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn deployments for %v annotation update", types.KubernetesClusterAutoscalerSafeToEvictKey)
	}

	longhornUI, err := sc.ds.GetDeployment(types.LonghornUIDeploymentName)
	if err != nil && !apierrors.IsNotFound(err) {
		return errors.Wrapf(err, "failed to get %v deployment", types.LonghornUIDeploymentName)
	}

	if longhornUI != nil {
		deploymentList = append(deploymentList, longhornUI)
	}

	for _, dp := range deploymentList {
		if !util.HasLocalStorageInDeployment(dp) {
			continue
		}

		anno := dp.Spec.Template.Annotations
		if anno == nil {
			anno = map[string]string{}
		}
		if clusterAutoscalerEnabled {
			if value, exists := anno[evictKey]; exists && value == strconv.FormatBool(clusterAutoscalerEnabled) {
				continue
			}

			anno[evictKey] = strconv.FormatBool(clusterAutoscalerEnabled)
			sc.logger.Infof("Updating the %v annotation to %v for %v", evictKey, clusterAutoscalerEnabled, dp.Name)
		} else {
			if _, exists := anno[evictKey]; !exists {
				continue
			}

			delete(anno, evictKey)
			sc.logger.Infof("Deleting the %v annotation for %v", evictKey, dp.Name)
		}
		dp.Spec.Template.Annotations = anno
		if _, err := sc.ds.UpdateDeployment(dp); err != nil {
			return err
		}
	}

	return nil
}

// updateCNI deletes all system-managed data plane components immediately with the updated CNI annotation.
func (sc *SettingController) updateCNI(funcPreupdate func() error) error {
	storageNetwork, err := sc.ds.GetSettingWithAutoFillingRO(types.SettingNameStorageNetwork)
	if err != nil {
		return err
	}

	endpointNetworkForRWXVolume, err := sc.ds.GetSettingWithAutoFillingRO(types.SettingNameEndpointNetworkForRWXVolume)
	if err != nil {
		return err
	}

	incorrectCNIDaemonSets, err := sc.getDaemonSetsWithIncorrectCNI(endpointNetworkForRWXVolume)
	if err != nil {
		return err
	}

	incorrectCNIPods, err := sc.getPodsWithIncorrectCNI(storageNetwork)
	if err != nil {
		return err
	}

	if len(incorrectCNIDaemonSets) == 0 && len(incorrectCNIPods) == 0 {
		return nil
	}

	err = funcPreupdate()
	if err != nil {
		return err
	}

	for _, daemonSet := range incorrectCNIDaemonSets {
		types.UpdateDaemonSetTemplateBasedOnCNISettings(daemonSet, endpointNetworkForRWXVolume)
		if _, err := sc.ds.UpdateDaemonSet(daemonSet); err != nil {
			return err
		}
	}

	for _, pod := range incorrectCNIPods {
		logrus.WithFields(logrus.Fields{
			"pod": pod.Name,
		}).Infof("Deleting pod for %v setting update", types.SettingNameStorageNetwork)

		if err := sc.ds.DeletePod(pod.Name); err != nil {
			return err
		}
	}

	return nil
}

func (sc *SettingController) getPodsWithIncorrectCNI(storageNetwork *longhorn.Setting) ([]*corev1.Pod, error) {
	// Retrieve annotation key and value for CNI.
	annotKey := string(types.CNIAnnotationNetworks)
	annotValue := types.CreateCniAnnotationFromSetting(storageNetwork, types.StorageNetworkInterface)

	var incorrectCNIPods []*corev1.Pod

	// Retrieve instance manager Pods.
	imPodList, err := sc.ds.ListInstanceManagerPods()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list instance manager Pods for %v setting update", types.SettingNameStorageNetwork)
	}

	// Retrieve backing image manager Pods.
	bimPodList, err := sc.ds.ListBackingImageManagerPods()
	if err != nil {
		return nil, errors.Wrapf(err, "failed to list backing image manager Pods for %v setting update", types.SettingNameStorageNetwork)
	}

	pods := append(imPodList, bimPodList...)

	// Check Pods for incorrect CNI annotation.
	for _, pod := range pods {
		if pod.Annotations[annotKey] == annotValue {
			continue
		}
		incorrectCNIPods = append(incorrectCNIPods, pod)
	}

	return incorrectCNIPods, nil
}

func (sc *SettingController) getDaemonSetsWithIncorrectCNI(endpointNetworkForRWXVolume *longhorn.Setting) ([]*appsv1.DaemonSet, error) {
	// Get CNI annotation key and value.
	annotKey := string(types.CNIAnnotationNetworks)

	annotValue := ""
	if endpointNetworkForRWXVolume.Value != string(types.CniNetworkNone) {
		annotValue = types.CreateCniAnnotationFromSetting(endpointNetworkForRWXVolume, types.EndpointNetworkInterface)
	}

	var incorrectCNIDaemonSets []*appsv1.DaemonSet

	// List of DaemonSet names to check.
	daemonSetNames := []string{
		types.CSIPluginName,
	}

	for _, daemonSetName := range daemonSetNames {
		daemonSet, err := sc.ds.GetDaemonSet(daemonSetName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get daemonset %v for %v setting update", daemonSetName, types.SettingNameStorageNetwork)
		}

		if annotValue == "" {
			if _, exist := daemonSet.Spec.Template.Annotations[annotKey]; exist {
				// Not expecting CNI annotation to exist.
				incorrectCNIDaemonSets = append(incorrectCNIDaemonSets, daemonSet)
			}
		} else if daemonSet.Spec.Template.Annotations[annotKey] != annotValue {
			// Expecting CNI annotation to exist, but value mismatch.
			incorrectCNIDaemonSets = append(incorrectCNIDaemonSets, daemonSet)
		}
	}

	return incorrectCNIDaemonSets, nil
}

func (sc *SettingController) updateLogLevel(settingName types.SettingName) error {
	setting, err := sc.ds.GetSettingWithAutoFillingRO(settingName)
	if err != nil {
		return err
	}
	oldLevel := logrus.GetLevel()
	newLevel, err := logrus.ParseLevel(setting.Value)
	if err != nil {
		return err
	}
	if oldLevel != newLevel {
		logrus.Warnf("Updating log level from %v to %v", oldLevel, newLevel)
		logrus.SetLevel(newLevel)
	}

	return nil
}

func (sc *SettingController) syncDefaultLonghornStaticStorageClass() error {
	setting, err := sc.ds.GetSettingWithAutoFillingRO(types.SettingNameDefaultLonghornStaticStorageClass)
	if err != nil {
		return err
	}

	defaultStaticStorageClassName := setting.Value

	definition, ok := types.GetSettingDefinition(types.SettingNameDefaultLonghornStaticStorageClass)
	if !ok {
		return fmt.Errorf("setting %v is not found", types.SettingNameDefaultLonghornStaticStorageClass)
	}

	// Only create the default Longhorn static storage class named 'longhorn-static' if it does not exist
	// And validator will check if the storage class exists when the setting value is not 'longhorn-static'.
	if defaultStaticStorageClassName != definition.Default {
		return nil
	}

	_, err = sc.ds.GetStorageClassRO(defaultStaticStorageClassName)
	if err != nil && apierrors.IsNotFound(err) {
		allowVolumeExpansion := true
		storageClass := &storagev1.StorageClass{
			ObjectMeta: metav1.ObjectMeta{
				Name: defaultStaticStorageClassName,
			},
			Provisioner:          types.LonghornDriverName,
			AllowVolumeExpansion: &allowVolumeExpansion,
			Parameters:           map[string]string{"staleReplicaTimeout": "30"},
		}

		if _, err := sc.ds.CreateStorageClass(storageClass); err != nil {
			return err
		}
		return nil
	}

	return err
}

// updateDataEngine deletes the corresponding instance manager pods immediately if the data engine setting is disabled.
func (sc *SettingController) updateDataEngine(setting types.SettingName) error {
	enabled, err := sc.ds.GetSettingAsBool(setting)
	if err != nil {
		return errors.Wrapf(err, "failed to get %v setting for updating data engine", setting)
	}

	var dataEngine longhorn.DataEngineType
	var ims []*longhorn.InstanceManager

	switch setting {
	case types.SettingNameV1DataEngine:
		dataEngine = longhorn.DataEngineTypeV1
		ims, err = sc.ds.ValidateV1DataEngineEnabled(enabled)
		if err != nil {
			return err
		}
	case types.SettingNameV2DataEngine:
		dataEngine = longhorn.DataEngineTypeV2
		ims, err = sc.ds.ValidateV2DataEngineEnabled(enabled)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown setting %v for updating data engine", setting)
	}

	if !enabled {
		return sc.cleanupInstanceManager(ims, dataEngine)
	}

	return nil
}

func (sc *SettingController) cleanupInstanceManager(ims []*longhorn.InstanceManager, dataEngine longhorn.DataEngineType) error {
	if len(ims) == 0 {
		return nil
	}

	sc.logger.Infof("Cleaning up %v instance managers immediately if all volumes are detached", dataEngine)

	for _, im := range ims {
		sc.logger.Infof("Cleaning up the %v instance manager %v", dataEngine, im.Name)
		if err := sc.ds.DeleteInstanceManager(im.Name); err != nil {
			return err
		}
	}

	return nil
}

func getFinalTolerations(existingTolerations, lastAppliedTolerations, newTolerations map[string]corev1.Toleration) []corev1.Toleration {
	resultMap := make(map[string]corev1.Toleration)

	for k, v := range existingTolerations {
		resultMap[k] = v
	}

	for k := range lastAppliedTolerations {
		delete(resultMap, k)
	}

	for k, v := range newTolerations {
		resultMap[k] = v
	}

	resultSlice := []corev1.Toleration{}
	for _, v := range resultMap {
		resultSlice = append(resultSlice, v)
	}

	return resultSlice
}

// updateNodeSelector deletes all user-deployed and system-managed components immediately with the updated node selector.
func (sc *SettingController) updateNodeSelector() error {
	setting, err := sc.ds.GetSettingWithAutoFillingRO(types.SettingNameSystemManagedComponentsNodeSelector)
	if err != nil {
		return err
	}
	newNodeSelector, err := types.UnmarshalNodeSelector(setting.Value)
	if err != nil {
		return err
	}

	updatingRuntimeObjects, err := sc.collectRuntimeObjects()
	if err != nil {
		return errors.Wrap(err, "failed to collect runtime objects for node selector update")
	}
	notUpdatedNodeSelectorObjs, err := getNotUpdatedNodeSelectorList(newNodeSelector, updatingRuntimeObjects...)
	if err != nil {
		return err
	}
	if len(notUpdatedNodeSelectorObjs) == 0 {
		return nil
	}

	detached, err := sc.ds.AreAllVolumesDetachedState()
	if err != nil {
		return errors.Wrapf(err, "failed to check volume detachment for %v setting update", types.SettingNameSystemManagedComponentsNodeSelector)
	}
	if !detached {
		return &types.ErrorInvalidState{Reason: fmt.Sprintf("failed to apply %v setting to Longhorn components when there are attached volumes. It will be eventually applied", types.SettingNameSystemManagedComponentsNodeSelector)}
	}

	for _, obj := range notUpdatedNodeSelectorObjs {
		switch objType := obj.(type) {
		case *appsv1.DaemonSet:
			ds := obj.(*appsv1.DaemonSet)
			sc.logger.Infof("Updating the node selector from %v to %v for %v", ds.Spec.Template.Spec.NodeSelector, newNodeSelector, ds.Name)
			ds.Spec.Template.Spec.NodeSelector = newNodeSelector
			if _, err := sc.ds.UpdateDaemonSet(ds); err != nil {
				return err
			}
		case *appsv1.Deployment:
			dp := obj.(*appsv1.Deployment)
			sc.logger.Infof("Updating the node selector from %v to %v for %v", dp.Spec.Template.Spec.NodeSelector, newNodeSelector, dp.Name)
			dp.Spec.Template.Spec.NodeSelector = newNodeSelector
			if _, err := sc.ds.UpdateDeployment(dp); err != nil {
				return err
			}
		case *corev1.Pod:
			pod := obj.(*corev1.Pod)
			if pod.DeletionTimestamp == nil {
				sc.logger.Infof("Deleting pod %v to update the node selector from %v to %v", pod.Name, pod.Spec.NodeSelector, newNodeSelector)
				if err := sc.ds.DeletePod(pod.Name); err != nil {
					return err
				}
			}
		default:
			return fmt.Errorf("unknown object type %v when updating %v setting", objType, types.SettingNamePriorityClass)
		}
	}

	return nil
}

// updateSystemManagedCSIComponentsResourceLimits updates CPU/Memory requests/limits for CSI components based on the setting.
// It rolls deployments/daemonset by updating the PodTemplate resources, triggering a rolling update.
func (sc *SettingController) updateSystemManagedCSIComponentsResourceLimits() error {
	setting, err := sc.ds.GetSettingWithAutoFillingRO(types.SettingNameSystemManagedCSIComponentsResourceLimits)
	if err != nil {
		return err
	}
	limits, err := types.UnmarshalCSIComponentResourceLimits(setting.Value)
	if err != nil {
		return err
	}

	// Update CSI sidecar deployments
	dpList, err := sc.ds.ListDeploymentWithLabels(types.GetBaseLabelsForSystemManagedComponent())
	if err != nil {
		return errors.Wrap(err, "failed to list Longhorn deployments for resource limits update")
	}
	for _, dp := range dpList {
		var desired *corev1.ResourceRequirements
		switch dp.Name {
		case types.CSIAttacherName:
			desired = limits.CSIAttacher
		case types.CSIProvisionerName:
			desired = limits.CSIProvisioner
		case types.CSIResizerName:
			desired = limits.CSIResizer
		case types.CSISnapshotterName:
			desired = limits.CSISnapshotter
		default:
			continue
		}
		// Find main container (same as deployment name)
		for i, c := range dp.Spec.Template.Spec.Containers {
			if c.Name != dp.Name {
				continue
			}
			// Setting acts as source of truth: if component is not in setting, clear its resources
			var targetResources corev1.ResourceRequirements
			if desired == nil {
				targetResources = corev1.ResourceRequirements{}
			} else {
				targetResources = *desired
			}
			if reflect.DeepEqual(c.Resources, targetResources) {
				// Already matches, no update needed
				break
			}
			sc.logger.Infof("Updating resources for deployment %v: %+v -> %+v", dp.Name, c.Resources, targetResources)
			dp.Spec.Template.Spec.Containers[i].Resources = targetResources
			if _, err := sc.ds.UpdateDeployment(dp); err != nil {
				return err
			}
			break
		}
	}

	// Update CSI plugin daemonset containers
	dsList, err := sc.ds.ListDaemonSetWithLabels(types.GetBaseLabelsForSystemManagedComponent())
	if err != nil {
		return errors.Wrap(err, "failed to list Longhorn daemonsets for resource limits update")
	}
	for _, ds := range dsList {
		if ds.Name != types.CSIPluginName {
			continue
		}
		// Apply all container changes first, then update once if changed
		changed := false
		for i := range ds.Spec.Template.Spec.Containers {
			c := &ds.Spec.Template.Spec.Containers[i]
			var desired *corev1.ResourceRequirements
			switch c.Name {
			case "node-driver-registrar":
				desired = limits.CSINodeDriverRegistrar
			case "longhorn-liveness-probe":
				desired = limits.CSILivenessProbe
			case types.CSIPluginName:
				desired = limits.CSIPlugin
			default:
				continue
			}
			// Setting acts as source of truth: if component is not in setting, clear its resources
			var targetResources corev1.ResourceRequirements
			if desired == nil {
				targetResources = corev1.ResourceRequirements{}
			} else {
				targetResources = *desired
			}
			if reflect.DeepEqual(c.Resources, targetResources) {
				// Already matches, no update needed
				continue
			}
			sc.logger.Infof("Planned resource update for daemonset %v container %v: %+v -> %+v", ds.Name, c.Name, c.Resources, targetResources)
			c.Resources = targetResources
			changed = true
		}
		if changed {
			sc.logger.Infof("Applying batched resource updates for daemonset %v", ds.Name)
			if _, err := sc.ds.UpdateDaemonSet(ds); err != nil {
				return err
			}
		}
	}

	return nil
}

func getNotUpdatedNodeSelectorList(newNodeSelector map[string]string, objs ...runtime.Object) ([]runtime.Object, error) {
	notUpdatedObjsList := []runtime.Object{}
	var oldNodeSelector map[string]string
	for _, obj := range objs {
		switch objType := obj.(type) {
		case *appsv1.DaemonSet:
			ds := obj.(*appsv1.DaemonSet)
			oldNodeSelector = ds.Spec.Template.Spec.NodeSelector
		case *appsv1.Deployment:
			dp := obj.(*appsv1.Deployment)
			oldNodeSelector = dp.Spec.Template.Spec.NodeSelector
		case *corev1.Pod:
			pod := obj.(*corev1.Pod)
			oldNodeSelector = pod.Spec.NodeSelector
		default:
			return nil, fmt.Errorf("unknown object type %v when updating %v setting", objType, types.SettingNameSystemManagedComponentsNodeSelector)
		}
		if oldNodeSelector == nil && len(newNodeSelector) == 0 {
			continue
		}
		if reflect.DeepEqual(oldNodeSelector, newNodeSelector) {
			continue
		}

		notUpdatedObjsList = append(notUpdatedObjsList, obj)
	}

	return notUpdatedObjsList, nil
}

func (sc *SettingController) syncUpgradeChecker() error {
	upgradeCheckerEnabled, err := sc.ds.GetSettingAsBool(types.SettingNameUpgradeChecker)
	if err != nil {
		return err
	}

	latestLonghornVersion, err := sc.ds.GetSetting(types.SettingNameLatestLonghornVersion)
	if err != nil {
		return err
	}
	stableLonghornVersions, err := sc.ds.GetSetting(types.SettingNameStableLonghornVersions)
	if err != nil {
		return err
	}

	if !upgradeCheckerEnabled {
		if latestLonghornVersion.Value != "" {
			latestLonghornVersion.Value = ""
			if _, err := sc.ds.UpdateSetting(latestLonghornVersion); err != nil {
				return err
			}
		}
		if stableLonghornVersions.Value != "" {
			stableLonghornVersions.Value = ""
			if _, err := sc.ds.UpdateSetting(stableLonghornVersions); err != nil {
				return err
			}
		}
		// reset timestamp so it can be triggered immediately when
		// setting changes next time
		sc.lastUpgradeCheckedTimestamp = time.Time{}
		return nil
	}

	now := time.Now()
	if now.Before(sc.lastUpgradeCheckedTimestamp.Add(upgradeCheckInterval)) {
		return nil
	}

	currentLatestVersion := latestLonghornVersion.Value
	currentStableVersions := stableLonghornVersions.Value
	latestLonghornVersion.Value, stableLonghornVersions.Value, err = sc.CheckLatestAndStableLonghornVersions()
	if err != nil {
		// non-critical error, don't retry
		sc.logger.WithError(err).Warn("Failed to check for the latest and stable Longhorn versions")
		return nil
	}

	sc.lastUpgradeCheckedTimestamp = now

	if latestLonghornVersion.Value != currentLatestVersion {
		sc.logger.Infof("Latest Longhorn version is %v", latestLonghornVersion.Value)
		if _, err := sc.ds.UpdateSetting(latestLonghornVersion); err != nil {
			// non-critical error, don't retry
			sc.logger.WithError(err).Warn("Failed to update latest Longhorn version")
			return nil
		}
	}
	if stableLonghornVersions.Value != currentStableVersions {
		sc.logger.Infof("The latest stable version of every minor release line: %v", stableLonghornVersions.Value)
		if _, err := sc.ds.UpdateSetting(stableLonghornVersions); err != nil {
			// non-critical error, don't retry
			sc.logger.WithError(err).Warn("Failed to update stable Longhorn versions")
			return nil
		}
	}

	return nil
}

func (sc *SettingController) CheckLatestAndStableLonghornVersions() (string, string, error) {
	var (
		resp    CheckUpgradeResponse
		content bytes.Buffer
	)

	extraTagInfo, extraFieldInfo, err := sc.GetCheckUpgradeRequestExtraInfo()
	if err != nil {
		return "", "", errors.Wrap(err, "failed to get extra info for upgrade checker")
	}

	version := sc.version
	if strings.Contains(version, "dev") {
		version = "dev"
	}
	req := &CheckUpgradeRequest{
		AppVersion:     version,
		ExtraTagInfo:   extraTagInfo,
		ExtraFieldInfo: extraFieldInfo,
	}
	if err := json.NewEncoder(&content).Encode(req); err != nil {
		return "", "", err
	}

	upgradeResponderURL, err := sc.ds.GetSettingValueExisted(types.SettingNameUpgradeResponderURL)
	if err != nil {
		return "", "", err
	}

	r, err := http.Post(upgradeResponderURL, "application/json", &content)
	if err != nil {
		return "", "", err
	}
	defer func(body io.ReadCloser) {
		if closeErr := body.Close(); closeErr != nil {
			sc.logger.WithError(closeErr).Warn("Failed to close upgrade responder response body")
		}
	}(r.Body)
	if r.StatusCode != http.StatusOK {
		message := ""
		messageBytes, err := io.ReadAll(r.Body)
		if err != nil {
			message = err.Error()
		} else {
			message = string(messageBytes)
		}
		return "", "", fmt.Errorf("query return status code %v, message %v", r.StatusCode, message)
	}
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		return "", "", err
	}

	latestVersion := ""
	stableVersions := []string{}
	for _, v := range resp.Versions {
		for _, tag := range v.Tags {
			if tag == VersionTagLatest {
				latestVersion = v.Name
			}
			if tag == VersionTagStable {
				stableVersions = append(stableVersions, v.Name)
			}
		}
	}
	if latestVersion == "" {
		return "", "", fmt.Errorf("failed to find latest Longhorn version during CheckLatestAndStableLonghornVersions")
	}
	sort.Strings(stableVersions)
	return latestVersion, strings.Join(stableVersions, ","), nil
}

func (sc *SettingController) enqueueSetting(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("failed to get key for object %#v: %v", obj, err))
		return
	}

	sc.queue.Add(key)
}

func (sc *SettingController) enqueueSettingForNode(obj interface{}) {
	if _, ok := obj.(*longhorn.Node); !ok {
		// Ignore deleted node
		return
	}

	sc.queue.Add(sc.namespace + "/" + string(types.SettingNameGuaranteedInstanceManagerCPU))
}

// updateInstanceManagerCPURequest deletes all instance manager pods immediately with the updated CPU request.
func (sc *SettingController) updateInstanceManagerCPURequest(dataEngine longhorn.DataEngineType) error {
	imPodList, err := sc.ds.ListInstanceManagerPodsBy("", "", longhorn.InstanceManagerTypeAllInOne, dataEngine)
	if err != nil {
		return errors.Wrap(err, "failed to list instance manager pods for toleration update")
	}
	imMap, err := sc.ds.ListInstanceManagersBySelectorRO("", "", longhorn.InstanceManagerTypeAllInOne, dataEngine)
	if err != nil {
		return err
	}
	notUpdatedPods := []*corev1.Pod{}

	for _, imPod := range imPodList {
		if _, exists := imMap[imPod.Name]; !exists {
			continue
		}
		lhNode, err := sc.ds.GetNode(imPod.Spec.NodeName)
		if err != nil {
			return err
		}
		if types.GetCondition(lhNode.Status.Conditions, longhorn.NodeConditionTypeReady).Status != longhorn.ConditionStatusTrue {
			continue
		}

		resourceReq, err := GetInstanceManagerCPURequirement(sc.ds, imPod.Name)
		if err != nil {
			return err
		}
		podResourceReq := imPod.Spec.Containers[0].Resources
		if IsSameGuaranteedCPURequirement(resourceReq, &podResourceReq) {
			continue
		}

		notUpdatedPods = append(notUpdatedPods, imPod)
	}

	if len(notUpdatedPods) == 0 {
		return nil
	}

	stopped, _, err := sc.ds.AreAllEngineInstancesStopped(dataEngine)
	if err != nil {
		return errors.Wrapf(err, "failed to check engine instances for %v setting update for data engine %v", types.SettingNameGuaranteedInstanceManagerCPU, dataEngine)
	}
	if !stopped {
		return &types.ErrorInvalidState{Reason: fmt.Sprintf("failed to apply %v setting for data engine %v to Longhorn components when there are running engine instances. It will be eventually applied", types.SettingNameGuaranteedInstanceManagerCPU, dataEngine)}
	}

	for _, pod := range notUpdatedPods {
		sc.logger.Infof("Deleting instance manager pod %v to refresh CPU request option", pod.Name)
		if err := sc.ds.DeletePod(pod.Name); err != nil {
			return err
		}
	}

	return nil
}

func (sc *SettingController) cleanupFailedSupportBundles() error {
	failedLimit, err := sc.ds.GetSettingAsInt(types.SettingNameSupportBundleFailedHistoryLimit)
	if err != nil {
		return err
	}

	if failedLimit > 0 {
		return nil
	}

	supportBundleList, err := sc.ds.ListSupportBundles()
	if err != nil {
		return errors.Wrap(err, "failed to list SupportBundles for auto-deletion")
	}

	for _, supportBundle := range supportBundleList {
		if supportBundle.Status.OwnerID != sc.controllerID {
			continue
		}

		if supportBundle.Status.State == longhorn.SupportBundleStatePurging {
			continue
		}
		if supportBundle.Status.State == longhorn.SupportBundleStateDeleting {
			continue
		}
		if supportBundle.Status.State != longhorn.SupportBundleStateError {
			continue
		}

		supportBundle.Status.State = longhorn.SupportBundleStatePurging
		_, err = sc.ds.UpdateSupportBundleStatus(supportBundle)
		if err != nil && !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "failed to purge SupportBundle %v", supportBundle.Name)
		}

		message := fmt.Sprintf("Purging failed SupportBundle %v", supportBundle.Name)
		sc.logger.Info(message)
		sc.eventRecorder.Eventf(supportBundle, corev1.EventTypeNormal, constant.EventReasonDeleting, message)
	}

	return nil
}

func (sc *SettingController) GetCheckUpgradeRequestExtraInfo() (extraTagInfo CheckUpgradeExtraInfo, extraFieldInfo CheckUpgradeExtraInfo, err error) {
	clusterInfo := &ClusterInfo{
		logger:        sc.logger,
		ds:            sc.ds,
		kubeClient:    sc.kubeClient,
		metricsClient: sc.metricsClient,
		structFields: ClusterInfoStructFields{
			tags:   util.StructFields{},
			fields: util.StructFields{},
		},
		controllerID: sc.controllerID,
		namespace:    sc.namespace,
	}

	defer func() {
		extraTagInfo = clusterInfo.structFields.tags.NewStruct()
		extraFieldInfo = clusterInfo.structFields.fields.NewStruct()
	}()

	kubeVersion, err := sc.kubeClient.Discovery().ServerVersion()
	if err != nil {
		err = errors.Wrap(err, "failed to get Kubernetes server version")
		return
	}
	clusterInfo.structFields.tags.Append(ClusterInfoKubernetesVersion, kubeVersion.GitVersion)

	allowCollectingUsage, err := sc.ds.GetSettingAsBool(types.SettingNameAllowCollectingLonghornUsage)
	if err != nil {
		sc.logger.WithError(err).Warnf("Failed to get Setting %v for extra info collection", types.SettingNameAllowCollectingLonghornUsage)
		return nil, nil, nil
	}

	if !allowCollectingUsage {
		return
	}

	clusterInfo.collectNodeScope()

	responsibleNodeID, err := getResponsibleNodeID(sc.ds)
	if err != nil {
		sc.logger.WithError(err).Warn("Failed to get responsible Node for extra info collection")
		return nil, nil, nil
	}
	if responsibleNodeID == sc.controllerID {
		clusterInfo.collectClusterScope()
	}

	return
}

// Cluster Scope Info: will be sent from one of the Longhorn cluster nodes
const (
	ClusterInfoNamespaceUID = util.StructName("LonghornNamespaceUid")
	ClusterInfoNodeCount    = util.StructName("LonghornNodeCount")

	ClusterInfoBackingImageCount      = util.StructName("LonghornBackingImageCount")
	ClusterInfoOrphanCount            = util.StructName("LonghornOrphanCount")
	ClusterInfoVolumeAvgActualSize    = util.StructName("LonghornVolumeAverageActualSizeBytes")
	ClusterInfoVolumeAvgSize          = util.StructName("LonghornVolumeAverageSizeBytes")
	ClusterInfoVolumeAvgSnapshotCount = util.StructName("LonghornVolumeAverageSnapshotCount")
	ClusterInfoVolumeAvgNumOfReplicas = util.StructName("LonghornVolumeAverageNumberOfReplicas")
	ClusterInfoVolumeNumOfReplicas    = util.StructName("LonghornVolumeNumberOfReplicas")
	ClusterInfoVolumeNumOfSnapshots   = util.StructName("LonghornVolumeNumberOfSnapshots")

	ClusterInfoBackupTargetSchemeCountFmt                            = "LonghornBackupTarget%sCount"
	ClusterInfoPodAvgCPUUsageFmt                                     = "Longhorn%sAverageCpuUsageMilliCores"
	ClusterInfoPodAvgMemoryUsageFmt                                  = "Longhorn%sAverageMemoryUsageBytes"
	ClusterInfoSettingFmt                                            = "LonghornSetting%s"
	ClusterInfoVolumeAccessModeCountFmt                              = "LonghornVolumeAccessMode%sCount"
	ClusterInfoVolumeDataEngineCountFmt                              = "LonghornVolumeDataEngine%sCount"
	ClusterInfoVolumeDataLocalityCountFmt                            = "LonghornVolumeDataLocality%sCount"
	ClusterInfoVolumeEncryptedCountFmt                               = "LonghornVolumeEncrypted%sCount"
	ClusterInfoVolumeFrontendCountFmt                                = "LonghornVolumeFrontend%sCount"
	ClusterInfoVolumeReplicaAutoBalanceCountFmt                      = "LonghornVolumeReplicaAutoBalance%sCount"
	ClusterInfoVolumeReplicaSoftAntiAffinityCountFmt                 = "LonghornVolumeReplicaSoftAntiAffinity%sCount"
	ClusterInfoVolumeReplicaZoneSoftAntiAffinityCountFmt             = "LonghornVolumeReplicaZoneSoftAntiAffinity%sCount"
	ClusterInfoVolumeReplicaDiskSoftAntiAffinityCountFmt             = "LonghornVolumeReplicaDiskSoftAntiAffinity%sCount"
	ClusterInfoVolumeRestoreVolumeRecurringJobCountFmt               = "LonghornVolumeRestoreVolumeRecurringJob%sCount"
	ClusterInfoVolumeSnapshotDataIntegrityCountFmt                   = "LonghornVolumeSnapshotDataIntegrity%sCount"
	ClusterInfoVolumeUnmapMarkSnapChainRemovedCountFmt               = "LonghornVolumeUnmapMarkSnapChainRemoved%sCount"
	ClusterInfoVolumeFreezeFilesystemForV1DataEngineSnapshotCountFmt = "LonghornVolumeFreezeFilesystemForV1DataEngineSnapshot%sCount"
)

// Node Scope Info: will be sent from all Longhorn cluster nodes
const (
	ClusterInfoKubernetesVersion      = util.StructName("KubernetesVersion")
	ClusterInfoKubernetesNodeProvider = util.StructName("KubernetesNodeProvider")

	ClusterInfoHostArch          = util.StructName("HostArch")
	ClusterInfoHostKernelRelease = util.StructName("HostKernelRelease")
	ClusterInfoHostOsDistro      = util.StructName("HostOsDistro")

	ClusterInfoLonghornImageRegistry = util.StructName("LonghornImageRegistry")

	ClusterInfoDiskCountFmt     = "LonghornDisk%sCount"
	ClusterInfoNodeDiskCountFmt = "LonghornNodeDisk%sCount"
)

// ClusterInfo struct is used to collect information about the cluster.
// This provides additional usage metrics to https://metrics.longhorn.io.
type ClusterInfo struct {
	logger logrus.FieldLogger

	ds *datastore.DataStore

	kubeClient    clientset.Interface
	metricsClient metricsclientset.Interface

	structFields ClusterInfoStructFields

	controllerID string
	namespace    string

	osDistro string

	imageRegistry string
}

type ClusterInfoStructFields struct {
	tags   util.StructFields
	fields util.StructFields
}

func (info *ClusterInfo) collectClusterScope() {
	if err := info.collectNamespace(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect Longhorn namespace")
	}

	if err := info.collectLonghornImageRegistry(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect Longhorn image registry")
	}

	if err := info.collectNodeCount(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect number of Longhorn nodes")
	}

	if err := info.collectResourceUsage(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect Longhorn resource usages")
	}

	if err := info.collectVolumesInfo(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect Longhorn volumes info")
	}

	if err := info.collectSettings(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect Longhorn settings")
	}

	if err := info.collectBackingImageInfo(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect Longhorn backing images info")
	}

	if err := info.collectOrphanInfo(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect Longhorn orphan info")
	}

	if err := info.collectBackupTargetInfo(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect Longhorn backup target info")
	}
}

func (info *ClusterInfo) collectNamespace() error {
	namespace, err := info.ds.GetNamespace(info.namespace)
	if err == nil {
		info.structFields.fields.Append(ClusterInfoNamespaceUID, string(namespace.UID))
	}
	return err
}

func (info *ClusterInfo) collectNodeCount() error {
	nodes, err := info.ds.ListNodesRO()
	if err == nil {
		info.structFields.fields.Append(ClusterInfoNodeCount, len(nodes))
	}
	return err
}

func (info *ClusterInfo) collectResourceUsage() error {
	componentMap := map[string]map[string]string{
		"Manager":         info.ds.GetManagerLabel(),
		"InstanceManager": types.GetInstanceManagerComponentLabel(),
	}

	metricsClient := info.metricsClient.MetricsV1beta1()
	for component, label := range componentMap {
		selector, err := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{
			MatchLabels: label,
		})
		if err != nil {
			logrus.WithError(err).Warnf("Failed to get %v label for %v", label, component)
			continue
		}

		pods, err := info.ds.ListPodsBySelector(selector)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to list %v Pod by %v label", component, label)
			continue
		}
		podCount := len(pods)

		if podCount == 0 {
			continue
		}

		var totalCPUUsage resource.Quantity
		var totalMemoryUsage resource.Quantity
		for _, pod := range pods {
			podMetrics, err := metricsClient.PodMetricses(info.namespace).Get(context.TODO(), pod.Name, metav1.GetOptions{})
			if err != nil && !apierrors.IsNotFound(err) {
				logrus.WithError(err).Warnf("Failed to get %v Pod", pod.Name)
				continue
			}
			for _, container := range podMetrics.Containers {
				totalCPUUsage.Add(*container.Usage.Cpu())
				totalMemoryUsage.Add(*container.Usage.Memory())
			}
		}

		avgCPUUsageMilli := totalCPUUsage.MilliValue() / int64(podCount)
		cpuStruct := util.StructName(fmt.Sprintf(ClusterInfoPodAvgCPUUsageFmt, component))
		info.structFields.fields.Append(cpuStruct, avgCPUUsageMilli)

		avgMemoryUsageBytes := totalMemoryUsage.Value() / int64(podCount)
		memStruct := util.StructName(fmt.Sprintf(ClusterInfoPodAvgMemoryUsageFmt, component))
		info.structFields.fields.Append(memStruct, avgMemoryUsageBytes)
	}

	return nil
}

func (info *ClusterInfo) collectSettings() error {
	includeAsBoolean := map[types.SettingName]bool{
		types.SettingNameTaintToleration:                     true,
		types.SettingNameSystemManagedComponentsNodeSelector: true,
		types.SettingNameRegistrySecret:                      true,
		types.SettingNamePriorityClass:                       true,
		types.SettingNameSnapshotDataIntegrityCronJob:        true,
		types.SettingNameStorageNetwork:                      true,
	}

	include := map[types.SettingName]bool{
		types.SettingNameAllowRecurringJobWhileVolumeDetached:                     true,
		types.SettingNameAllowVolumeCreationWithDegradedAvailability:              true,
		types.SettingNameAutoCleanupSystemGeneratedSnapshot:                       true,
		types.SettingNameAutoDeletePodWhenVolumeDetachedUnexpectedly:              true,
		types.SettingNameAutoSalvage:                                              true,
		types.SettingNameBackingImageCleanupWaitInterval:                          true,
		types.SettingNameBackingImageRecoveryWaitInterval:                         true,
		types.SettingNameBackupCompressionMethod:                                  true,
		types.SettingNameBackupConcurrentLimit:                                    true,
		types.SettingNameConcurrentAutomaticEngineUpgradePerNodeLimit:             true,
		types.SettingNameConcurrentBackupRestorePerNodeLimit:                      true,
		types.SettingNameConcurrentReplicaRebuildPerNodeLimit:                     true,
		types.SettingNameReplicaRebuildConcurrentSyncLimit:                        true,
		types.SettingNameConcurrentBackingImageCopyReplenishPerNodeLimit:          true,
		types.SettingNameCRDAPIVersion:                                            true,
		types.SettingNameCreateDefaultDiskLabeledNodes:                            true,
		types.SettingNameDefaultDataLocality:                                      true,
		types.SettingNameDefaultMinNumberOfBackingImageCopies:                     true,
		types.SettingNameDefaultReplicaCount:                                      true,
		types.SettingNameDisableRevisionCounter:                                   true,
		types.SettingNameDisableSchedulingOnCordonedNode:                          true,
		types.SettingNameEngineReplicaTimeout:                                     true,
		types.SettingNameFailedBackupTTL:                                          true,
		types.SettingNameFastReplicaRebuildEnabled:                                true,
		types.SettingNameGuaranteedInstanceManagerCPU:                             true,
		types.SettingNameKubernetesClusterAutoscalerEnabled:                       true,
		types.SettingNameNodeDownPodDeletionPolicy:                                true,
		types.SettingNameNodeDrainPolicy:                                          true,
		types.SettingNameOrphanResourceAutoDeletion:                               true,
		types.SettingNameRecurringFailedJobsHistoryLimit:                          true,
		types.SettingNameRecurringSuccessfulJobsHistoryLimit:                      true,
		types.SettingNameRemoveSnapshotsDuringFilesystemTrim:                      true,
		types.SettingNameReplicaAutoBalance:                                       true,
		types.SettingNameReplicaAutoBalanceDiskPressurePercentage:                 true,
		types.SettingNameReplicaFileSyncHTTPClientTimeout:                         true,
		types.SettingNameReplicaReplenishmentWaitInterval:                         true,
		types.SettingNameReplicaSoftAntiAffinity:                                  true,
		types.SettingNameReplicaZoneSoftAntiAffinity:                              true,
		types.SettingNameReplicaDiskSoftAntiAffinity:                              true,
		types.SettingNameRestoreConcurrentLimit:                                   true,
		types.SettingNameRestoreVolumeRecurringJobs:                               true,
		types.SettingNameRWXVolumeFastFailover:                                    true,
		types.SettingNameSnapshotDataIntegrity:                                    true,
		types.SettingNameSnapshotDataIntegrityImmediateCheckAfterSnapshotCreation: true,
		types.SettingNameStorageMinimalAvailablePercentage:                        true,
		types.SettingNameStorageOverProvisioningPercentage:                        true,
		types.SettingNameStorageReservedPercentageForDefaultDisk:                  true,
		types.SettingNameSupportBundleFailedHistoryLimit:                          true,
		types.SettingNameSupportBundleNodeCollectionTimeout:                       true,
		types.SettingNameSystemManagedPodsImagePullPolicy:                         true,
		types.SettingNameDefaultBackupBlockSize:                                   true,
		types.SettingNameReplicaRebuildingBandwidthLimit:                          true,
		types.SettingNameDefaultUblkQueueDepth:                                    true,
		types.SettingNameDefaultUblkNumberOfQueue:                                 true,
		types.SettingNameV1DataEngine:                                             true,
		types.SettingNameV2DataEngine:                                             true,
	}

	settings, err := info.ds.ListSettings()
	if err != nil {
		return err
	}

	settingMap := make(map[string]interface{})
	for _, setting := range settings {
		settingName := types.SettingName(setting.Name)

		switch {
		// Setting that should be collected as boolean (true if configured, false if not)
		case includeAsBoolean[settingName]:
			definition, ok := types.GetSettingDefinition(types.SettingName(setting.Name))
			if !ok {
				logrus.WithError(err).Warnf("Failed to get Setting %v definition", setting.Name)
				continue
			}
			settingMap[setting.Name] = setting.Value != definition.Default

		// Setting value
		case include[settingName]:
			convertedValue, err := info.convertSettingValueType(setting)
			if err != nil {
				logrus.WithError(err).Warnf("Failed to convert Setting %v value", setting.Name)
				continue
			}
			settingMap[setting.Name] = convertedValue
		}

		if value, ok := settingMap[setting.Name]; ok && value == "" {
			settingMap[setting.Name] = types.ValueEmpty
		}
	}

	for name, value := range settingMap {
		structName := util.StructName(fmt.Sprintf(ClusterInfoSettingFmt, util.ConvertToCamel(name, "-")))

		switch reflect.TypeOf(value).Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64, reflect.Float32, reflect.Float64:
			info.structFields.fields.Append(structName, value)
		default:
			info.structFields.tags.Append(structName, fmt.Sprint(value))
		}
	}
	return nil
}

func (info *ClusterInfo) convertSettingValueType(setting *longhorn.Setting) (convertedValue interface{}, err error) {
	definition, ok := types.GetSettingDefinition(types.SettingName(setting.Name))
	if !ok {
		return false, fmt.Errorf("failed to get Setting %v definition", setting.Name)
	}

	switch definition.Type {
	case types.SettingTypeInt:
		if !definition.DataEngineSpecific {
			return strconv.ParseInt(setting.Value, 10, 64)
		}
	case types.SettingTypeBool:
		if !definition.DataEngineSpecific {
			return strconv.ParseBool(setting.Value)
		}
	}
	return setting.Value, nil
}

func (info *ClusterInfo) collectVolumesInfo() error {
	volumesRO, err := info.ds.ListVolumesRO()
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn Volumes")
	}
	volumeCount := len(volumesRO)
	volumeCountV1 := 0
	for _, volume := range volumesRO {
		if types.IsDataEngineV1(volume.Spec.DataEngine) {
			volumeCountV1++
		}
	}

	var totalVolumeSize int
	var totalVolumeActualSize int
	var totalVolumeNumOfReplicas int
	newStruct := func() map[util.StructName]int { return make(map[util.StructName]int, volumeCount) }
	accessModeCountStruct := newStruct()
	dataEngineCountStruct := newStruct()
	dataLocalityCountStruct := newStruct()
	encryptedCountStruct := newStruct()
	frontendCountStruct := newStruct()
	replicaAutoBalanceCountStruct := newStruct()
	replicaSoftAntiAffinityCountStruct := newStruct()
	replicaZoneSoftAntiAffinityCountStruct := newStruct()
	replicaDiskSoftAntiAffinityCountStruct := newStruct()
	restoreVolumeRecurringJobCountStruct := newStruct()
	snapshotDataIntegrityCountStruct := newStruct()
	unmapMarkSnapChainRemovedCountStruct := newStruct()
	freezeFilesystemForSnapshotCountStruct := newStruct()
	for _, volume := range volumesRO {
		dataEngine := types.ValueUnknown
		if volume.Spec.DataEngine != "" {
			dataEngine = util.ConvertToCamel(string(volume.Spec.DataEngine), "-")
		}
		dataEngineCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeDataEngineCountFmt, dataEngine))]++

		// TODO: Remove this condition when v2 volume actual size is implemented.
		//       https://github.com/longhorn/longhorn/issues/5947
		isVolumeUsingV2DataEngine := types.IsDataEngineV2(volume.Spec.DataEngine)
		if !isVolumeUsingV2DataEngine {
			totalVolumeSize += int(volume.Spec.Size)
			totalVolumeActualSize += int(volume.Status.ActualSize)
		}
		totalVolumeNumOfReplicas += volume.Spec.NumberOfReplicas

		accessMode := types.ValueUnknown
		if volume.Spec.AccessMode != "" {
			accessMode = util.ConvertToCamel(string(volume.Spec.AccessMode), "-")
		}
		accessModeCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeAccessModeCountFmt, accessMode))]++

		dataLocality := types.ValueUnknown
		if volume.Spec.DataLocality != "" {
			dataLocality = util.ConvertToCamel(string(volume.Spec.DataLocality), "-")
		}
		dataLocalityCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeDataLocalityCountFmt, dataLocality))]++

		encrypted := util.ConvertToCamel(strconv.FormatBool(volume.Spec.Encrypted), "-")
		encryptedCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeEncryptedCountFmt, encrypted))]++

		if volume.Spec.Frontend != "" && !isVolumeUsingV2DataEngine {
			frontend := util.ConvertToCamel(string(volume.Spec.Frontend), "-")
			frontendCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeFrontendCountFmt, frontend))]++
		}

		replicaAutoBalance := info.collectSettingInVolume(string(volume.Spec.ReplicaAutoBalance), string(longhorn.ReplicaAutoBalanceIgnored), volume.Spec.DataEngine, types.SettingNameReplicaAutoBalance)
		replicaAutoBalanceCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeReplicaAutoBalanceCountFmt, util.ConvertToCamel(string(replicaAutoBalance), "-")))]++

		replicaSoftAntiAffinity := info.collectSettingInVolume(string(volume.Spec.ReplicaSoftAntiAffinity), string(longhorn.ReplicaSoftAntiAffinityDefault), volume.Spec.DataEngine, types.SettingNameReplicaSoftAntiAffinity)
		replicaSoftAntiAffinityCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeReplicaSoftAntiAffinityCountFmt, util.ConvertToCamel(string(replicaSoftAntiAffinity), "-")))]++

		replicaZoneSoftAntiAffinity := info.collectSettingInVolume(string(volume.Spec.ReplicaZoneSoftAntiAffinity), string(longhorn.ReplicaZoneSoftAntiAffinityDefault), volume.Spec.DataEngine, types.SettingNameReplicaZoneSoftAntiAffinity)
		replicaZoneSoftAntiAffinityCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeReplicaZoneSoftAntiAffinityCountFmt, util.ConvertToCamel(string(replicaZoneSoftAntiAffinity), "-")))]++

		replicaDiskSoftAntiAffinity := info.collectSettingInVolume(string(volume.Spec.ReplicaDiskSoftAntiAffinity), string(longhorn.ReplicaDiskSoftAntiAffinityDefault), volume.Spec.DataEngine, types.SettingNameReplicaDiskSoftAntiAffinity)
		replicaDiskSoftAntiAffinityCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeReplicaDiskSoftAntiAffinityCountFmt, util.ConvertToCamel(string(replicaDiskSoftAntiAffinity), "-")))]++

		restoreVolumeRecurringJob := info.collectSettingInVolume(string(volume.Spec.RestoreVolumeRecurringJob), string(longhorn.RestoreVolumeRecurringJobDefault), volume.Spec.DataEngine, types.SettingNameRestoreVolumeRecurringJobs)
		restoreVolumeRecurringJobCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeRestoreVolumeRecurringJobCountFmt, util.ConvertToCamel(string(restoreVolumeRecurringJob), "-")))]++

		snapshotDataIntegrity := info.collectSettingInVolume(string(volume.Spec.SnapshotDataIntegrity), string(longhorn.SnapshotDataIntegrityIgnored), volume.Spec.DataEngine, types.SettingNameSnapshotDataIntegrity)
		snapshotDataIntegrityCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeSnapshotDataIntegrityCountFmt, util.ConvertToCamel(string(snapshotDataIntegrity), "-")))]++

		unmapMarkSnapChainRemoved := info.collectSettingInVolume(string(volume.Spec.UnmapMarkSnapChainRemoved), string(longhorn.UnmapMarkSnapChainRemovedIgnored), volume.Spec.DataEngine, types.SettingNameRemoveSnapshotsDuringFilesystemTrim)
		unmapMarkSnapChainRemovedCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeUnmapMarkSnapChainRemovedCountFmt, util.ConvertToCamel(string(unmapMarkSnapChainRemoved), "-")))]++

		if types.IsDataEngineV1(volume.Spec.DataEngine) {
			freezeFilesystemForSnapshot := info.collectSettingInVolume(string(volume.Spec.FreezeFilesystemForSnapshot), string(longhorn.FreezeFilesystemForSnapshotDefault), volume.Spec.DataEngine, types.SettingNameFreezeFilesystemForSnapshot)
			freezeFilesystemForSnapshotCountStruct[util.StructName(fmt.Sprintf(ClusterInfoVolumeFreezeFilesystemForV1DataEngineSnapshotCountFmt, util.ConvertToCamel(string(freezeFilesystemForSnapshot), "-")))]++
		}
	}
	info.structFields.fields.Append(ClusterInfoVolumeNumOfReplicas, totalVolumeNumOfReplicas)
	info.structFields.fields.AppendCounted(accessModeCountStruct)
	info.structFields.fields.AppendCounted(dataEngineCountStruct)
	info.structFields.fields.AppendCounted(dataLocalityCountStruct)
	info.structFields.fields.AppendCounted(encryptedCountStruct)
	info.structFields.fields.AppendCounted(frontendCountStruct)
	info.structFields.fields.AppendCounted(replicaAutoBalanceCountStruct)
	info.structFields.fields.AppendCounted(replicaSoftAntiAffinityCountStruct)
	info.structFields.fields.AppendCounted(replicaZoneSoftAntiAffinityCountStruct)
	info.structFields.fields.AppendCounted(replicaDiskSoftAntiAffinityCountStruct)
	info.structFields.fields.AppendCounted(restoreVolumeRecurringJobCountStruct)
	info.structFields.fields.AppendCounted(snapshotDataIntegrityCountStruct)
	info.structFields.fields.AppendCounted(unmapMarkSnapChainRemovedCountStruct)
	info.structFields.fields.AppendCounted(freezeFilesystemForSnapshotCountStruct)

	// TODO: Use the total volume count instead when v2 volume actual size is implemented.
	//       https://github.com/longhorn/longhorn/issues/5947
	var avgVolumeSize int
	var avgVolumeActualSize int
	if volumeCountV1 > 0 && totalVolumeSize > 0 {
		avgVolumeSize = totalVolumeSize / volumeCountV1

		if totalVolumeActualSize > 0 {
			avgVolumeActualSize = totalVolumeActualSize / volumeCountV1
		}
	}

	var avgVolumeSnapshotCount int
	var avgVolumeNumOfReplicas int
	var totalVolumeNumOfSnapshots int
	if volumeCount > 0 {
		avgVolumeNumOfReplicas = totalVolumeNumOfReplicas / volumeCount

		snapshotsRO, err := info.ds.ListSnapshotsRO(labels.Everything())
		if err != nil {
			return errors.Wrapf(err, "failed to list Longhorn Snapshots")
		}
		totalVolumeNumOfSnapshots = len(snapshotsRO)
		avgVolumeSnapshotCount = totalVolumeNumOfSnapshots / volumeCount
	}
	info.structFields.fields.Append(ClusterInfoVolumeAvgSize, avgVolumeSize)
	info.structFields.fields.Append(ClusterInfoVolumeAvgActualSize, avgVolumeActualSize)
	info.structFields.fields.Append(ClusterInfoVolumeAvgSnapshotCount, avgVolumeSnapshotCount)
	info.structFields.fields.Append(ClusterInfoVolumeAvgNumOfReplicas, avgVolumeNumOfReplicas)
	info.structFields.fields.Append(ClusterInfoVolumeNumOfSnapshots, totalVolumeNumOfSnapshots)

	return nil
}

func (info *ClusterInfo) collectSettingInVolume(volumeSpecValue, ignoredValue string, dataEngine longhorn.DataEngineType, settingName types.SettingName) string {
	if volumeSpecValue == ignoredValue {
		globalSettingValue, err := info.ds.GetSettingValueExistedByDataEngine(settingName, dataEngine)
		if err != nil {
			info.logger.WithError(err).Warnf("Failed to get Longhorn Setting %v", settingName)
		}

		return globalSettingValue
	}
	return volumeSpecValue
}

func (info *ClusterInfo) collectBackingImageInfo() error {
	backingImages, err := info.ds.ListBackingImagesRO()
	if err == nil {
		info.structFields.fields.Append(ClusterInfoBackingImageCount, len(backingImages))
	}
	return err
}

func (info *ClusterInfo) collectOrphanInfo() error {
	orphans, err := info.ds.ListOrphansRO()
	if err == nil {
		info.structFields.fields.Append(ClusterInfoOrphanCount, len(orphans))
	}
	return err
}

func (info *ClusterInfo) collectBackupTargetInfo() error {
	backupTargets, err := info.ds.ListBackupTargetsRO()
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn Backup Targets")
	}

	backupTargetDriverCountStruct := make(map[util.StructName]int, len(backupTargets))
	for _, backupTarget := range backupTargets {
		backupTargetScheme := types.GetBackupTargetSchemeFromURL(backupTarget.Spec.BackupTargetURL)
		backupTargetDriverCountStruct[util.StructName(fmt.Sprintf(ClusterInfoBackupTargetSchemeCountFmt, util.ConvertToCamel(backupTargetScheme, "-")))]++
	}
	info.structFields.fields.AppendCounted(backupTargetDriverCountStruct)

	return err
}

func (info *ClusterInfo) collectNodeScope() {
	if err := info.collectHostArch(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect host architecture")
	}

	if err := info.collectHostKernelRelease(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect host kernel release")
	}

	if err := info.collectHostOSDistro(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect host OS distro")
	}

	if err := info.collectNodeDiskCount(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect number of node disks")
	}

	if err := info.collectKubernetesNodeProvider(); err != nil {
		info.logger.WithError(err).Warn("Failed to collect node provider")
	}
}

func (info *ClusterInfo) collectHostArch() error {
	arch, err := lhns.GetArch()
	if err == nil {
		info.structFields.tags.Append(ClusterInfoHostArch, arch)
	}
	return err
}

func (info *ClusterInfo) collectHostKernelRelease() error {
	kernelRelease, err := lhns.GetKernelRelease()
	if err == nil {
		info.structFields.tags.Append(ClusterInfoHostKernelRelease, kernelRelease)
	}
	return err
}

func (info *ClusterInfo) collectHostOSDistro() (err error) {
	if info.osDistro == "" {
		info.osDistro, err = lhns.GetOSDistro()
		if err != nil {
			return err
		}
	}
	info.structFields.tags.Append(ClusterInfoHostOsDistro, info.osDistro)
	return nil
}

// getRegistry returns the registry + namespace from a container image string.
func getRegistry(image string) string {
	parts := strings.Split(image, "/")

	switch len(parts) {
	case 1:
		// No registry and namespace, default to docker.io/library
		return "docker.io/library"
	case 2:
		// No explicit registry, default to docker.io
		return "docker.io/" + parts[0]
	default:
		// parts[0] is the registry, parts[1] is the namespace
		return parts[0] + "/" + parts[1]
	}
}

func (info *ClusterInfo) collectLonghornImageRegistry() (err error) {
	if info.imageRegistry == "" {
		pod, err := info.ds.GetManagerPodForNode(info.controllerID)
		if err != nil {
			return err
		}
		if len(pod.Spec.Containers) == 0 {
			return fmt.Errorf("no container found in manager pod %v", pod.Name)
		}

		info.imageRegistry = getRegistry(pod.Spec.Containers[0].Image)
	}
	info.structFields.tags.Append(ClusterInfoLonghornImageRegistry, info.imageRegistry)
	return nil
}

func (info *ClusterInfo) collectKubernetesNodeProvider() error {
	node, err := info.ds.GetKubernetesNodeRO(info.controllerID)
	if err == nil {
		scheme := types.GetKubernetesProviderNameFromURL(node.Spec.ProviderID)
		info.structFields.tags.Append(ClusterInfoKubernetesNodeProvider, scheme)
	}
	return err
}

func (info *ClusterInfo) collectNodeDiskCount() error {
	node, err := info.ds.GetNodeRO(info.controllerID)
	if err != nil {
		return err
	}

	structMap := make(map[util.StructName]int)
	for _, disk := range node.Spec.Disks {
		var deviceType string
		switch disk.Type {
		case longhorn.DiskTypeFilesystem:
			deviceType, err = types.GetDeviceTypeOf(disk.Path)
		case longhorn.DiskTypeBlock:
			deviceType, err = types.GetBlockDeviceType(disk.Path)
		default:
			err = fmt.Errorf("unknown disk type %v", disk.Type)
		}
		if err != nil {
			info.logger.WithError(err).Warnf("Failed to get %v device type of %v", disk.Type, disk.Path)
			deviceType = types.ValueUnknown
		}

		structMap[util.StructName(fmt.Sprintf(ClusterInfoNodeDiskCountFmt, strings.ToUpper(deviceType)))]++
		structMap[util.StructName(fmt.Sprintf(ClusterInfoDiskCountFmt, util.ConvertToCamel(string(disk.Type), "-")))]++

	}
	for structName, value := range structMap {
		info.structFields.fields.Append(structName, value)
	}

	return nil
}
