package controller

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/manager"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1beta1"
)

const (
	VersionTagLatest = "latest"
)

var (
	upgradeCheckInterval          = time.Hour
	settingControllerResyncPeriod = time.Hour
	checkUpgradeURL               = "https://longhorn-upgrade-responder.rancher.io/v1/checkupgrade"
)

type SettingController struct {
	*baseController

	namespace string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	sStoreSynced   cache.InformerSynced
	cfmStoreSynced cache.InformerSynced

	// upgrade checker
	lastUpgradeCheckedTimestamp time.Time
	version                     string

	// backup store monitor
	bsMonitor *BackupStoreMonitor
}

type BackupStoreMonitor struct {
	backupTarget                 string
	backupTargetCredentialSecret string

	pollInterval time.Duration

	target *engineapi.BackupTarget
	ds     *datastore.DataStore
	stopCh chan struct{}
}

type Version struct {
	Name        string // must be in semantic versioning
	ReleaseDate string
	Tags        []string
}

type CheckUpgradeRequest struct {
	LonghornVersion   string `json:"longhornVersion"`
	KubernetesVersion string `json:"kubernetesVersion"`
}

type CheckUpgradeResponse struct {
	Versions []Version `json:"versions"`
}

func NewSettingController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	settingInformer lhinformers.SettingInformer,
	configMapInformer coreinformers.ConfigMapInformer,
	kubeClient clientset.Interface,
	version string,
	namespace string) *SettingController {

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events("")})

	sc := &SettingController{
		baseController: newBaseController("longhorn-setting", logger),

		namespace: namespace,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-setting-controller"}),

		ds: ds,

		sStoreSynced:   settingInformer.Informer().HasSynced,
		cfmStoreSynced: configMapInformer.Informer().HasSynced,

		version: version,
	}

	settingInformer.Informer().AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.enqueueSetting,
		UpdateFunc: func(old, cur interface{}) { sc.enqueueSetting(cur) },
		DeleteFunc: sc.enqueueSetting,
	}, settingControllerResyncPeriod)

	configMapInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    sc.enqueueConfigMapChange,
		UpdateFunc: func(old, cur interface{}) { sc.enqueueConfigMapChange(cur) },
		DeleteFunc: sc.enqueueConfigMapChange,
	})

	return sc
}

func (sc *SettingController) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer sc.queue.ShutDown()

	sc.logger.Info("Start Longhorn Setting controller")
	defer sc.logger.Info("Shutting down Longhorn Setting controller")

	if !controller.WaitForCacheSync("longhorn settings", stopCh, sc.sStoreSynced, sc.cfmStoreSynced) {
		return
	}

	// must remain single threaded since backup store monitor is not thread-safe now
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

	if sc.queue.NumRequeues(key) < maxRetries {
		sc.logger.WithError(err).Warnf("Error syncing Longhorn setting %v", key)
		sc.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	sc.logger.WithError(err).Warnf("Dropping Longhorn setting %v out of the queue", key)
	sc.queue.Forget(key)
}

func (sc *SettingController) syncSetting(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "fail to sync setting for %v", key)
	}()

	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	switch name {
	case string(types.SettingNameUpgradeChecker):
		if err := sc.syncUpgradeChecker(); err != nil {
			return err
		}
	case string(types.SettingNameBackupTargetCredentialSecret):
		fallthrough
	case string(types.SettingNameBackupTarget):
		if err := sc.syncBackupTarget(); err != nil {
			return err
		}
	case string(types.SettingNameBackupstorePollInterval):
		if err := sc.updateBackupstorePollInterval(); err != nil {
			return err
		}
	case string(types.SettingNameTaintToleration):
		if err := sc.updateTaintToleration(); err != nil {
			return err
		}
	case string(types.SettingNameGuaranteedEngineCPU):
		if err := sc.updateGuaranteedEngineCPU(); err != nil {
			return err
		}
	case string(types.SettingNamePriorityClass):
		if err := sc.updatePriorityClass(); err != nil {
			return err
		}
	case string(types.SettingNameDefaultStorageClassConfigMap):
		if err := sc.updateDefaultStorageClass(); err != nil {
			return err
		}
	default:
	}

	return nil
}

func (sc *SettingController) syncBackupTarget() (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync backup target")
	}()

	targetSetting, err := sc.ds.GetSetting(types.SettingNameBackupTarget)
	if err != nil {
		return err
	}

	secretSetting, err := sc.ds.GetSetting(types.SettingNameBackupTargetCredentialSecret)
	if err != nil {
		return err
	}

	interval, err := sc.ds.GetSettingAsInt(types.SettingNameBackupstorePollInterval)
	if err != nil {
		return err
	}

	if sc.bsMonitor != nil {
		if sc.bsMonitor.backupTarget == targetSetting.Value &&
			sc.bsMonitor.backupTargetCredentialSecret == secretSetting.Value {
			// already monitoring
			return nil
		}
		sc.logger.Infof("Restarting backup store monitor because backup target changed from %v to %v", sc.bsMonitor.backupTarget, targetSetting.Value)
		sc.bsMonitor.Stop()
		sc.bsMonitor = nil
		manager.SyncVolumesLastBackupWithBackupVolumes(nil,
			sc.ds.ListVolumes, sc.ds.GetVolume, sc.ds.UpdateVolumeStatus)
	}

	if targetSetting.Value == "" {
		return nil
	}

	target, err := manager.GenerateBackupTarget(sc.ds)
	if err != nil {
		return err
	}
	sc.bsMonitor = &BackupStoreMonitor{
		backupTarget:                 targetSetting.Value,
		backupTargetCredentialSecret: secretSetting.Value,

		pollInterval: time.Duration(interval) * time.Second,

		target: target,
		ds:     sc.ds,
		stopCh: make(chan struct{}),
	}
	go sc.bsMonitor.Start()
	return nil
}

func (sc *SettingController) updateBackupstorePollInterval() (err error) {
	if sc.bsMonitor == nil {
		return nil
	}

	defer func() {
		err = errors.Wrapf(err, "failed to sync backup target")
	}()

	interval, err := sc.ds.GetSettingAsInt(types.SettingNameBackupstorePollInterval)
	if err != nil {
		return err
	}

	if sc.bsMonitor.pollInterval == time.Duration(interval)*time.Second {
		return nil
	}

	sc.bsMonitor.Stop()

	sc.bsMonitor.pollInterval = time.Duration(interval) * time.Second
	sc.bsMonitor.stopCh = make(chan struct{})

	go sc.bsMonitor.Start()
	return nil
}

func (sc *SettingController) updateTaintToleration() error {
	setting, err := sc.ds.GetSetting(types.SettingNameTaintToleration)
	if err != nil {
		return err
	}
	tolerationList, err := types.UnmarshalTolerations(setting.Value)
	if err != nil {
		return err
	}
	newTolerations := util.TolerationListToMap(tolerationList)

	daemonsetList, err := sc.ds.ListDaemonSet()
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn daemonsets for toleration update")
	}

	deploymentList, err := sc.ds.ListDeployment()
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn deployments for toleration update")
	}

	imPodList, err := sc.ds.ListInstanceManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list instance manager pods for toleration update")
	}

	for _, dp := range deploymentList {
		if util.AreIdenticalTolerations(util.TolerationListToMap(dp.Spec.Template.Spec.Tolerations), newTolerations) {
			continue
		}
		dp.Spec.Template.Spec.Tolerations = getFinalTolerations(util.TolerationListToMap(dp.Spec.Template.Spec.Tolerations), newTolerations)
		if _, err := sc.ds.UpdateDeployment(dp); err != nil {
			return err
		}
	}
	for _, ds := range daemonsetList {
		if util.AreIdenticalTolerations(util.TolerationListToMap(ds.Spec.Template.Spec.Tolerations), newTolerations) {
			continue
		}
		ds.Spec.Template.Spec.Tolerations = getFinalTolerations(util.TolerationListToMap(ds.Spec.Template.Spec.Tolerations), newTolerations)
		if _, err := sc.ds.UpdateDaemonSet(ds); err != nil {
			return err
		}
	}
	for _, imPod := range imPodList {
		if util.AreIdenticalTolerations(util.TolerationListToMap(imPod.Spec.Tolerations), newTolerations) {
			continue
		}
		if err := sc.ds.DeletePod(imPod.Name); err != nil {
			return err
		}
	}

	return nil
}

func (sc *SettingController) updatePriorityClass() error {
	setting, err := sc.ds.GetSetting(types.SettingNamePriorityClass)
	if err != nil {
		return err
	}
	newPriorityClass := setting.Value

	daemonsetList, err := sc.ds.ListDaemonSet()
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn daemonsets for priority class update")
	}

	deploymentList, err := sc.ds.ListDeployment()
	if err != nil {
		return errors.Wrapf(err, "failed to list Longhorn deployments for priority class update")
	}

	imPodList, err := sc.ds.ListInstanceManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list instance manager pods for priority class update")
	}

	for _, dp := range deploymentList {
		if dp.Spec.Template.Spec.PriorityClassName == newPriorityClass {
			continue
		}
		dp.Spec.Template.Spec.PriorityClassName = newPriorityClass
		if _, err := sc.ds.UpdateDeployment(dp); err != nil {
			return err
		}
	}
	for _, ds := range daemonsetList {
		if ds.Spec.Template.Spec.PriorityClassName == newPriorityClass {
			continue
		}
		ds.Spec.Template.Spec.PriorityClassName = newPriorityClass
		if _, err := sc.ds.UpdateDaemonSet(ds); err != nil {
			return err
		}
	}
	for _, imPod := range imPodList {
		if imPod.Spec.PriorityClassName == newPriorityClass {
			continue
		}
		if err := sc.ds.DeletePod(imPod.Name); err != nil {
			return err
		}
	}

	return nil
}

func (sc *SettingController) updateDefaultStorageClass() error {
	storageConfigMap, err := sc.ds.GetConfigMapRO(types.DefaultStorageClassConfigMapName)
	if err != nil {
		if datastore.ErrorIsNotFound(err) {
			// The storageclass configmap is deleted therefore we delete the actual storageclass
			err := sc.ds.DeleteStorageClass(types.DefaultStorageClassName)
			if err != nil && !datastore.ErrorIsNotFound(err) {
				return nil
			}
			return nil
		}
		return err
	}

	storageclass, err := buildStorageClassManifestFromConfigMap(storageConfigMap)
	if err != nil {
		return err
	}

	existingSC, err := sc.ds.GetStorageClassRO(types.DefaultStorageClassName)
	if err != nil && !datastore.ErrorIsNotFound(err) {
		return err
	}

	if storageclassesHaveSameValues(existingSC, storageclass) {
		return nil
	}

	err = sc.ds.DeleteStorageClass(types.DefaultStorageClassName)
	if err != nil && !datastore.ErrorIsNotFound(err) {
		return err
	}

	storageclass, err = sc.ds.CreateStorageClass(storageclass)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			return nil
		}
		return err
	}

	sc.logger.Infof("Updated Longhorn storagecclass: %v", storageclass)

	return nil
}

func buildStorageClassManifestFromConfigMap(cfm *v1.ConfigMap) (*storagev1.StorageClass, error) {
	data, ok := cfm.Data["storageclass.yaml"]
	if !ok {
		return nil, fmt.Errorf("cannot find storageclass.yaml inside the default storageclass configmap")
	}

	decode := scheme.Codecs.UniversalDeserializer().Decode
	obj, _, err := decode([]byte(data), nil, nil)
	if err != nil {
		return nil, errors.Wrapf(err, "error while decoding YAML object")
	}

	storageclass, ok := obj.(*storagev1.StorageClass)
	if !ok {
		return nil, fmt.Errorf("invalid storageclass.yaml: %v", data)
	}
	return storageclass, nil
}

// storageclassesHaveSameValues compare the values of SC1 and SC2,
// ignoring fields in their ObjectMeta and TypeMeta except for Labels and Annotations
func storageclassesHaveSameValues(sc1, sc2 *storagev1.StorageClass) bool {
	if sc1 == nil || sc2 == nil {
		return false
	}

	sc1Copy := sc1.DeepCopy()
	sc2Copy := sc2.DeepCopy()
	sc1Copy.ObjectMeta = metav1.ObjectMeta{}
	sc1Copy.TypeMeta = metav1.TypeMeta{}
	sc2Copy.ObjectMeta = metav1.ObjectMeta{}
	sc2Copy.TypeMeta = metav1.TypeMeta{}

	return reflect.DeepEqual(sc1.Labels, sc2.Labels) &&
		reflect.DeepEqual(sc1.Annotations, sc2.Annotations) &&
		reflect.DeepEqual(sc1Copy, sc2Copy)
}

func getFinalTolerations(oldTolerations, newTolerations map[string]v1.Toleration) []v1.Toleration {
	res := []v1.Toleration{}
	// Combine Kubernetes default tolerations with new Longhorn toleration setting
	for _, t := range oldTolerations {
		if util.IsKubernetesDefaultToleration(t) {
			res = append(res, t)
		}
	}
	for _, t := range newTolerations {
		res = append(res, t)
	}

	return res
}

func (bm *BackupStoreMonitor) Start() {
	if bm.pollInterval == time.Duration(0) {
		logrus.Infof("Backup store polling has been disabled for %v", bm.target.URL)
		return
	}
	logrus.Debugf("Start backup store monitoring for %v", bm.target.URL)
	defer func() {
		logrus.Debugf("Stop backup store monitoring %v", bm.target.URL)
	}()

	wait.Until(func() {
		backupVolumes, err := bm.target.ListVolumes()
		if err != nil {
			logrus.Warnf("backup store monitor: failed to list backup volumes in %v: %v", bm.target.URL, err)
		}
		manager.SyncVolumesLastBackupWithBackupVolumes(backupVolumes,
			bm.ds.ListVolumes, bm.ds.GetVolume, bm.ds.UpdateVolumeStatus)
	}, bm.pollInterval, bm.stopCh)
}

func (bm *BackupStoreMonitor) Stop() {
	if bm.pollInterval != time.Duration(0) {
		bm.stopCh <- struct{}{}
	}
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

	if upgradeCheckerEnabled == false {
		if latestLonghornVersion.Value != "" {
			latestLonghornVersion.Value = ""
			if _, err := sc.ds.UpdateSetting(latestLonghornVersion); err != nil {
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

	oldVersion := latestLonghornVersion.Value
	latestLonghornVersion.Value, err = sc.CheckLatestLonghornVersion()
	if err != nil {
		// non-critical error, don't retry
		sc.logger.WithError(err).Debug("Failed to check for the latest upgrade")
		return nil
	}

	sc.lastUpgradeCheckedTimestamp = now

	if latestLonghornVersion.Value != oldVersion {
		sc.logger.Infof("Latest Longhorn version is %v", latestLonghornVersion.Value)
		if _, err := sc.ds.UpdateSetting(latestLonghornVersion); err != nil {
			// non-critical error, don't retry
			sc.logger.WithError(err).Debug("Cannot update latest Longhorn version")
			return nil
		}
	}
	return nil
}

func (sc *SettingController) CheckLatestLonghornVersion() (string, error) {
	var (
		resp    CheckUpgradeResponse
		content bytes.Buffer
	)
	kubeVersion, err := sc.kubeClient.Discovery().ServerVersion()
	if err != nil {
		return "", errors.Wrap(err, "failed to get Kubernetes server version")
	}

	req := &CheckUpgradeRequest{
		LonghornVersion:   sc.version,
		KubernetesVersion: kubeVersion.GitVersion,
	}
	if err := json.NewEncoder(&content).Encode(req); err != nil {
		return "", err
	}
	r, err := http.Post(checkUpgradeURL, "application/json", &content)
	if err != nil {
		return "", err
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		message := ""
		messageBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			message = err.Error()
		} else {
			message = string(messageBytes)
		}
		return "", fmt.Errorf("query return status code %v, message %v", r.StatusCode, message)
	}
	if err := json.NewDecoder(r.Body).Decode(&resp); err != nil {
		return "", err
	}

	latestVersion := ""
	for _, v := range resp.Versions {
		found := false
		for _, tag := range v.Tags {
			if tag == VersionTagLatest {
				found = true
				break
			}
		}
		if found {
			latestVersion = v.Name
			break
		}
	}
	if latestVersion == "" {
		return "", fmt.Errorf("cannot find latest version in response: %+v", resp)
	}

	return latestVersion, nil
}

func (sc *SettingController) enqueueSetting(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	sc.queue.AddRateLimited(key)
}

func (sc *SettingController) enqueueConfigMapChange(obj interface{}) {
	cfm, ok := obj.(*v1.ConfigMap)
	if !ok {
		deletedState, ok := obj.(*cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		cfm, ok = deletedState.Obj.(*v1.ConfigMap)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	if cfm.GetNamespace() == sc.namespace && cfm.GetName() == types.DefaultStorageClassConfigMapName {
		key := fmt.Sprintf("%s/%s", sc.namespace, types.SettingNameDefaultStorageClassConfigMap)
		sc.queue.AddRateLimited(key)
	}
}

func (sc *SettingController) updateGuaranteedEngineCPU() error {
	resourceReq, err := GetGuaranteedResourceRequirement(sc.ds)
	if err != nil {
		return err
	}

	imPodList, err := sc.ds.ListInstanceManagerPods()
	if err != nil {
		return errors.Wrapf(err, "failed to list instance manager pods for toleration update")
	}

	for _, imPod := range imPodList {
		podResourceReq := imPod.Spec.Containers[0].Resources
		if IsSameGuaranteedCPURequirement(resourceReq, &podResourceReq) {
			continue
		}
		sc.logger.Infof("Delete instance manager pod %v to refresh GuaranteedEngineCPU option", imPod.Name)
		if err := sc.ds.DeletePod(imPod.Name); err != nil {
			return err
		}
	}

	return nil
}
