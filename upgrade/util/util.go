package util

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/mod/semver"

	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/meta"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	// LonghornV1ToV2MinorVersionNum v1 minimal minor version when the upgrade path is from v1.x to v2.0
	// TODO: decide the v1 minimum version that could be upgraded to v2.0
	LonghornV1ToV2MinorVersionNum = 30
)

type ProgressMonitor struct {
	description                 string
	targetValue                 int
	currentValue                int
	currentProgressInPercentage float64
	mutex                       *sync.RWMutex
}

func NewProgressMonitor(description string, currentValue, targetValue int) *ProgressMonitor {
	pm := &ProgressMonitor{
		description:                 description,
		targetValue:                 targetValue,
		currentValue:                currentValue,
		currentProgressInPercentage: math.Floor(float64(currentValue*100) / float64(targetValue)),
		mutex:                       &sync.RWMutex{},
	}
	pm.logCurrentProgress()
	return pm
}

func (pm *ProgressMonitor) logCurrentProgress() {
	logrus.Infof("%v: current progress %v%% (%v/%v)", pm.description, pm.currentProgressInPercentage, pm.currentValue, pm.targetValue)
}

func (pm *ProgressMonitor) Inc() int {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	oldProgressInPercentage := pm.currentProgressInPercentage

	pm.currentValue++
	pm.currentProgressInPercentage = math.Floor(float64(pm.currentValue*100) / float64(pm.targetValue))
	if pm.currentProgressInPercentage != oldProgressInPercentage {
		pm.logCurrentProgress()
	}
	return pm.currentValue
}

func (pm *ProgressMonitor) SetCurrentValue(newValue int) {
	pm.mutex.Lock()
	defer pm.mutex.Unlock()

	oldProgressInPercentage := pm.currentProgressInPercentage

	pm.currentValue = newValue
	pm.currentProgressInPercentage = math.Floor(float64(pm.currentValue*100) / float64(pm.targetValue))
	if pm.currentProgressInPercentage != oldProgressInPercentage {
		pm.logCurrentProgress()
	}
}

func (pm *ProgressMonitor) GetCurrentProgress() (int, int, float64) {
	pm.mutex.RLock()
	defer pm.mutex.RUnlock()
	return pm.currentValue, pm.targetValue, pm.currentProgressInPercentage
}

func ListShareManagerPods(namespace string, kubeClient *clientset.Clientset) ([]v1.Pod, error) {
	smPodsList, err := kubeClient.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: labels.Set(types.GetShareManagerComponentLabel()).String(),
	})
	if err != nil {
		return nil, err
	}
	return smPodsList.Items, nil
}

func ListIMPods(namespace string, kubeClient *clientset.Clientset) ([]v1.Pod, error) {
	imPodsList, err := kubeClient.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("%s=%s", types.GetLonghornLabelComponentKey(), types.LonghornLabelInstanceManager),
	})
	if err != nil {
		return nil, err
	}
	return imPodsList.Items, nil
}

func MergeStringMaps(baseMap, overwriteMap map[string]string) map[string]string {
	result := map[string]string{}
	for k, v := range baseMap {
		result[k] = v
	}
	for k, v := range overwriteMap {
		result[k] = v
	}
	return result
}

func GetCurrentLonghornVersion(namespace string, lhClient lhclientset.Interface) (string, error) {
	currentLHVersionSetting, err := lhClient.LonghornV1beta2().Settings(namespace).Get(context.TODO(), string(types.SettingNameCurrentLonghornVersion), metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", err
	}

	return currentLHVersionSetting.Value, nil
}

func CreateOrUpdateLonghornVersionSetting(namespace string, lhClient *lhclientset.Clientset) error {
	s, err := lhClient.LonghornV1beta2().Settings(namespace).Get(context.TODO(), string(types.SettingNameCurrentLonghornVersion), metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return err
		}

		s = &longhorn.Setting{
			ObjectMeta: metav1.ObjectMeta{
				Name: string(types.SettingNameCurrentLonghornVersion),
			},
			Value: meta.Version,
		}
		_, err := lhClient.LonghornV1beta2().Settings(namespace).Create(context.TODO(), s, metav1.CreateOptions{})
		return err
	}

	if s.Value != meta.Version {
		s.Value = meta.Version
		_, err = lhClient.LonghornV1beta2().Settings(namespace).Update(context.TODO(), s, metav1.UpdateOptions{})
		return err
	}
	return nil
}

// CheckUpgradePathSupported returns if the upgrade path from lhCurrentVersion to meta.Version is supported.
//
//	For example: upgrade path is from x.y.z to a.b.c,
//	0 <= a-x <= 1 is supported, and y should be after a specific version if a-x == 1
//	0 <= b-y <= 1 is supported when a-x == 0
//	all downgrade is not supported
func CheckUpgradePathSupported(namespace string, lhClient lhclientset.Interface) error {
	lhCurrentVersion, err := GetCurrentLonghornVersion(namespace, lhClient)
	if err != nil {
		return err
	}

	if lhCurrentVersion == "" {
		return nil
	}

	logrus.Infof("Checking if the upgrade path from %v to %v is supported", lhCurrentVersion, meta.Version)

	if !semver.IsValid(meta.Version) {
		return fmt.Errorf("failed to upgrade since upgrading version %v is not valid", meta.Version)
	}

	lhNewMajorVersion := semver.Major(meta.Version)
	lhCurrentMajorVersion := semver.Major(lhCurrentVersion)

	lhNewMajorVersionNum, lhNewMinorVersionNum, err := getMajorMinorInt(meta.Version)
	if err != nil {
		return errors.Wrapf(err, "failed to parse upgrading %v major/minor version", meta.Version)
	}

	lhCurrentMajorVersionNum, lhCurrentMinorVersionNum, err := getMajorMinorInt(lhCurrentVersion)
	if err != nil {
		return errors.Wrapf(err, "failed to parse current %v major/minor version", meta.Version)
	}

	if semver.Compare(lhCurrentMajorVersion, lhNewMajorVersion) > 0 {
		return fmt.Errorf("failed to upgrade since downgrading from %v to %v for major version is not supported", lhCurrentVersion, meta.Version)
	}

	if semver.Compare(lhCurrentMajorVersion, lhNewMajorVersion) < 0 {
		if (lhNewMajorVersionNum - lhCurrentMajorVersionNum) > 1 {
			return fmt.Errorf("failed to upgrade since upgrading from %v to %v for major version is not supported", lhCurrentVersion, meta.Version)
		}
		if lhCurrentMinorVersionNum < LonghornV1ToV2MinorVersionNum {
			return fmt.Errorf("failed to upgrade since upgrading major version with minor version under %v is not supported", LonghornV1ToV2MinorVersionNum)
		}
		return nil
	}

	if (lhNewMinorVersionNum - lhCurrentMinorVersionNum) > 1 {
		return fmt.Errorf("failed to upgrade since upgrading from %v to %v for minor version is not supported", lhCurrentVersion, meta.Version)
	}

	if (lhNewMinorVersionNum - lhCurrentMinorVersionNum) == 1 {
		return nil
	}

	if semver.Compare(lhCurrentVersion, meta.Version) > 0 {
		return fmt.Errorf("failed to upgrade since downgrading from %v to %v is not supported", lhCurrentVersion, meta.Version)
	}

	return nil
}

func getMajorMinorInt(v string) (int, int, error) {
	majorNum, err := getMajorInt(v)
	if err != nil {
		return -1, -1, err
	}
	minorNum, err := getMinorInt(v)
	if err != nil {
		return -1, -1, err
	}
	return majorNum, minorNum, nil
}

func getMajorInt(v string) (int, error) {
	versionStr := removeFirstChar(v)
	return strconv.Atoi(strings.Split(versionStr, ".")[0])
}

func getMinorInt(v string) (int, error) {
	versionStr := removeFirstChar(v)
	return strconv.Atoi(strings.Split(versionStr, ".")[1])
}

func removeFirstChar(v string) string {
	if v[0] == 'v' {
		return v[1:]
	}
	return v
}

// ListAndUpdateSettingsInProvidedCache list all settings and save them into the provided cached `resourceMap`. This method is not thread-safe.
func ListAndUpdateSettingsInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*v1beta2.Setting, error) {
	if v, ok := resourceMaps[types.LonghornKindSetting]; ok {
		return v.(map[string]*v1beta2.Setting), nil
	}

	settings := map[string]*v1beta2.Setting{}
	settingList, err := lhClient.LonghornV1beta2().Settings(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, setting := range settingList.Items {
		settingCopy := v1beta2.Setting{}
		if err := copier.Copy(&settingCopy, setting); err != nil {
			return nil, err
		}
		settings[setting.Name] = &settingCopy
	}

	resourceMaps[types.LonghornKindSetting] = settings

	return settings, nil
}

// ListAndUpdateNodesInProvidedCache list all nodes and save them into the provided cached `resourceMap`. This method is not thread-safe.
func ListAndUpdateNodesInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*v1beta2.Node, error) {
	if v, ok := resourceMaps[types.LonghornKindNode]; ok {
		return v.(map[string]*v1beta2.Node), nil
	}

	nodes := map[string]*v1beta2.Node{}
	nodeList, err := lhClient.LonghornV1beta2().Nodes(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i, node := range nodeList.Items {
		nodes[node.Name] = &nodeList.Items[i]
	}

	resourceMaps[types.LonghornKindNode] = nodes

	return nodes, nil
}

// ListAndUpdateInstanceManagersInProvidedCache list all instanceManagers and save them into the provided cached `resourceMap`. This method is not thread-safe.
func ListAndUpdateInstanceManagersInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*v1beta2.InstanceManager, error) {
	if v, ok := resourceMaps[types.LonghornKindInstanceManager]; ok {
		return v.(map[string]*v1beta2.InstanceManager), nil
	}

	ims := map[string]*v1beta2.InstanceManager{}
	imList, err := lhClient.LonghornV1beta2().InstanceManagers(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i, im := range imList.Items {
		ims[im.Name] = &imList.Items[i]
	}

	resourceMaps[types.LonghornKindInstanceManager] = ims

	return ims, nil
}

// ListAndUpdateVolumesInProvidedCache list all volumes and save them into the provided cached `resourceMap`. This method is not thread-safe.
func ListAndUpdateVolumesInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*v1beta2.Volume, error) {
	if v, ok := resourceMaps[types.LonghornKindVolume]; ok {
		return v.(map[string]*v1beta2.Volume), nil
	}

	volumes := map[string]*v1beta2.Volume{}
	volumeList, err := lhClient.LonghornV1beta2().Volumes(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i, volume := range volumeList.Items {
		volumes[volume.Name] = &volumeList.Items[i]
	}

	resourceMaps[types.LonghornKindVolume] = volumes

	return volumes, nil
}

// ListAndUpdateReplicasInProvidedCache list all replicas and save them into the provided cached `resourceMap`. This method is not thread-safe.
func ListAndUpdateReplicasInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*v1beta2.Replica, error) {
	if v, ok := resourceMaps[types.LonghornKindReplica]; ok {
		return v.(map[string]*v1beta2.Replica), nil
	}

	replicas := map[string]*v1beta2.Replica{}
	replicaList, err := lhClient.LonghornV1beta2().Replicas(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i, replica := range replicaList.Items {
		replicas[replica.Name] = &replicaList.Items[i]
	}

	resourceMaps[types.LonghornKindReplica] = replicas

	return replicas, nil
}

// ListAndUpdateEnginesInProvidedCache list all engines and save them into the provided cached `resourceMap`. This method is not thread-safe.
func ListAndUpdateEnginesInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*v1beta2.Engine, error) {
	if v, ok := resourceMaps[types.LonghornKindEngine]; ok {
		return v.(map[string]*v1beta2.Engine), nil
	}

	engines := map[string]*v1beta2.Engine{}
	engineList, err := lhClient.LonghornV1beta2().Engines(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i, engine := range engineList.Items {
		engines[engine.Name] = &engineList.Items[i]
	}

	resourceMaps[types.LonghornKindEngine] = engines

	return engines, nil
}

// ListAndUpdateBackupsInProvidedCache list all backups and save them into the provided cached `resourceMap`. This method is not thread-safe.
func ListAndUpdateBackupsInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*v1beta2.Backup, error) {
	if v, ok := resourceMaps[types.LonghornKindBackup]; ok {
		return v.(map[string]*v1beta2.Backup), nil
	}

	backups := map[string]*v1beta2.Backup{}
	backupList, err := lhClient.LonghornV1beta2().Backups(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i, backup := range backupList.Items {
		backups[backup.Name] = &backupList.Items[i]
	}

	resourceMaps[types.LonghornKindBackup] = backups

	return backups, nil
}

// ListAndUpdateEngineImagesInProvidedCache list all engineImages and save them into the provided cached `resourceMap`. This method is not thread-safe.
func ListAndUpdateEngineImagesInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*v1beta2.EngineImage, error) {
	if v, ok := resourceMaps[types.LonghornKindEngineImage]; ok {
		return v.(map[string]*v1beta2.EngineImage), nil
	}

	eis := map[string]*v1beta2.EngineImage{}
	eiList, err := lhClient.LonghornV1beta2().EngineImages(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i, ei := range eiList.Items {
		eis[ei.Name] = &eiList.Items[i]
	}

	resourceMaps[types.LonghornKindEngineImage] = eis

	return eis, nil
}

// ListAndUpdateShareManagersInProvidedCache list all shareManagers and save them into the provided cached `resourceMap`. This method is not thread-safe.
func ListAndUpdateShareManagersInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*v1beta2.ShareManager, error) {
	if v, ok := resourceMaps[types.LonghornKindShareManager]; ok {
		return v.(map[string]*v1beta2.ShareManager), nil
	}

	sms := map[string]*v1beta2.ShareManager{}
	smList, err := lhClient.LonghornV1beta2().ShareManagers(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i, sm := range smList.Items {
		sms[sm.Name] = &smList.Items[i]
	}

	resourceMaps[types.LonghornKindShareManager] = sms

	return sms, nil
}

// ListAndUpdateBackingImagesInProvidedCache list all backingImages and save them into the provided cached `resourceMap`. This method is not thread-safe.
func ListAndUpdateBackingImagesInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*v1beta2.BackingImage, error) {
	if v, ok := resourceMaps[types.LonghornKindBackingImage]; ok {
		return v.(map[string]*v1beta2.BackingImage), nil
	}

	bis := map[string]*v1beta2.BackingImage{}
	biList, err := lhClient.LonghornV1beta2().BackingImages(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i, bi := range biList.Items {
		bis[bi.Name] = &biList.Items[i]
	}

	resourceMaps[types.LonghornKindBackingImage] = bis

	return bis, nil
}

// ListAndUpdateBackingImageDataSourcesInProvidedCache list all backingImageDataSources and save them into the provided cached `resourceMap`. This method is not thread-safe.
func ListAndUpdateBackingImageDataSourcesInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*v1beta2.BackingImageDataSource, error) {
	if v, ok := resourceMaps[types.LonghornKindBackingImageDataSource]; ok {
		return v.(map[string]*v1beta2.BackingImageDataSource), nil
	}

	bidss := map[string]*v1beta2.BackingImageDataSource{}
	bidsList, err := lhClient.LonghornV1beta2().BackingImageDataSources(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i, bids := range bidsList.Items {
		bidss[bids.Name] = &bidsList.Items[i]
	}

	resourceMaps[types.LonghornKindBackingImageDataSource] = bidss

	return bidss, nil
}

// ListAndUpdateRecurringJobsInProvidedCache list all recurringJobs and save them into the provided cached `resourceMap`. This method is not thread-safe.
func ListAndUpdateRecurringJobsInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) (map[string]*v1beta2.RecurringJob, error) {
	if v, ok := resourceMaps[types.LonghornKindRecurringJob]; ok {
		return v.(map[string]*v1beta2.RecurringJob), nil
	}

	recurringJobs := map[string]*v1beta2.RecurringJob{}
	recurringJobList, err := lhClient.LonghornV1beta2().RecurringJobs(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for i, recurringJob := range recurringJobList.Items {
		recurringJobs[recurringJob.Name] = &recurringJobList.Items[i]
	}

	resourceMaps[types.LonghornKindRecurringJob] = recurringJobs

	return recurringJobs, nil
}

// GetNodeFromProvidedCache gets the node from the provided cached `resourceMap`. This method is not thread-safe.
func GetNodeFromProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}, name string) (*v1beta2.Node, error) {
	nodes, err := ListAndUpdateNodesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return nil, err
	}
	node, ok := nodes[name]
	if !ok {
		return nil, apierrors.NewNotFound(v1beta2.Resource("node"), name)
	}
	return node, nil
}

// GetSettingFromProvidedCache gets the setting from the provided cached `resourceMap`. This method is not thread-safe.
func GetSettingFromProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}, name string) (*v1beta2.Setting, error) {
	settings, err := ListAndUpdateSettingsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return nil, err
	}
	setting, ok := settings[name]
	if !ok {
		return nil, apierrors.NewNotFound(v1beta2.Resource("setting"), name)
	}
	return setting, nil
}

// GetEngineImageFromProvidedCache gets the engineImage from the provided cached `resourceMap`. This method is not thread-safe.
func GetEngineImageFromProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}, name string) (*v1beta2.EngineImage, error) {
	eis, err := ListAndUpdateEngineImagesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return nil, err
	}
	ei, ok := eis[name]
	if !ok {
		return nil, apierrors.NewNotFound(v1beta2.Resource("engineimage"), name)
	}
	return ei, nil
}

// GetInstanceManagerFromProvidedCache gets the instanceManager from the provided cached `resourceMap`. This method is not thread-safe.
func GetInstanceManagerFromProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}, name string) (*v1beta2.InstanceManager, error) {
	ims, err := ListAndUpdateInstanceManagersInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return nil, err
	}
	im, ok := ims[name]
	if !ok {
		return nil, apierrors.NewNotFound(v1beta2.Resource("instancemanager"), name)
	}
	return im, nil
}

// GetShareManagerFromProvidedCache gets the shareManager from the provided cached `resourceMap`. This method is not thread-safe.
func GetShareManagerFromProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}, name string) (*v1beta2.ShareManager, error) {
	sms, err := ListAndUpdateShareManagersInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return nil, err
	}
	sm, ok := sms[name]
	if !ok {
		return nil, apierrors.NewNotFound(v1beta2.Resource("sharemanager"), name)
	}
	return sm, nil
}

// GetVolumeFromProvidedCache gets the volume from the provided cached `resourceMap`. This method is not thread-safe.
func GetVolumeFromProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}, name string) (*v1beta2.Volume, error) {
	volumes, err := ListAndUpdateVolumesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return nil, err
	}
	volume, ok := volumes[name]
	if !ok {
		return nil, apierrors.NewNotFound(v1beta2.Resource("volume"), name)
	}
	return volume, nil
}

// GetBackingImageFromProvidedCache gets the backingImage from the provided cached `resourceMap`. This method is not thread-safe.
func GetBackingImageFromProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}, name string) (*v1beta2.BackingImage, error) {
	bis, err := ListAndUpdateBackingImagesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return nil, err
	}
	bi, ok := bis[name]
	if !ok {
		return nil, apierrors.NewNotFound(v1beta2.Resource("backingimage"), name)
	}
	return bi, nil
}

// GetBackingImageDataSourceFromProvidedCache gets the backingImageDataSource from the provided cached `resourceMap`. This method is not thread-safe.
func GetBackingImageDataSourceFromProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}, name string) (*v1beta2.BackingImageDataSource, error) {
	bidss, err := ListAndUpdateBackingImageDataSourcesInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return nil, err
	}
	bids, ok := bidss[name]
	if !ok {
		return nil, apierrors.NewNotFound(v1beta2.Resource("backingimagedatasource"), name)
	}
	return bids, nil
}

// GetRecurringJobFromProvidedCache gets the recurringJob from the provided cached `resourceMap`. This method is not thread-safe.
func GetRecurringJobFromProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}, name string) (*v1beta2.RecurringJob, error) {
	recurringJobs, err := ListAndUpdateRecurringJobsInProvidedCache(namespace, lhClient, resourceMaps)
	if err != nil {
		return nil, err
	}
	recurringJob, ok := recurringJobs[name]
	if !ok {
		return nil, apierrors.NewNotFound(v1beta2.Resource("recurringjob"), name)
	}
	return recurringJob, nil
}

// CreateAndUpdateRecurringJobInProvidedCache creates a recurringJob and saves it into the provided cached `resourceMap`. This method is not thread-safe.
func CreateAndUpdateRecurringJobInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}, job *longhorn.RecurringJob) (*v1beta2.RecurringJob, error) {
	obj, err := lhClient.LonghornV1beta2().RecurringJobs(namespace).Create(context.TODO(), job, metav1.CreateOptions{})
	if err != nil {
		return obj, err
	}

	var recurringJobs map[string]*v1beta2.RecurringJob
	if v, ok := resourceMaps[types.LonghornKindRecurringJob]; ok {
		recurringJobs = v.(map[string]*v1beta2.RecurringJob)
	} else {
		recurringJobs = map[string]*v1beta2.RecurringJob{}
	}
	recurringJobs[job.Name] = obj

	resourceMaps[types.LonghornKindRecurringJob] = recurringJobs

	return obj, nil
}

// CreateAndUpdateBackingImageInProvidedCache creates a backingImage and saves it into the provided cached `resourceMap`. This method is not thread-safe.
func CreateAndUpdateBackingImageInProvidedCache(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}, bid *longhorn.BackingImageDataSource) (*v1beta2.BackingImageDataSource, error) {
	obj, err := lhClient.LonghornV1beta2().BackingImageDataSources(namespace).Create(context.TODO(), bid, metav1.CreateOptions{})
	if err != nil {
		return obj, err
	}

	var bids map[string]*v1beta2.BackingImageDataSource
	if v, ok := resourceMaps[types.LonghornKindBackingImageDataSource]; ok {
		bids = v.(map[string]*v1beta2.BackingImageDataSource)
	} else {
		bids = map[string]*v1beta2.BackingImageDataSource{}
	}
	bids[bid.Name] = obj

	resourceMaps[types.LonghornKindBackingImageDataSource] = bids

	return obj, nil
}

// UpdateResources persists all the resources in provided cached `resourceMap`. This method is not thread-safe.
func UpdateResources(namespace string, lhClient *lhclientset.Clientset, resourceMaps map[string]interface{}) error {
	var err error

	for resourceKind, resourceMap := range resourceMaps {
		switch resourceKind {
		case types.LonghornKindNode:
			err = updateNodes(namespace, lhClient, resourceMap.(map[string]*longhorn.Node))
		case types.LonghornKindVolume:
			err = updateVolumes(namespace, lhClient, resourceMap.(map[string]*longhorn.Volume))
		case types.LonghornKindEngine:
			err = updateEngines(namespace, lhClient, resourceMap.(map[string]*longhorn.Engine))
		case types.LonghornKindReplica:
			err = updateReplicas(namespace, lhClient, resourceMap.(map[string]*longhorn.Replica))
		case types.LonghornKindBackup:
			err = updateBackups(namespace, lhClient, resourceMap.(map[string]*longhorn.Backup))
		case types.LonghornKindEngineImage:
			err = updateEngineImages(namespace, lhClient, resourceMap.(map[string]*longhorn.EngineImage))
		case types.LonghornKindInstanceManager:
			err = updateInstanceManagers(namespace, lhClient, resourceMap.(map[string]*longhorn.InstanceManager))
		case types.LonghornKindShareManager:
			err = updateShareManagers(namespace, lhClient, resourceMap.(map[string]*longhorn.ShareManager))
		case types.LonghornKindBackingImage:
			err = updateBackingImages(namespace, lhClient, resourceMap.(map[string]*longhorn.BackingImage))
		case types.LonghornKindRecurringJob:
			err = updateRecurringJobs(namespace, lhClient, resourceMap.(map[string]*longhorn.RecurringJob))
		case types.LonghornKindSetting:
			err = updateSettings(namespace, lhClient, resourceMap.(map[string]*longhorn.Setting))
		default:
			return fmt.Errorf("resource kind %v is not able to updated", resourceKind)
		}

		if err != nil {
			return err
		}
	}

	return nil
}

func updateNodes(namespace string, lhClient *lhclientset.Clientset, nodes map[string]*longhorn.Node) error {
	existingNodeList, err := lhClient.LonghornV1beta2().Nodes(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, existingNode := range existingNodeList.Items {
		node, ok := nodes[existingNode.Name]
		if !ok {
			continue
		}

		if !reflect.DeepEqual(existingNode.Status, node.Status) {
			if _, err = lhClient.LonghornV1beta2().Nodes(namespace).UpdateStatus(context.TODO(), node, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}

		if !reflect.DeepEqual(existingNode.Spec, node.Spec) ||
			!reflect.DeepEqual(existingNode.ObjectMeta, node.ObjectMeta) {
			if _, err = lhClient.LonghornV1beta2().Nodes(namespace).Update(context.TODO(), node, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}
	}

	return nil
}

func updateVolumes(namespace string, lhClient *lhclientset.Clientset, volumes map[string]*longhorn.Volume) error {
	existingVolumeList, err := lhClient.LonghornV1beta2().Volumes(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, existingVolume := range existingVolumeList.Items {
		volume, ok := volumes[existingVolume.Name]
		if !ok {
			continue
		}

		if !reflect.DeepEqual(existingVolume.Status, volume.Status) {
			if _, err = lhClient.LonghornV1beta2().Volumes(namespace).UpdateStatus(context.TODO(), volume, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}

		if !reflect.DeepEqual(existingVolume.Spec, volume.Spec) ||
			!reflect.DeepEqual(existingVolume.ObjectMeta, volume.ObjectMeta) {
			if _, err = lhClient.LonghornV1beta2().Volumes(namespace).Update(context.TODO(), volume, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}
	}

	return nil
}

func updateReplicas(namespace string, lhClient *lhclientset.Clientset, replicas map[string]*longhorn.Replica) error {
	existingReplicaList, err := lhClient.LonghornV1beta2().Replicas(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, existingReplica := range existingReplicaList.Items {
		replica, ok := replicas[existingReplica.Name]
		if !ok {
			continue
		}

		if !reflect.DeepEqual(existingReplica.Status, replica.Status) {
			if _, err = lhClient.LonghornV1beta2().Replicas(namespace).UpdateStatus(context.TODO(), replica, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}

		if !reflect.DeepEqual(existingReplica.Spec, replica.Spec) ||
			!reflect.DeepEqual(existingReplica.ObjectMeta, replica.ObjectMeta) {
			if _, err = lhClient.LonghornV1beta2().Replicas(namespace).Update(context.TODO(), replica, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}
	}

	return nil
}

func updateEngines(namespace string, lhClient *lhclientset.Clientset, engines map[string]*longhorn.Engine) error {
	existingEngineList, err := lhClient.LonghornV1beta2().Engines(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, existingEngine := range existingEngineList.Items {
		engine, ok := engines[existingEngine.Name]
		if !ok {
			continue
		}

		if !reflect.DeepEqual(existingEngine.Status, engine.Status) {
			if _, err = lhClient.LonghornV1beta2().Engines(namespace).UpdateStatus(context.TODO(), engine, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}

		if !reflect.DeepEqual(existingEngine.Spec, engine.Spec) ||
			!reflect.DeepEqual(existingEngine.ObjectMeta, engine.ObjectMeta) {
			if _, err = lhClient.LonghornV1beta2().Engines(namespace).Update(context.TODO(), engine, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}
	}

	return nil
}

func updateBackups(namespace string, lhClient *lhclientset.Clientset, backups map[string]*longhorn.Backup) error {
	existingBackupList, err := lhClient.LonghornV1beta2().Backups(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, existingBackup := range existingBackupList.Items {
		backup, ok := backups[existingBackup.Name]
		if !ok {
			continue
		}

		if !reflect.DeepEqual(existingBackup.Status, backup.Status) {
			if _, err = lhClient.LonghornV1beta2().Backups(namespace).UpdateStatus(context.TODO(), backup, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}

		if !reflect.DeepEqual(existingBackup.Spec, backup.Spec) ||
			!reflect.DeepEqual(existingBackup.ObjectMeta, backup.ObjectMeta) {
			if _, err = lhClient.LonghornV1beta2().Backups(namespace).Update(context.TODO(), backup, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}
	}

	return nil
}

func updateEngineImages(namespace string, lhClient *lhclientset.Clientset, engineImages map[string]*longhorn.EngineImage) error {
	existingEngineImageList, err := lhClient.LonghornV1beta2().EngineImages(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, existingEngineImage := range existingEngineImageList.Items {
		engineImage, ok := engineImages[existingEngineImage.Name]
		if !ok {
			continue
		}

		if !reflect.DeepEqual(existingEngineImage.Status, engineImage.Status) {
			if _, err = lhClient.LonghornV1beta2().EngineImages(namespace).UpdateStatus(context.TODO(), engineImage, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}

		if !reflect.DeepEqual(existingEngineImage.Spec, engineImage.Spec) ||
			!reflect.DeepEqual(existingEngineImage.ObjectMeta, engineImage.ObjectMeta) {
			if _, err = lhClient.LonghornV1beta2().EngineImages(namespace).Update(context.TODO(), engineImage, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}
	}

	return nil
}

func updateInstanceManagers(namespace string, lhClient *lhclientset.Clientset, instanceManagers map[string]*longhorn.InstanceManager) error {
	existingInstanceManagerList, err := lhClient.LonghornV1beta2().InstanceManagers(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, existingInstanceManager := range existingInstanceManagerList.Items {
		instanceManager, ok := instanceManagers[existingInstanceManager.Name]
		if !ok {
			continue
		}

		if !reflect.DeepEqual(existingInstanceManager.Status, instanceManager.Status) {
			if _, err = lhClient.LonghornV1beta2().InstanceManagers(namespace).UpdateStatus(context.TODO(), instanceManager, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}

		if !reflect.DeepEqual(existingInstanceManager.Spec, instanceManager.Spec) ||
			!reflect.DeepEqual(existingInstanceManager.ObjectMeta, instanceManager.ObjectMeta) {
			if _, err = lhClient.LonghornV1beta2().InstanceManagers(namespace).Update(context.TODO(), instanceManager, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}
	}

	return nil
}

func updateShareManagers(namespace string, lhClient *lhclientset.Clientset, shareManagers map[string]*longhorn.ShareManager) error {
	existingShareManagerList, err := lhClient.LonghornV1beta2().ShareManagers(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, existingShareManager := range existingShareManagerList.Items {
		shareManager, ok := shareManagers[existingShareManager.Name]
		if !ok {
			continue
		}

		if !reflect.DeepEqual(existingShareManager.Status, shareManager.Status) {
			if _, err = lhClient.LonghornV1beta2().ShareManagers(namespace).UpdateStatus(context.TODO(), shareManager, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}

		if !reflect.DeepEqual(existingShareManager.Spec, shareManager.Spec) ||
			!reflect.DeepEqual(existingShareManager.ObjectMeta, shareManager.ObjectMeta) {
			if _, err = lhClient.LonghornV1beta2().ShareManagers(namespace).Update(context.TODO(), shareManager, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}
	}

	return nil
}

func updateBackingImages(namespace string, lhClient *lhclientset.Clientset, backingImages map[string]*longhorn.BackingImage) error {
	existingBackingImagesList, err := lhClient.LonghornV1beta2().BackingImages(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, existingBackingImage := range existingBackingImagesList.Items {
		backingImage, ok := backingImages[existingBackingImage.Name]
		if !ok {
			continue
		}

		if !reflect.DeepEqual(existingBackingImage.Status, backingImage.Status) {
			if _, err = lhClient.LonghornV1beta2().BackingImages(namespace).UpdateStatus(context.TODO(), backingImage, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}

		if !reflect.DeepEqual(existingBackingImage.Spec, backingImage.Spec) ||
			!reflect.DeepEqual(existingBackingImage.ObjectMeta, backingImage.ObjectMeta) {
			if _, err = lhClient.LonghornV1beta2().BackingImages(namespace).Update(context.TODO(), backingImage, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}
	}

	return nil
}

func updateRecurringJobs(namespace string, lhClient *lhclientset.Clientset, recurringJobs map[string]*longhorn.RecurringJob) error {
	existingRecurringJobList, err := lhClient.LonghornV1beta2().RecurringJobs(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, existingRecurringJob := range existingRecurringJobList.Items {
		recurringJob, ok := recurringJobs[existingRecurringJob.Name]
		if !ok {
			continue
		}

		if !reflect.DeepEqual(existingRecurringJob.Status, recurringJob.Status) {
			if _, err = lhClient.LonghornV1beta2().RecurringJobs(namespace).UpdateStatus(context.TODO(), recurringJob, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}

		if !reflect.DeepEqual(existingRecurringJob.Spec, recurringJob.Spec) ||
			!reflect.DeepEqual(existingRecurringJob.ObjectMeta, recurringJob.ObjectMeta) {
			if _, err = lhClient.LonghornV1beta2().RecurringJobs(namespace).Update(context.TODO(), recurringJob, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}
	}

	return nil
}

func updateSettings(namespace string, lhClient *lhclientset.Clientset, settings map[string]*longhorn.Setting) error {
	existingSettingList, err := lhClient.LonghornV1beta2().Settings(namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return err
	}

	for _, existingSetting := range existingSettingList.Items {
		setting, ok := settings[existingSetting.Name]
		if !ok {
			continue
		}

		if !reflect.DeepEqual(existingSetting.Value, setting.Value) {
			if _, err = lhClient.LonghornV1beta2().Settings(namespace).Update(context.TODO(), setting, metav1.UpdateOptions{}); err != nil && !apierrors.IsConflict(errors.Cause(err)) {
				return err
			}
		}
	}

	return nil
}
