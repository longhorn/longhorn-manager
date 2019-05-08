package manager

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/util"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

type BundleState string

const (
	BundleStateInProgress  = BundleState("InProgress")
	BundleReadyForDownload = BundleState("ReadyForDownload")
	BundleStateError       = BundleState("Error")
)

type BundleErrorMessage string

const (
	BundleMkdirFailed = BundleErrorMessage("Failed to create bundle file directory")
	BundleZipFailed   = BundleErrorMessage("Failed to compress the support bundle files")
	BundleOpenFailed  = BundleErrorMessage("Failed to open the compressed bundle file")
	BundleStatFailed  = BundleErrorMessage("Failed to compute the size of the compressed bundle file")
)

type SupportBundle struct {
	State      BundleState
	Size       int64
	Error      BundleErrorMessage
	Filename   string
	createTime time.Time
}

func NewSupportBundle(state BundleState, filename string) *SupportBundle {
	return &SupportBundle{State: state, Filename: filename}
}

type VolumeManager struct {
	ds *datastore.DataStore

	currentNodeID string
	sb            *SupportBundle
}

func NewVolumeManager(currentNodeID string, ds *datastore.DataStore) *VolumeManager {
	return &VolumeManager{
		ds: ds,

		currentNodeID: currentNodeID,
	}
}

func (m *VolumeManager) GetCurrentNodeID() string {
	return m.currentNodeID
}

func (m *VolumeManager) GetSupportBundle(filename string) (*SupportBundle, error) {
	if m.sb.Filename != filename {
		return nil, errors.Errorf("cannot find the bundle file - %s", filename)
	}

	return m.sb, nil
}

func (m *VolumeManager) DeleteSupportBundle() {
	os.Remove(filepath.Join("/tmp", m.sb.Filename))
	m.sb = nil
}

func (m *VolumeManager) GetBundleFileHandler() (io.ReadCloser, error) {
	f, err := os.Open(filepath.Join("/tmp", m.sb.Filename))
	if err != nil {
		m.sb.Error = BundleOpenFailed
		return nil, errors.Wrapf(err, "unable to open the bundle file")
	}
	return f, nil
}

func (m *VolumeManager) isPreviousSupportBundleExpired() bool {
	t := time.Now()
	return t.Sub(m.sb.createTime).Hours() >= 1
}

func (m *VolumeManager) Node2APIAddress(nodeID string) (string, error) {
	nodeIPMap, err := m.ds.GetManagerNodeIPMap()
	if err != nil {
		return "", err
	}
	ip, exists := nodeIPMap[nodeID]
	if !exists {
		return "", fmt.Errorf("cannot find longhorn manager on node %v", nodeID)
	}
	return types.GetAPIServerAddressFromIP(ip), nil
}

func (m *VolumeManager) List() (map[string]*longhorn.Volume, error) {
	return m.ds.ListVolumes()
}

// sortKeys accepts a map with string keys and returns a sorted slice of keys
func sortKeys(mapObj interface{}) ([]string, error) {
	if mapObj == nil {
		return []string{}, fmt.Errorf("BUG: mapObj was nil")
	}
	m := reflect.ValueOf(mapObj)
	if m.Kind() != reflect.Map {
		return []string{}, fmt.Errorf("BUG: expected map, got %v", m.Kind())
	}

	keys := make([]string, m.Len())
	for i, key := range m.MapKeys() {
		if key.Kind() != reflect.String {
			return []string{}, fmt.Errorf("BUG: expect map[string]interface{}, got map[%v]interface{}", key.Kind())
		}
		keys[i] = key.String()
	}
	sort.Strings(keys)
	return keys, nil
}

func (m *VolumeManager) ListSorted() ([]*longhorn.Volume, error) {
	volumeMap, err := m.List()
	if err != nil {
		return []*longhorn.Volume{}, err
	}

	volumes := make([]*longhorn.Volume, len(volumeMap))
	volumeNames, err := sortKeys(volumeMap)
	if err != nil {
		return []*longhorn.Volume{}, err
	}
	for i, volumeName := range volumeNames {
		volumes[i] = volumeMap[volumeName]
	}
	return volumes, nil
}

func (m *VolumeManager) Get(vName string) (*longhorn.Volume, error) {
	return m.ds.GetVolume(vName)
}

func (m *VolumeManager) GetEngines(vName string) (map[string]*longhorn.Engine, error) {
	return m.ds.ListVolumeEngines(vName)
}

func (m *VolumeManager) GetEnginesSorted(vName string) ([]*longhorn.Engine, error) {
	engineMap, err := m.ds.ListVolumeEngines(vName)
	if err != nil {
		return []*longhorn.Engine{}, err
	}

	engines := make([]*longhorn.Engine, len(engineMap))
	engineNames, err := sortKeys(engineMap)
	if err != nil {
		return []*longhorn.Engine{}, err
	}
	for i, engineName := range engineNames {
		engines[i] = engineMap[engineName]
	}
	return engines, nil
}

func (m *VolumeManager) GetReplicas(vName string) (map[string]*longhorn.Replica, error) {
	return m.ds.ListVolumeReplicas(vName)
}

func (m *VolumeManager) GetReplicasSorted(vName string) ([]*longhorn.Replica, error) {
	replicaMap, err := m.ds.ListVolumeReplicas(vName)
	if err != nil {
		return []*longhorn.Replica{}, err
	}

	replicas := make([]*longhorn.Replica, len(replicaMap))
	replicaNames, err := sortKeys(replicaMap)
	if err != nil {
		return []*longhorn.Replica{}, err
	}
	for i, replicaName := range replicaNames {
		replicas[i] = replicaMap[replicaName]
	}
	return replicas, nil
}

func (m *VolumeManager) getDefaultReplicaCount() (int, error) {
	c, err := m.ds.GetSettingAsInt(types.SettingNameDefaultReplicaCount)
	if err != nil {
		return 0, err
	}
	return int(c), nil
}

func (m *VolumeManager) Create(name string, spec *types.VolumeSpec) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to create volume %v: %+v", name, spec)
	}()

	if !util.ValidateName(name) {
		return nil, fmt.Errorf("invalid name %v", name)
	}

	size := spec.Size
	if spec.FromBackup != "" {
		backupTarget, err := GenerateBackupTarget(m.ds)
		if err != nil {
			return nil, err
		}
		backup, err := backupTarget.GetBackup(spec.FromBackup)
		if err != nil {
			return nil, fmt.Errorf("cannot get backup %v: %v", spec.FromBackup, err)
		}
		logrus.Infof("Override size of volume %v to %v because it's from backup", name, backup.VolumeSize)
		// formalize the final size to the unit in bytes
		size, err = util.ConvertSize(backup.VolumeSize)
		if err != nil {
			return nil, fmt.Errorf("get invalid size for volume %v: %v", backup.VolumeSize, err)
		}
		if baseImage, ok := backup.Labels[types.BaseImageLabel]; ok {
			spec.BaseImage = baseImage
		}
	}

	// make sure it's multiples of 4096
	size = util.RoundUpSize(size)

	if spec.NumberOfReplicas == 0 {
		spec.NumberOfReplicas, err = m.getDefaultReplicaCount()
		if err != nil {
			return nil, errors.Wrap(err, "BUG: cannot get valid number for setting default replica count")
		}
		logrus.Infof("Use the default number of replicas %v", spec.NumberOfReplicas)
	}
	defaultEngineImage, err := m.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if defaultEngineImage == "" {
		return nil, fmt.Errorf("BUG: Invalid empty Setting.EngineImage")
	}
	if err := m.CheckEngineImageReadiness(defaultEngineImage); err != nil {
		return nil, errors.Wrapf(err, "cannot create volume with image %v", defaultEngineImage)
	}

	if spec.Frontend != types.VolumeFrontendBlockDev && spec.Frontend != types.VolumeFrontendISCSI {
		return nil, fmt.Errorf("invalid volume frontend specified: %v", spec.Frontend)
	}

	if spec.BaseImage != "" {
		nodes, err := m.ListNodes()
		if err != nil {
			return nil, fmt.Errorf("couldn't list nodes")
		}
		for _, node := range nodes {
			conditions := types.GetNodeConditionFromStatus(node.Status, types.NodeConditionTypeMountPropagation)
			if conditions.Status != types.ConditionStatusTrue {
				return nil, fmt.Errorf("cannot support BaseImage, node doesn't support mount propagation: %v", node)
			}
		}
	}

	if err := m.validateRecurringJobs(spec.RecurringJobs); err != nil {
		return nil, err
	}

	v = &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: types.VolumeSpec{
			OwnerID:             "", // the first controller who see it will pick it up
			Size:                size,
			Frontend:            spec.Frontend,
			EngineImage:         defaultEngineImage,
			FromBackup:          spec.FromBackup,
			NumberOfReplicas:    spec.NumberOfReplicas,
			StaleReplicaTimeout: spec.StaleReplicaTimeout,
			BaseImage:           spec.BaseImage,
			RecurringJobs:       spec.RecurringJobs,
		},
	}
	v, err = m.ds.CreateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Created volume %v: %+v", v.Name, v.Spec)
	return v, nil
}

func (m *VolumeManager) Delete(name string) error {
	if err := m.ds.DeleteVolume(name); err != nil {
		return err
	}
	logrus.Debugf("Deleted volume %v", name)
	return nil
}

func (m *VolumeManager) Attach(name, nodeID string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to attach volume %v to %v", name, nodeID)
	}()

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}
	if v.Status.State != types.VolumeStateDetached {
		return nil, fmt.Errorf("invalid state to attach %v: %v", name, v.Status.State)
	}
	if err := m.CheckEngineImageReadiness(v.Spec.EngineImage); err != nil {
		return nil, errors.Wrapf(err, "cannot attach volume %v with image %v", v.Name, v.Spec.EngineImage)
	}

	condition := types.GetVolumeConditionFromStatus(v.Status, types.VolumeConditionTypeScheduled)
	if condition.Status != types.ConditionStatusTrue {
		return nil, fmt.Errorf("volume %v not scheduled", name)
	}

	// already desired to be attached
	if v.Spec.NodeID != "" {
		if v.Spec.NodeID != nodeID {
			return nil, fmt.Errorf("Node to be attached %v is different from previous spec %v", nodeID, v.Spec.NodeID)
		}
		return v, nil
	}
	v.Spec.NodeID = nodeID
	v.Spec.OwnerID = v.Spec.NodeID

	// Must be owned by the manager on the same node
	v, err = m.ds.UpdateVolumeAndOwner(v)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("Attaching volume %v to %v", v.Name, v.Spec.NodeID)
	return v, nil
}

func (m *VolumeManager) Detach(name string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to detach volume %v", name)
	}()

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}
	if v.Status.State != types.VolumeStateAttached && v.Status.State != types.VolumeStateAttaching {
		return nil, fmt.Errorf("invalid state to detach %v: %v", v.Name, v.Status.State)
	}

	oldNodeID := v.Spec.NodeID
	if oldNodeID == "" {
		return v, nil
	}

	v.Spec.OwnerID = m.currentNodeID
	v.Spec.NodeID = ""

	// Ownership transfer to the one called detach in case the original
	// owner is down (so it cannot do anything to proceed)
	v, err = m.ds.UpdateVolumeAndOwner(v)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("Detaching volume %v from %v", v.Name, oldNodeID)
	return v, nil
}

func (m *VolumeManager) Salvage(volumeName string, replicaNames []string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to salvage volume %v", volumeName)
	}()

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}
	if v.Status.State != types.VolumeStateDetached {
		return nil, fmt.Errorf("invalid volume state to salvage: %v", v.Status.State)
	}
	if v.Status.Robustness != types.VolumeRobustnessFaulted {
		return nil, fmt.Errorf("invalid robustness state to salvage: %v", v.Status.Robustness)
	}

	for _, name := range replicaNames {
		r, err := m.ds.GetReplica(name)
		if err != nil {
			return nil, err
		}
		if r.Spec.VolumeName != v.Name {
			return nil, fmt.Errorf("replica %v doesn't belong to volume %v", r.Name, v.Name)
		}
		if r.Spec.FailedAt == "" {
			// already updated, ignore it for idempotency
			continue
		}
		r.Spec.FailedAt = ""
		if _, err := m.ds.UpdateReplica(r); err != nil {
			return nil, err
		}
	}

	v.Spec.NodeID = ""
	v.Status.Robustness = types.VolumeRobustnessUnknown
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Salvaged replica %+v for volume %v", replicaNames, v.Name)
	return v, nil
}

func (m *VolumeManager) UpdateRecurringJobs(volumeName string, jobs []types.RecurringJob) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to update volume recurring jobs for %v", volumeName)
	}()

	if err = m.validateRecurringJobs(jobs); err != nil {
		return nil, err
	}

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	v.Spec.RecurringJobs = jobs
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Updated volume %v recurring jobs to %+v", v.Name, v.Spec.RecurringJobs)
	return v, nil
}

func (m *VolumeManager) checkDuplicateJobs(recurringJobs []types.RecurringJob) error {
	exist := map[string]bool{}
	for _, job := range recurringJobs {
		if _, ok := exist[job.Name]; ok {
			return fmt.Errorf("duplicate job %v in recurringJobs %v", job.Name, recurringJobs)
		}
		exist[job.Name] = true
	}
	return nil
}

func (m *VolumeManager) validateRecurringJobs(jobs []types.RecurringJob) error {
	if jobs == nil {
		return nil
	}
	for _, job := range jobs {
		if job.Cron == "" || job.Task == "" || job.Name == "" || job.Retain == 0 {
			return fmt.Errorf("invalid job %+v", job)
		}
		if len(job.Name) > types.MaximumJobNameSize {
			return fmt.Errorf("job name %v is too long, must be %v characters or less", job.Name, types.MaximumJobNameSize)
		}
	}
	if err := m.checkDuplicateJobs(jobs); err != nil {
		return err
	}
	return nil
}

func (m *VolumeManager) DeleteReplica(volumeName, replicaName string) error {
	hasHealthyReplicas := false
	rs, err := m.ds.ListVolumeReplicas(volumeName)
	if err != nil {
		return err
	}
	if _, exists := rs[replicaName]; !exists {
		return fmt.Errorf("cannot find replica %v of volume %v", replicaName, volumeName)
	}
	for _, r := range rs {
		if r.Name == replicaName {
			continue
		}
		if r.Spec.FailedAt == "" && r.Spec.HealthyAt != "" {
			hasHealthyReplicas = true
			break
		}
	}
	if !hasHealthyReplicas {
		return fmt.Errorf("no other healthy replica available, cannot delete replica %v since it may still contain data for recovery", replicaName)
	}
	if err := m.ds.DeleteReplica(replicaName); err != nil {
		return err
	}
	logrus.Debugf("Deleted replica %v of volume %v", replicaName, volumeName)
	return nil
}

func (m *VolumeManager) GetManagerNodeIPMap() (map[string]string, error) {
	podList, err := m.ds.ListManagerPods()
	if err != nil {
		return nil, err
	}

	nodeIPMap := map[string]string{}
	for _, pod := range podList {
		if nodeIPMap[pod.Spec.NodeName] != "" {
			return nil, fmt.Errorf("multiple managers on the node %v", pod.Spec.NodeName)
		}
		nodeIPMap[pod.Spec.NodeName] = pod.Status.PodIP
	}
	return nodeIPMap, nil
}

func (m *VolumeManager) EngineUpgrade(volumeName, image string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "cannot upgrade engine for volume %v using image %v", volumeName, image)
	}()
	if err := m.CheckEngineImageReadiness(image); err != nil {
		return nil, err
	}

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	if v.Spec.EngineImage == image {
		return nil, fmt.Errorf("upgrading in process for volume %v engine image from %v to %v already",
			v.Name, v.Status.CurrentImage, v.Spec.EngineImage)
	}

	if v.Spec.EngineImage != v.Status.CurrentImage && image != v.Status.CurrentImage {
		return nil, fmt.Errorf("upgrading in process for volume %v engine image from %v to %v, cannot upgrade to another engine image",
			v.Name, v.Status.CurrentImage, v.Spec.EngineImage)
	}
	if v.Spec.MigrationNodeID != "" {
		return nil, fmt.Errorf("cannot upgrade during migration")
	}

	oldImage := v.Spec.EngineImage
	v.Spec.EngineImage = image

	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	if image != v.Status.CurrentImage {
		logrus.Debugf("Upgrading volume %v engine image from %v to %v", v.Name, oldImage, v.Spec.EngineImage)
	} else {
		logrus.Debugf("Rolling back volume %v engine image to %v", v.Name, v.Status.CurrentImage)
	}

	return v, nil
}

func (m *VolumeManager) checkVolumeNotInMigration(volumeName string) error {
	v, err := m.Get(volumeName)
	if err != nil {
		return err
	}
	if v.Spec.MigrationNodeID != "" {
		return fmt.Errorf("cannot operate during migration")
	}
	return nil
}

func (m *VolumeManager) MigrationStart(name, nodeID string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to start migration for volume %v to %v", name, nodeID)
	}()

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}
	if v.Spec.NodeID == "" || v.Status.State != types.VolumeStateAttached {
		return nil, fmt.Errorf("invalid volume state to start migration %v", v.Status.State)
	}
	if v.Status.Robustness != types.VolumeRobustnessHealthy {
		return nil, fmt.Errorf("volume must be healthy to start migration")
	}
	if v.Spec.EngineImage != v.Status.CurrentImage {
		return nil, fmt.Errorf("upgrading in process for volume, cannot start migration")
	}
	if v.Spec.MigrationNodeID != "" {
		return nil, fmt.Errorf("migration already started")
	}
	if v.Spec.NodeID == nodeID {
		return nil, fmt.Errorf("cannot migrate to the same node as volume currently attached to")
	}

	if nodeID == "" {
		return nil, fmt.Errorf("empty migration destination")
	}
	if _, err := m.Node2APIAddress(nodeID); err != nil {
		return nil, err
	}

	v.Spec.MigrationNodeID = nodeID
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Migration started for volume %v from %v to %v", v.Name, v.Spec.NodeID, nodeID)
	return v, nil
}

func (m *VolumeManager) MigrationConfirm(name string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to confirm migration for volume %v", name)
	}()

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}
	if v.Spec.NodeID == "" || v.Status.State != types.VolumeStateAttached {
		return nil, fmt.Errorf("invalid volume state to confirm migration %v", v.Status.State)
	}
	if v.Spec.MigrationNodeID == "" {
		return nil, fmt.Errorf("no migration in process to be confirm")
	}

	// the engine must be running in order to be confirmed
	es, err := m.ds.ListVolumeEngines(name)
	attached := false
	for _, e := range es {
		if e.Spec.NodeID == v.Spec.MigrationNodeID &&
			e.Status.CurrentState == types.InstanceStateRunning {
			attached = true
			break
		}
	}
	if !attached {
		return nil, fmt.Errorf("migration is not ready yet")
	}

	oldNodeID := v.Spec.NodeID
	v.Spec.NodeID = v.Spec.MigrationNodeID
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Start migration confirming for volume %v from %v to %v", v.Name, oldNodeID, v.Spec.NodeID)
	return v, nil
}

func (m *VolumeManager) MigrationRollback(name string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to rollback migration for volume %v", name)
	}()

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}
	if v.Spec.MigrationNodeID == "" {
		return nil, fmt.Errorf("no migration in process to be rollback")
	}

	v.Spec.MigrationNodeID = ""
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Start rolling back migration for volume %v", v.Name)
	return v, nil
}

func (m *VolumeManager) validateReplicaCount(count int) error {
	if count < 1 || count > 20 {
		return fmt.Errorf("replica count value must between 1 to 20")
	}
	return nil
}

func (m *VolumeManager) UpdateReplicaCount(name string, count int) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to update replica count for volume %v", name)
	}()

	if err := m.validateReplicaCount(count); err != nil {
		return nil, err
	}

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}

	if v.Spec.NodeID == "" || v.Status.State != types.VolumeStateAttached {
		return nil, fmt.Errorf("invalid volume state to update replica count%v", v.Status.State)
	}
	if v.Status.Robustness != types.VolumeRobustnessHealthy {
		return nil, fmt.Errorf("volume must be healthy to update replica count")
	}
	if v.Spec.EngineImage != v.Status.CurrentImage {
		return nil, fmt.Errorf("upgrading in process, cannot update replica count")
	}
	if v.Spec.MigrationNodeID != "" {
		return nil, fmt.Errorf("migration in process, cannot update replica count")
	}
	oldCount := v.Spec.NumberOfReplicas
	v.Spec.NumberOfReplicas = count

	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Updated volume %v replica count from %v to %v", v.Name, oldCount, v.Spec.NumberOfReplicas)
	return v, nil
}
