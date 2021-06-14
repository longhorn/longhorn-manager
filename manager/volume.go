package manager

import (
	"fmt"
	"reflect"
	"sort"
	"strconv"

	"github.com/pkg/errors"
	"github.com/robfig/cron"
	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/backupstore"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

type VolumeManager struct {
	ds *datastore.DataStore

	currentNodeID string
	sb            *SupportBundle
}

const (
	MaxRecurringJobRetain = 50
)

func NewVolumeManager(currentNodeID string, ds *datastore.DataStore) *VolumeManager {
	return &VolumeManager{
		ds: ds,

		currentNodeID: currentNodeID,
	}
}

func (m *VolumeManager) GetCurrentNodeID() string {
	return m.currentNodeID
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
		err = errors.Wrapf(err, "unable to create volume %v", name)
		if err != nil {
			logrus.Errorf("manager: unable to create volume %v: %+v: %v", name, spec, err)
		}
	}()

	name = util.AutoCorrectName(name, datastore.NameMaximumLength)
	if !util.ValidateName(name) {
		return nil, fmt.Errorf("invalid name %v", name)
	}

	size := spec.Size
	if spec.FromBackup != "" {
		backupTarget, err := GenerateBackupTarget(m.ds)
		if err != nil {
			return nil, err
		}
		bvName, err := backupstore.GetVolumeFromBackupURL(spec.FromBackup)
		if err != nil {
			return nil, fmt.Errorf("cannot unmarshal backup volume name from backup URL %v: %v", spec.FromBackup, err)
		}
		backupVolume, err := backupTarget.GetVolume(bvName)
		if err != nil {
			return nil, fmt.Errorf("cannot get backup volume %v from backup URL %v: %v", bvName, spec.FromBackup, err)
		}
		if backupVolume.BackingImageName != "" {
			if spec.BackingImage == "" {
				spec.BackingImage = backupVolume.BackingImageName
				logrus.Debugf("Since the backing image is not specified during the restore, "+
					"the previous backing image %v used by backup volume %v will be set for volume %v creation",
					backupVolume.BackingImageName, bvName, name)
			}
		}

		backup, err := backupTarget.GetBackup(spec.FromBackup)
		if err != nil {
			return nil, fmt.Errorf("cannot get backup %v: %v", spec.FromBackup, err)
		}
		if backup == nil {
			return nil, fmt.Errorf("cannot find backup %v of volume %v", v.Spec.FromBackup, v.Name)
		}

		logrus.Infof("Override size of volume %v to %v because it's from backup", name, backup.VolumeSize)
		// formalize the final size to the unit in bytes
		size, err = util.ConvertSize(backup.VolumeSize)
		if err != nil {
			return nil, fmt.Errorf("get invalid size for volume %v: %v", backup.VolumeSize, err)
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

	if string(spec.ReplicaAutoBalance) == "" {
		spec.ReplicaAutoBalance = types.ReplicaAutoBalanceIgnored
		logrus.Infof("Use the %v to inherit global replicas auto-balance setting", spec.ReplicaAutoBalance)
	}
	if err := types.ValidateReplicaAutoBalance(spec.ReplicaAutoBalance); err != nil {
		return nil, errors.Wrapf(err, "cannot create volume with replica auto-balance %v", spec.ReplicaAutoBalance)
	}

	if string(spec.DataLocality) == "" {
		defaultDataLocality, err := m.GetSettingValueExisted(types.SettingNameDefaultDataLocality)
		if err != nil {
			return nil, errors.Wrapf(err, "cannot get valid mode for setting default data locality for volume: %v", name)
		}
		spec.DataLocality = types.DataLocality(defaultDataLocality)
	}
	if err := types.ValidateDataLocality(spec.DataLocality); err != nil {
		return nil, errors.Wrapf(err, "cannot create volume with data locality %v", spec.DataLocality)
	}

	if string(spec.AccessMode) == "" {
		spec.AccessMode = types.AccessModeReadWriteOnce
	}

	if spec.Migratable && spec.AccessMode != types.AccessModeReadWriteMany {
		return nil, fmt.Errorf("migratable volumes are only supported in ReadWriteMany (rwx) access mode")
	}

	defaultEngineImage, err := m.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
	if defaultEngineImage == "" {
		return nil, fmt.Errorf("BUG: Invalid empty Setting.EngineImage")
	}

	// Check engine version before disable revision counter
	if spec.RevisionCounterDisabled {
		if ok, err := m.canDisableRevisionCounter(defaultEngineImage); !ok {
			return nil, errors.Wrapf(err, "can not create volume with current engine image that doesn't support disable revision counter")
		}
	}

	if !spec.Standby {
		if spec.Frontend != types.VolumeFrontendBlockDev && spec.Frontend != types.VolumeFrontendISCSI {
			return nil, fmt.Errorf("invalid volume frontend specified: %v", spec.Frontend)
		}
	}

	if spec.BackingImage != "" {
		if _, err := m.ds.GetBackingImage(spec.BackingImage); err != nil {
			return nil, err
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
			Size:                    size,
			AccessMode:              spec.AccessMode,
			Migratable:              spec.Migratable,
			Frontend:                spec.Frontend,
			EngineImage:             defaultEngineImage,
			FromBackup:              spec.FromBackup,
			NumberOfReplicas:        spec.NumberOfReplicas,
			ReplicaAutoBalance:      spec.ReplicaAutoBalance,
			DataLocality:            spec.DataLocality,
			StaleReplicaTimeout:     spec.StaleReplicaTimeout,
			BackingImage:            spec.BackingImage,
			RecurringJobs:           spec.RecurringJobs,
			Standby:                 spec.Standby,
			DiskSelector:            spec.DiskSelector,
			NodeSelector:            spec.NodeSelector,
			RevisionCounterDisabled: spec.RevisionCounterDisabled,
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

func (m *VolumeManager) Attach(name, nodeID string, disableFrontend bool, attachedBy string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to attach volume %v to %v", name, nodeID)
	}()

	node, err := m.ds.GetNode(nodeID)
	if err != nil {
		return nil, err
	}
	readyCondition := types.GetCondition(node.Status.Conditions, types.NodeConditionTypeReady)
	if readyCondition.Status != types.ConditionStatusTrue {
		return nil, fmt.Errorf("node %v is not ready, couldn't attach volume %v to it", node.Name, name)
	}

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}

	if isReady, err := m.ds.CheckEngineImageReadyOnAtLeastOneVolumeReplica(v.Spec.EngineImage, v.Name, nodeID); !isReady {
		if err != nil {
			return nil, fmt.Errorf("cannot attach volume %v with image %v: %v", v.Name, v.Spec.EngineImage, err)
		}
		return nil, fmt.Errorf("cannot attach volume %v because the engine image %v is not deployed on at least one of the the replicas' nodes or the node that the volume is going to attach to", v.Name, v.Spec.EngineImage)
	}

	restoreCondition := types.GetCondition(v.Status.Conditions, types.VolumeConditionTypeRestore)
	if restoreCondition.Status == types.ConditionStatusTrue {
		return nil, fmt.Errorf("volume %v is restoring data", name)
	}

	if v.Status.RestoreRequired {
		return nil, fmt.Errorf("volume %v is pending restoring", name)
	}

	if v.Spec.NodeID == nodeID {
		logrus.Debugf("Volume %v is already attached to node %v", v.Name, v.Spec.NodeID)
		return v, nil
	}

	if v.Spec.MigrationNodeID == nodeID {
		logrus.Debugf("Volume %v is already migrating to node %v from node %v", v.Name, nodeID, v.Spec.NodeID)
		return v, nil
	}

	if v.Spec.AccessMode != types.AccessModeReadWriteMany && v.Status.State != types.VolumeStateDetached {
		return nil, fmt.Errorf("invalid state %v to attach RWO volume %v", v.Status.State, name)
	}

	isVolumeShared := v.Spec.AccessMode == types.AccessModeReadWriteMany && !v.Spec.Migratable
	isVolumeDetached := v.Spec.NodeID == ""
	if isVolumeDetached {
		if !isVolumeShared || disableFrontend {
			v.Spec.NodeID = nodeID
			logrus.Infof("Volume %v attachment to %v with disableFrontend %v requested", v.Name, v.Spec.NodeID, disableFrontend)
		}
	} else if isVolumeShared {
		// shared volumes only need to be attached if maintenance mode is requested
		// otherwise we just set the disabled frontend and last attached by states
		logrus.Debugf("No need to attach volume %v since it's shared via %v", v.Name, v.Status.ShareEndpoint)
	} else {
		// non shared volume that is already attached needs to be migratable
		// to be able to attach to a new node, without detaching from the previous node
		if !v.Spec.Migratable {
			return nil, fmt.Errorf("non migratable volume %v cannot attach to node %v is already attached to node %v", v.Name, nodeID, v.Spec.NodeID)
		}
		if v.Spec.MigrationNodeID != "" && v.Spec.MigrationNodeID != nodeID {
			return nil, fmt.Errorf("unable to migrate volume %v from %v to %v since it's already migrating to %v",
				v.Name, v.Spec.NodeID, nodeID, v.Spec.MigrationNodeID)
		}
		if v.Status.State != types.VolumeStateAttached {
			return nil, fmt.Errorf("invalid volume state to start migration %v", v.Status.State)
		}
		if v.Status.Robustness != types.VolumeRobustnessHealthy {
			return nil, fmt.Errorf("volume must be healthy to start migration")
		}
		if v.Spec.EngineImage != v.Status.CurrentImage {
			return nil, fmt.Errorf("upgrading in process for volume, cannot start migration")
		}
		if v.Spec.Standby || v.Status.IsStandby {
			return nil, fmt.Errorf("dr volume migration is not supported")
		}
		if v.Status.ExpansionRequired {
			return nil, fmt.Errorf("cannot migrate volume while an expansion is required")
		}

		v.Spec.MigrationNodeID = nodeID
		logrus.Infof("Volume %v migration from %v to %v requested", v.Name, v.Spec.NodeID, nodeID)
	}

	v.Spec.DisableFrontend = disableFrontend
	v.Spec.LastAttachedBy = attachedBy
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// Detach will handle regular detachment as well as volume migration confirmation/rollback
// if nodeID is not specified, the volume will be detached from all nodes
func (m *VolumeManager) Detach(name, nodeID string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to detach volume %v", name)
	}()

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}

	if v.Status.IsStandby {
		return nil, fmt.Errorf("cannot detach standby volume %v", v.Name)
	}

	if v.Spec.NodeID == "" && v.Spec.MigrationNodeID == "" {
		logrus.Infof("No need to detach volume %v is already detached from all nodes", v.Name)
		return v, nil
	}

	// shared volumes only need to be detached if they are attached in maintenance mode
	if v.Spec.AccessMode == types.AccessModeReadWriteMany && !v.Spec.Migratable && !v.Spec.DisableFrontend {
		logrus.Infof("No need to detach volume %v since it's shared via %v", v.Name, v.Status.ShareEndpoint)
		return v, nil
	}

	if nodeID != "" && nodeID != v.Spec.NodeID && nodeID != v.Spec.MigrationNodeID {
		logrus.Infof("No need to detach volume %v since it's not attached to node %v", v.Name, nodeID)
		return v, nil
	}

	isMigratingVolume := v.Spec.Migratable && v.Spec.MigrationNodeID != "" && v.Spec.NodeID != ""
	isMigrationConfirmation := isMigratingVolume && nodeID == v.Spec.NodeID
	isMigrationRollback := isMigratingVolume && nodeID == v.Spec.MigrationNodeID

	// Since all invalid/unexcepted cases have been handled above, we only need to take care of regular detach or migration here.
	if isMigrationConfirmation {
		if !m.isVolumeAvailableOnNode(name, v.Spec.MigrationNodeID) {
			return nil, fmt.Errorf("migration is not ready yet")
		}
		v.Spec.NodeID = v.Spec.MigrationNodeID
		v.Spec.MigrationNodeID = ""
		logrus.Infof("Volume %v migration from %v to %v confirmed", v.Name, nodeID, v.Spec.NodeID)
	} else if isMigrationRollback {
		v.Spec.MigrationNodeID = ""
		logrus.Infof("Volume %v migration from %v to %v rollback", v.Name, nodeID, v.Spec.NodeID)
	} else {
		v.Spec.NodeID = ""
		v.Spec.MigrationNodeID = ""
		logrus.Infof("Volume %v detachment from node %v requested", v.Name, nodeID)
	}

	v.Spec.DisableFrontend = false
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	return v, nil
}

func (m *VolumeManager) isVolumeAvailableOnNode(volume, node string) bool {
	es, _ := m.ds.ListVolumeEngines(volume)
	for _, e := range es {
		if e.Spec.NodeID == node && e.Status.CurrentState == types.InstanceStateRunning {
			return true
		}
	}

	return false
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
	v.Spec.NodeID = ""
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}

	for _, name := range replicaNames {
		r, err := m.ds.GetReplica(name)
		if err != nil {
			return nil, err
		}
		if r.Spec.VolumeName != v.Name {
			return nil, fmt.Errorf("replica %v doesn't belong to volume %v", r.Name, v.Name)
		}
		isDownOrDeleted, err := m.ds.IsNodeDownOrDeleted(r.Spec.NodeID)
		if err != nil {
			return nil, fmt.Errorf("Failed to check if the related node %v is still running for replica %v", r.Spec.NodeID, name)
		}
		if isDownOrDeleted {
			return nil, fmt.Errorf("Unable to check if the related node %v is down or deleted for replica %v", r.Spec.NodeID, name)
		}
		node, err := m.ds.GetNode(r.Spec.NodeID)
		if err != nil {
			return nil, fmt.Errorf("Failed to get the related node %v for replica %v", r.Spec.NodeID, name)
		}
		diskSchedulable := false
		for _, diskStatus := range node.Status.DiskStatus {
			if diskStatus.DiskUUID == r.Spec.DiskID {
				if types.GetCondition(diskStatus.Conditions, types.DiskConditionTypeSchedulable).Status == types.ConditionStatusTrue {
					diskSchedulable = true
					break
				}
			}
		}
		if !diskSchedulable {
			return nil, fmt.Errorf("Disk with UUID %v on node %v is unschedulable for replica %v", r.Spec.DiskID, r.Spec.NodeID, name)
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

	logrus.Debugf("Salvaged replica %+v for volume %v", replicaNames, v.Name)
	return v, nil
}

func (m *VolumeManager) Activate(volumeName string, frontend string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to activate volume %v", volumeName)
	}()

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	if !v.Status.IsStandby {
		return nil, fmt.Errorf("volume %v is already in active mode", v.Name)
	}
	if !v.Spec.Standby {
		return nil, fmt.Errorf("volume %v is being activated", v.Name)
	}

	if frontend != string(types.VolumeFrontendBlockDev) && frontend != string(types.VolumeFrontendISCSI) {
		return nil, fmt.Errorf("invalid frontend %v", frontend)
	}

	// ListBackupVolumes() will trigger the update for LastBackup
	_, err = m.ListBackupVolumes()
	if err != nil {
		logrus.Warnf("failed to update LastBackup and backup volume list before activating standby volume %v: %v", volumeName, err)
	}

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	var engine *longhorn.Engine
	es, err := m.ds.ListVolumeEngines(v.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to list engines for volume %v: %v", v.Name, err)
	}
	if len(es) != 1 {
		return nil, fmt.Errorf("found more than 1 engines for volume %v", v.Name)
	}
	for _, e := range es {
		engine = e
	}

	if v.Status.LastBackup != engine.Status.LastRestoredBackup || engine.Spec.RequestedBackupRestore != engine.Status.LastRestoredBackup {
		logrus.Infof("Standby volume %v will be activated after finishing restoration, "+
			"backup volume's latest backup: %v, "+
			"engine requested backup restore: %v, engine last restored backup: %v",
			v.Name, v.Status.LastBackup, engine.Spec.RequestedBackupRestore, engine.Status.LastRestoredBackup)
	}

	v.Spec.Frontend = types.VolumeFrontend(frontend)
	v.Spec.Standby = false
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("Activating volume %v with frontend %v", v.Name, frontend)
	return v, nil
}

func (m *VolumeManager) Expand(volumeName string, size int64) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to expand volume %v", volumeName)
	}()

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}

	if v.Status.State != types.VolumeStateDetached {
		return nil, fmt.Errorf("invalid volume state to expand: %v", v.Status.State)
	}

	if types.GetCondition(v.Status.Conditions, types.VolumeConditionTypeScheduled).Status != types.ConditionStatusTrue {
		return nil, fmt.Errorf("cannot expand volume before replica scheduling success")
	}

	size = util.RoundUpSize(size)

	kubernetesStatus := &v.Status.KubernetesStatus
	if kubernetesStatus.PVCName != "" && kubernetesStatus.LastPVCRefAt == "" {
		pvc, err := m.ds.GetPersistentVolumeClaim(kubernetesStatus.Namespace, kubernetesStatus.PVCName)
		if err != nil {
			return nil, err
		}

		requestedSize := resource.MustParse(strconv.FormatInt(size, 10))

		// TODO: Should check for pvc.Spec.Resources.Requests.Storage() here, once upgrade API to v0.18.x.
		pvcSpecValue, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]
		if !ok {
			return nil, fmt.Errorf("cannot get request storage")
		}

		if pvcSpecValue.Cmp(requestedSize) < 0 {
			pvc.Spec.Resources = corev1.ResourceRequirements{
				Requests: corev1.ResourceList{
					corev1.ResourceStorage: requestedSize,
				},
			}

			logrus.Infof("Persistent Volume Claim %v expansion from %v to %v requested", v.Name, v.Spec.Size, size)
			_, err = m.ds.UpdatePersistentVolumeClaim(kubernetesStatus.Namespace, pvc)
			if err != nil {
				return nil, err
			}

			// return and CSI plugin call this API later for Longhorn volume expansion
			return v, nil
		}

		// all other case belong to the CSI plugin call
		logrus.Infof("CSI plugin call to expand volume %v", v.Name)

		if pvcSpecValue.Cmp(requestedSize) > 0 {
			size = util.RoundUpSize(pvcSpecValue.Value())
		}
	}

	if v.Spec.Size >= size {
		logrus.Infof("Volume %v expansion is not necessary since current size %v >= %v", v.Name, v.Spec.Size, size)
		return v, nil
	}

	logrus.Infof("Volume %v expansion from %v to %v requested", v.Name, v.Spec.Size, size)
	v.Spec.Size = size

	// Support off-line expansion only.
	v.Spec.DisableFrontend = true

	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (m *VolumeManager) CancelExpansion(volumeName string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to cancel expansion for volume %v", volumeName)
	}()

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}
	if !v.Status.ExpansionRequired {
		return nil, fmt.Errorf("volume expansion is not started")
	}
	if v.Status.IsStandby {
		return nil, fmt.Errorf("canceling expansion for standby volume is not supported")
	}

	var engine *longhorn.Engine
	es, err := m.ds.ListVolumeEngines(v.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to list engines for volume %v: %v", v.Name, err)
	}
	if len(es) != 1 {
		return nil, fmt.Errorf("found more than 1 engines for volume %v", v.Name)
	}
	for _, e := range es {
		engine = e
	}

	if engine.Status.IsExpanding {
		return nil, fmt.Errorf("the engine expansion is in progress")
	}
	if engine.Status.CurrentSize == v.Spec.Size {
		return nil, fmt.Errorf("the engine expansion is already complete")
	}

	v.Spec.Size = engine.Status.CurrentSize
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("Canceling expansion for volume %v", v.Name)
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

	totalJobRetainCount := 0
	for _, job := range jobs {
		if job.Cron == "" || job.Task == "" || job.Name == "" || job.Retain == 0 {
			return fmt.Errorf("invalid job %+v", job)
		}
		if _, err := cron.ParseStandard(job.Cron); err != nil {
			return fmt.Errorf("invalid cron format(%v): %v", job.Cron, err)
		}
		if len(job.Name) > types.MaximumJobNameSize {
			return fmt.Errorf("job name %v is too long, must be %v characters or less", job.Name, types.MaximumJobNameSize)
		}
		if job.Labels != nil {
			if _, err := util.ValidateSnapshotLabels(job.Labels); err != nil {
				return err
			}
		}
		totalJobRetainCount += job.Retain
	}

	if err := m.checkDuplicateJobs(jobs); err != nil {
		return err
	}
	if totalJobRetainCount > MaxRecurringJobRetain {
		return fmt.Errorf("Job Can't retain more than %d snapshots", MaxRecurringJobRetain)
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

	// Only allow to upgrade to the default engine image if the setting `Automatically upgrade volumes' engine to the default engine image` is enabled
	concurrentAutomaticEngineUpgradePerNodeLimit, err := m.ds.GetSettingAsInt(types.SettingNameConcurrentAutomaticEngineUpgradePerNodeLimit)
	if err != nil {
		return nil, err
	}
	if concurrentAutomaticEngineUpgradePerNodeLimit > 0 {
		defaultEngineImage, err := m.ds.GetSettingValueExisted(types.SettingNameDefaultEngineImage)
		if err != nil {
			return nil, err
		}
		if image != defaultEngineImage {
			return nil, fmt.Errorf("updrading to %v is not allowed. "+
				"Only allow to upgrade to the default engine image %v because the setting "+
				"`Concurrent Automatic Engine Upgrade Per Node Limit` is greater than 0",
				image, defaultEngineImage)
		}
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

	if isReady, err := m.ds.CheckEngineImageReadyOnAllVolumeReplicas(image, v.Name, v.Status.CurrentNodeID); !isReady {
		if err != nil {
			return nil, fmt.Errorf("cannot upgrade engine image for volume %v from image %v to image %v: %v", v.Name, v.Spec.EngineImage, image, err)
		}
		return nil, fmt.Errorf("cannot upgrade engine image for volume %v from image %v to image %v because the engine image %v is not deployed on the replicas' nodes or the node that the volume is attached to", v.Name, v.Spec.EngineImage, image, image)
	}

	if isReady, err := m.ds.CheckEngineImageReadyOnAllVolumeReplicas(v.Status.CurrentImage, v.Name, v.Status.CurrentNodeID); !isReady {
		if err != nil {
			return nil, fmt.Errorf("cannot upgrade engine image for volume %v from image %v to image %v: %v", v.Name, v.Spec.EngineImage, image, err)
		}
		return nil, fmt.Errorf("cannot upgrade engine image for volume %v from image %v to image %v because the volume's current engine image %v is not deployed on the replicas' nodes or the node that the volume is attached to", v.Name, v.Spec.EngineImage, image, v.Status.CurrentImage)
	}

	if v.Spec.MigrationNodeID != "" {
		return nil, fmt.Errorf("cannot upgrade during migration")
	}

	// Note: Rebuild is not supported for old DR volumes and the handling of a degraded DR volume live upgrade will get stuck.
	//  Hence if you modify this part, the live upgrade should be prevented in API level for all old DR volumes.
	if v.Status.State == types.VolumeStateAttached && v.Status.Robustness != types.VolumeRobustnessHealthy {
		return nil, fmt.Errorf("cannot do live upgrade for a unhealthy volume %v", v.Name)
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

func (m *VolumeManager) UpdateReplicaCount(name string, count int) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to update replica count for volume %v", name)
	}()

	if err := types.ValidateReplicaCount(count); err != nil {
		return nil, err
	}

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}

	if v.Spec.NodeID == "" || v.Status.State != types.VolumeStateAttached {
		return nil, fmt.Errorf("invalid volume state to update replica count%v", v.Status.State)
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

func (m *VolumeManager) UpdateReplicaAutoBalance(name string, inputSpec types.ReplicaAutoBalance) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to update replica auto-balance for volume %v", name)
	}()

	if err = types.ValidateReplicaAutoBalance(inputSpec); err != nil {
		return nil, err
	}

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}

	if v.Spec.ReplicaAutoBalance == inputSpec {
		logrus.Debugf("Volume %v already has replica auto-balance set to %v", v.Name, inputSpec)
		return v, nil
	}

	oldSpec := v.Spec.ReplicaAutoBalance
	v.Spec.ReplicaAutoBalance = inputSpec
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("Updated volume %v replica auto-balance spec from %v to %v", v.Name, oldSpec, v.Spec.ReplicaAutoBalance)
	return v, nil
}

func (m *VolumeManager) UpdateDataLocality(name string, dataLocality types.DataLocality) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to update data locality for volume %v", name)
	}()

	if err := types.ValidateDataLocality(dataLocality); err != nil {
		return nil, err
	}

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}

	if v.Spec.DataLocality == dataLocality {
		logrus.Debugf("Volume %v already has data locality %v", v.Name, dataLocality)
		return v, nil
	}

	oldDataLocality := v.Spec.DataLocality
	v.Spec.DataLocality = dataLocality
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}

	logrus.Debugf("Updated volume %v data locality from %v to %v", v.Name, oldDataLocality, v.Spec.DataLocality)
	return v, nil
}

func (m *VolumeManager) UpdateAccessMode(name string, accessMode types.AccessMode) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to update access mode for volume %v", name)
	}()

	if err := types.ValidateAccessMode(accessMode); err != nil {
		return nil, err
	}

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}

	if v.Spec.AccessMode == accessMode {
		logrus.Debugf("Volume %v already has access mode %v", v.Name, accessMode)
		return v, nil
	}

	if v.Spec.NodeID != "" || v.Status.State != types.VolumeStateDetached {
		return nil, fmt.Errorf("can only update volume access mode while volume is detached")
	}

	oldAccessMode := v.Spec.AccessMode
	v.Spec.AccessMode = accessMode
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}

	logrus.Infof("Updated volume %v access mode from %v to %v", v.Name, oldAccessMode, accessMode)
	return v, nil
}

func (m *VolumeManager) canDisableRevisionCounter(engineImage string) (bool, error) {
	cliAPIVersion, err := m.ds.GetEngineImageCLIAPIVersion(engineImage)
	if err != nil {
		return false, err
	}
	if cliAPIVersion < engineapi.CLIVersionFour {
		return false, fmt.Errorf("current engine image version %v doesn't support disable revision counter", cliAPIVersion)
	}

	return true, nil
}
