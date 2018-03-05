package manager

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/engineapi"
	"github.com/rancher/longhorn-manager/types"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

type VolumeManager struct {
	ds *datastore.DataStore

	currentNodeID string
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

func (m *VolumeManager) Get(vName string) (*longhorn.Volume, error) {
	return m.ds.GetVolume(vName)
}

func (m *VolumeManager) GetEngine(vName string) (*longhorn.Engine, error) {
	return m.ds.GetVolumeEngine(vName)
}

func (m *VolumeManager) GetReplicas(vName string) (map[string]*longhorn.Replica, error) {
	return m.ds.GetVolumeReplicas(vName)
}

func (m *VolumeManager) Create(name string, spec *types.VolumeSpec) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to create volume %v: %+v", name, spec)
	}()

	// make it random node's responsibility
	ownerID, err := m.getRandomOwnerID()
	if err != nil {
		return nil, err
	}

	size := spec.Size
	if spec.FromBackup != "" {
		backup, err := engineapi.GetBackup(spec.FromBackup)
		if err != nil {
			return nil, fmt.Errorf("cannot get backup %v: %v", spec.FromBackup, err)
		}
		size = backup.VolumeSize
	}

	if spec.NumberOfReplicas == 0 {
		logrus.Warnf("Invalid number of replicas %v, override it to default", spec.NumberOfReplicas)
		spec.NumberOfReplicas = 3
	}

	v = &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: types.VolumeSpec{
			OwnerID:             ownerID,
			Size:                size,
			FromBackup:          spec.FromBackup,
			NumberOfReplicas:    spec.NumberOfReplicas,
			StaleReplicaTimeout: spec.StaleReplicaTimeout,
		},
	}
	v, err = m.ds.CreateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Created volume %v", v.Name)
	return v, nil
}

func (m *VolumeManager) getRandomOwnerID() (string, error) {
	var node string

	nodeIPMap, err := m.ds.GetManagerNodeIPMap()
	if err != nil {
		return "", err
	}
	// map is random in Go
	for node = range nodeIPMap {
		break
	}

	return node, nil
}
func (m *VolumeManager) Delete(name string) error {
	return m.ds.DeleteVolume(name)
}

func (m *VolumeManager) Attach(name, nodeID string) (v *longhorn.Volume, err error) {
	defer func() {
		err = errors.Wrapf(err, "unable to attach volume %v to %v", name, nodeID)
	}()

	v, err = m.ds.GetVolume(name)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, fmt.Errorf("cannot find volume %v", name)
	}
	if v.Status.State != types.VolumeStateDetached {
		return nil, fmt.Errorf("invalid state to attach %v: %v", name, v.Status.State)
	}
	// already desired to be attached
	if v.Spec.NodeID != "" {
		if v.Spec.NodeID != nodeID {
			return nil, fmt.Errorf("Node to be attached %v is different from previous spec %v", nodeID, v.Spec.NodeID)
		}
		return v, nil
	}
	v.Spec.NodeID = nodeID
	// Must be owned by the manager on the same node
	v.Spec.OwnerID = v.Spec.NodeID
	v, err = m.ds.UpdateVolume(v)
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
	if v == nil {
		return nil, fmt.Errorf("cannot find volume %v", name)
	}
	if v.Status.State != types.VolumeStateAttached && v.Status.State != types.VolumeStateAttaching {
		return nil, fmt.Errorf("invalid state to detach %v: %v", v.Name, v.Status.State)
	}

	oldNodeID := v.Spec.NodeID
	if oldNodeID == "" {
		return v, nil
	}

	v.Spec.NodeID = ""
	v, err = m.ds.UpdateVolume(v)
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
	if v == nil {
		return nil, fmt.Errorf("cannot find volume %v", volumeName)
	}
	if v.Status.State != types.VolumeStateDetached {
		return nil, fmt.Errorf("invalid volume state to salvage: %v", v.Status.State)
	}
	if v.Status.Robustness != types.VolumeRobustnessFaulted {
		return nil, fmt.Errorf("invalid robustness state to salvage: %v", v.Status.Robustness)
	}

	for _, names := range replicaNames {
		r, err := m.ds.GetReplica(names)
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

	for _, job := range jobs {
		if job.Cron == "" || job.Type == "" || job.Name == "" || job.Retain == 0 {
			return nil, fmt.Errorf("invalid job %+v", job)
		}
	}

	v, err = m.ds.GetVolume(volumeName)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return nil, fmt.Errorf("cannot find volume %v", volumeName)
	}

	v.Spec.RecurringJobs = jobs
	v, err = m.ds.UpdateVolume(v)
	if err != nil {
		return nil, err
	}
	logrus.Debugf("Updating volume %v recurring jobs", v.Name)
	return v, nil
}

func (m *VolumeManager) DeleteReplica(replicaName string) error {
	return m.ds.DeleteReplica(replicaName)
}

func (m *VolumeManager) GetManagerNodeIPMap() (map[string]string, error) {
	return m.ds.GetManagerNodeIPMap()
}
