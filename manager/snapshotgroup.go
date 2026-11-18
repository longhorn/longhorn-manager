package manager

import (
	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (m *VolumeManager) GetSnapshotGroup(name string) (*longhorn.SnapshotGroup, error) {
	return m.ds.GetSnapshotGroup(name)
}

func (m *VolumeManager) ListSnapshotGroupsSorted() ([]*longhorn.SnapshotGroup, error) {
	snapshotGroupMap, err := m.ds.ListSnapshotGroups()
	if err != nil {
		return []*longhorn.SnapshotGroup{}, err
	}

	snapshotGroups := make([]*longhorn.SnapshotGroup, len(snapshotGroupMap))
	snapshotGroupNames, err := util.SortKeys(snapshotGroupMap)
	if err != nil {
		return []*longhorn.SnapshotGroup{}, err
	}
	for i, name := range snapshotGroupNames {
		snapshotGroups[i] = snapshotGroupMap[name]
	}
	return snapshotGroups, nil
}

func (m *VolumeManager) CreateSnapshotGroup(name string, spec *longhorn.SnapshotGroupSpec) (*longhorn.SnapshotGroup, error) {
	// No name auto-correction: the name is identity (member snapshot names
	// embed it) and the webhook is the single validator.
	snapshotGroup := &longhorn.SnapshotGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: *spec,
	}

	snapshotGroup, err := m.ds.CreateSnapshotGroup(snapshotGroup)
	if err != nil {
		return nil, err
	}
	logrus.Infof("Created snapshot group %v", name)
	return snapshotGroup, nil
}

// ResolveSnapshotGroupMemberCandidates previews the member set a snapshot
// group spec would select.
func (m *VolumeManager) ResolveSnapshotGroupMemberCandidates(spec *longhorn.SnapshotGroupSpec) ([]datastore.SnapshotGroupMemberCandidate, error) {
	return m.ds.ResolveSnapshotGroupMemberCandidates(spec)
}

func (m *VolumeManager) DeleteSnapshotGroup(name string) error {
	if err := m.ds.DeleteSnapshotGroup(name); err != nil {
		return err
	}
	logrus.Infof("Deleted snapshot group %v", name)
	return nil
}
