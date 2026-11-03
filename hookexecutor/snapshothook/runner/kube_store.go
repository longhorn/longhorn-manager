package runner

import (
	"context"
	"fmt"
	"strconv"

	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhlisters "github.com/longhorn/longhorn-manager/k8s/pkg/client/listers/longhorn/v1beta2"
)

// KubeStore implements Store with the API server and the executor's own
// listers.
//
// It does not reuse the datastore package: the hook executor is a separate
// process that watches only what it needs, while a DataStore requires
// informers for every resource the manager uses.
type KubeStore struct {
	namespace  string
	lhClient   lhclientset.Interface
	kubeClient clientset.Interface

	groupLister   lhlisters.SnapshotGroupLister
	volumeLister  lhlisters.VolumeLister
	settingLister lhlisters.SettingLister
}

// NewKubeStore wires the clientsets and listers into a Store.
func NewKubeStore(namespace string, lhClient lhclientset.Interface, kubeClient clientset.Interface,
	groupLister lhlisters.SnapshotGroupLister, volumeLister lhlisters.VolumeLister, settingLister lhlisters.SettingLister) *KubeStore {
	return &KubeStore{
		namespace:     namespace,
		lhClient:      lhClient,
		kubeClient:    kubeClient,
		groupLister:   groupLister,
		volumeLister:  volumeLister,
		settingLister: settingLister,
	}
}

func (s *KubeStore) Namespace() string {
	return s.namespace
}

func (s *KubeStore) GetSnapshotGroup(ctx context.Context, name string) (*longhorn.SnapshotGroup, error) {
	return s.lhClient.LonghornV1beta2().SnapshotGroups(s.namespace).Get(ctx, name, metav1.GetOptions{})
}

func (s *KubeStore) UpdateSnapshotGroupStatus(ctx context.Context, group *longhorn.SnapshotGroup) (*longhorn.SnapshotGroup, error) {
	return s.lhClient.LonghornV1beta2().SnapshotGroups(s.namespace).UpdateStatus(ctx, group, metav1.UpdateOptions{})
}

func (s *KubeStore) ListSnapshotGroupsRO() ([]*longhorn.SnapshotGroup, error) {
	return s.groupLister.SnapshotGroups(s.namespace).List(labels.Everything())
}

func (s *KubeStore) GetVolumeRO(name string) (*longhorn.Volume, error) {
	return s.volumeLister.Volumes(s.namespace).Get(name)
}

func (s *KubeStore) GetVolumeHookPolicy(ctx context.Context, namespace, name string) (*longhorn.VolumeHookPolicy, error) {
	return s.lhClient.LonghornV1beta2().VolumeHookPolicies(namespace).Get(ctx, name, metav1.GetOptions{})
}

func (s *KubeStore) ListPods(ctx context.Context, namespace string) ([]corev1.Pod, error) {
	list, err := s.kubeClient.CoreV1().Pods(namespace).List(ctx, util.NonTerminalPodsListOptions())
	if err != nil {
		return nil, err
	}
	return list.Items, nil
}

func (s *KubeStore) FindPod(ctx context.Context, namespace, name string) (*corev1.Pod, error) {
	list, err := s.kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		FieldSelector: fields.OneTermEqualSelector("metadata.name", name).String(),
	})
	if err != nil {
		return nil, err
	}
	if len(list.Items) == 0 {
		return nil, nil
	}
	return &list.Items[0], nil
}

// GetSettingAsInt reads an integer setting, falling back to the definition
// default when the Setting object does not exist.
func (s *KubeStore) GetSettingAsInt(name types.SettingName) (int64, error) {
	definition, ok := types.GetSettingDefinition(name)
	if !ok {
		return -1, fmt.Errorf("setting %v is not supported", name)
	}
	if definition.Type != types.SettingTypeInt {
		return -1, fmt.Errorf("setting %v is not an integer setting", name)
	}
	value := definition.Default
	setting, err := s.settingLister.Settings(s.namespace).Get(string(name))
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return -1, err
		}
	} else {
		value = setting.Value
	}
	return strconv.ParseInt(value, 10, 64)
}
