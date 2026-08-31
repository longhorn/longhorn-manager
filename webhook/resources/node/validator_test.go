package node

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/longhorn/longhorn-manager/datastore"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	managerutil "github.com/longhorn/longhorn-manager/util"
)

const (
	nodeValidatorTestName      = "lost-node"
	nodeValidatorTestNamespace = "longhorn-system"
)

func TestValidateNodeDiskPaths(t *testing.T) {
	testCases := []struct {
		name          string
		disks         map[string]longhorn.DiskSpec
		errorMessages []string
	}{
		{
			name: "duplicate paths",
			disks: map[string]longhorn.DiskSpec{
				"disk-1": {Path: "/fake/path/disk1"},
				"disk-2": {Path: "/fake/path/disk1"},
			},
			errorMessages: []string{"duplicate disk paths", "node1", "disk-1", "disk-2", "/fake/path/disk1"},
		},
		{
			name: "unique paths",
			disks: map[string]longhorn.DiskSpec{
				"disk-1": {Path: "/fake/path/disk1"},
				"disk-2": {Path: "/fake/path/disk2"},
			},
		},
		{
			name: "normalized duplicate paths",
			disks: map[string]longhorn.DiskSpec{
				"disk-1": {Path: "/fake/path/disk1"},
				"disk-2": {Path: "/fake/path/../path/disk1"},
			},
			errorMessages: []string{"duplicate disk paths", "node1", "/fake/path/disk1"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateNodeDiskPaths("node1", tc.disks)
			if len(tc.errorMessages) == 0 {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			for _, message := range tc.errorMessages {
				assert.Contains(t, err.Error(), message)
			}
		})
	}
}

func TestFilepathCleanWithBDF(t *testing.T) {
	input := "00:1f.3"
	cleaned := filepath.Clean(input)
	assert.Equal(t, "00:1f.3", cleaned, "filepath.Clean should not alter BDF paths")
}

func TestNodeValidatorUpdateLostKubernetesNode(t *testing.T) {
	testCases := []struct {
		name             string
		kubeNodeExists   bool
		disksSynced      bool
		mutate           func(*longhorn.Node, *longhorn.Node)
		expectedErrorMsg string
	}{
		{
			name: "deleted node with unsynchronized disks allows scheduling disable",
			mutate: func(_, newNode *longhorn.Node) {
				newNode.Spec.AllowScheduling = false
			},
		},
		{
			name:        "deleted node with synchronized disks allows scheduling disable",
			disksSynced: true,
			mutate: func(_, newNode *longhorn.Node) {
				newNode.Spec.AllowScheduling = false
			},
		},
		{
			name: "deleted node with unsynchronized disks rejects disk change",
			mutate: func(_, newNode *longhorn.Node) {
				disk := newNode.Spec.Disks["disk-1"]
				disk.AllowScheduling = false
				newNode.Spec.Disks["disk-1"] = disk
			},
			expectedErrorMsg: "cannot modify disks",
		},
		{
			name:        "deleted node with synchronized disks rejects disk change",
			disksSynced: true,
			mutate: func(_, newNode *longhorn.Node) {
				disk := newNode.Spec.Disks["disk-1"]
				disk.AllowScheduling = false
				newNode.Spec.Disks["disk-1"] = disk
			},
			expectedErrorMsg: "cannot modify disks",
		},
		{
			name: "deleted node rejects scheduling disable with node spec co-update",
			mutate: func(_, newNode *longhorn.Node) {
				newNode.Spec.AllowScheduling = false
				newNode.Spec.Tags = []string{"changed"}
			},
			expectedErrorMsg: "only disabling scheduling",
		},
		{
			name: "deleted node with unsynchronized disks rejects unchanged scheduling",
			mutate: func(_, _ *longhorn.Node) {
			},
			expectedErrorMsg: "only disabling scheduling",
		},
		{
			name: "deleted node with unsynchronized disks rejects scheduling enable",
			mutate: func(oldNode, newNode *longhorn.Node) {
				oldNode.Spec.AllowScheduling = false
				newNode.Spec.AllowScheduling = true
			},
			expectedErrorMsg: "only disabling scheduling",
		},
		{
			name:        "deleted node with synchronized disks rejects scheduling enable",
			disksSynced: true,
			mutate: func(oldNode, newNode *longhorn.Node) {
				oldNode.Spec.AllowScheduling = false
				newNode.Spec.AllowScheduling = true
			},
			expectedErrorMsg: "only disabling scheduling",
		},
		{
			name:           "existing node with unsynchronized disks rejects scheduling disable",
			kubeNodeExists: true,
			mutate: func(_, newNode *longhorn.Node) {
				newNode.Spec.AllowScheduling = false
			},
			expectedErrorMsg: "spec and status of disks",
		},
		{
			name:           "updating block disk when v2 data engine is disabled is rejected",
			kubeNodeExists: true,
			disksSynced:    true,
			mutate: func(_, newNode *longhorn.Node) {
				newNode.Spec.Disks["disk-2"] = longhorn.DiskSpec{
					Path:            "/var/lib/longhorn/disk2",
					Type:            longhorn.DiskTypeBlock,
					AllowScheduling: true,
				}
			},
			expectedErrorMsg: "is a block device, but the SPDK feature is not enabled",
		},
		{
			name:           "annotation update on node with block disk remaining unchanged (v2 disabled) is allowed",
			kubeNodeExists: true,
			disksSynced:    true,
			mutate: func(oldNode, newNode *longhorn.Node) {
				blockDisk := oldNode.Spec.Disks["disk-1"]
				blockDisk.Type = longhorn.DiskTypeBlock

				oldNode.Spec.Disks["disk-1"] = blockDisk
				newNode.Spec.Disks["disk-1"] = blockDisk

				if newNode.Annotations == nil {
					newNode.Annotations = make(map[string]string)
				}
				newNode.Annotations["example.com/test-annotation"] = "updated-value"
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kubeClient := kubefake.NewSimpleClientset()
			validator := newNodeValidatorForTest(kubeClient)
			if tc.kubeNodeExists {
				err := validator.ds.KubeNodeInformer.GetStore().Add(&corev1.Node{
					ObjectMeta: metav1.ObjectMeta{Name: nodeValidatorTestName},
				})
				require.NoError(t, err)
			}
			oldNode, newNode := newNodeValidatorUpdate()
			if tc.disksSynced {
				oldNode.Status.DiskStatus["disk-1"] = &longhorn.DiskStatus{}
				newNode.Status.DiskStatus["disk-1"] = &longhorn.DiskStatus{}
			}
			tc.mutate(oldNode, newNode)

			err := validator.Update(nil, oldNode, newNode)
			if tc.expectedErrorMsg == "" {
				require.NoError(t, err)
				return
			}

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectedErrorMsg)
		})
	}
}

func newNodeValidatorForTest(kubeClient *kubefake.Clientset) *nodeValidator {
	lhClient := lhfake.NewClientset()
	extensionsClient := apiextensionsfake.NewSimpleClientset()
	informerFactories := managerutil.NewInformerFactories(nodeValidatorTestNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStore(nodeValidatorTestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	return &nodeValidator{ds: ds}
}

func newNodeValidatorUpdate() (*longhorn.Node, *longhorn.Node) {
	oldNode := &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeValidatorTestName,
			Namespace: nodeValidatorTestNamespace,
		},
		Spec: longhorn.NodeSpec{
			Name:            nodeValidatorTestName,
			AllowScheduling: true,
			Disks: map[string]longhorn.DiskSpec{
				"disk-1": {
					Path:            "/var/lib/longhorn",
					AllowScheduling: true,
				},
			},
		},
		Status: longhorn.NodeStatus{
			DiskStatus: map[string]*longhorn.DiskStatus{},
		},
	}
	return oldNode, oldNode.DeepCopy()
}
