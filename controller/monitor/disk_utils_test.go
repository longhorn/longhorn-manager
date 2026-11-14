package monitor

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	kubefake "k8s.io/client-go/kubernetes/fake"

	lhns "github.com/longhorn/go-common-libs/ns"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"

	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

// TestSyncDirectory covers longhorn/longhorn#14102: the disk config file must be durable before its UUID is
// recorded, so syncing the data path has to work on a directory and report a path that cannot be synced.
func TestSyncDirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "longhorn-disk.cfg"), []byte("{}"), 0644))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "replicas"), 0755))

	require.NoError(t, syncDirectory(dir))

	require.Error(t, syncDirectory(filepath.Join(dir, "missing")))
}

// hostlessJoiner runs the function in the test process and fails the failOnCall-th call, if set.
type hostlessJoiner struct {
	calls      *int
	failOnCall int
}

func (hostlessJoiner) Revert() error { return nil }

func (j hostlessJoiner) Run(fn func() (interface{}, error)) (interface{}, error) {
	*j.calls++
	if *j.calls == j.failOnCall {
		return nil, errors.New("injected host failure")
	}
	return fn()
}

// TestGenerateFilesystemTypeDiskConfigKeepsReplicas covers longhorn/longhorn#14102: when generating the config fails
// at the directory sync, only the generated config file may be rolled back. The replica subdirectory can already hold
// replica data when the disk has no recorded UUID and its config is missing.
func TestGenerateFilesystemTypeDiskConfigKeepsReplicas(t *testing.T) {
	kubeClient := kubefake.NewSimpleClientset()
	lhClient := lhfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(testNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStoreForGlobal(testNamespace, lhClient, kubeClient, apiextensionsfake.NewSimpleClientset(), informerFactories)

	diskPath := t.TempDir()
	replicaData := filepath.Join(diskPath, util.ReplicaDirectory, "vol-1-6b63f803", "volume-head-000.img")
	require.NoError(t, os.MkdirAll(filepath.Dir(replicaData), 0755))
	require.NoError(t, os.WriteFile(replicaData, []byte("replica data"), 0644))

	origJoiner := lhns.NewJoiner
	t.Cleanup(func() { lhns.NewJoiner = origJoiner })
	// Host calls in order: GetFileInfo, WriteFile, CreateDirectory, SyncFile, then the directory sync.
	calls := 0
	lhns.NewJoiner = func(string, time.Duration) (lhns.JoinerInterface, error) {
		return hostlessJoiner{calls: &calls, failOnCall: 5}, nil
	}

	_, err := generateFilesystemTypeDiskConfig("default-disk", diskPath, ds)
	require.ErrorContains(t, err, "injected host failure")

	require.NoFileExists(t, filepath.Join(diskPath, util.DiskConfigFile))
	require.FileExists(t, replicaData, "existing replica data must survive the rollback")
}
