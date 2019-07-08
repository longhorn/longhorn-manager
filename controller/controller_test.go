package controller

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/types"

	. "gopkg.in/check.v1"
)

const (
	TestNamespace      = "default"
	TestIP1            = "1.2.3.4"
	TestIP2            = "5.6.7.8"
	TestNode1          = "test-node-name-1"
	TestNode2          = "test-node-name-2"
	TestOwnerID1       = TestNode1
	TestOwnerID2       = TestNode2
	TestEngineImage    = "longhorn-engine:latest"
	TestManagerImage   = "longhorn-manager:latest"
	TestServiceAccount = "longhorn-service-account"

	TestInstanceManagerName = "instance-manager-engine-image-name"

	TestPod1 = "test-pod-name-1"
	TestPod2 = "test-pod-name-2"

	TestVolumeName         = "test-volume"
	TestVolumeSize         = 1073741824
	TestVolumeStaleTimeout = 60

	TestPVName  = "test-pv"
	TestPVCName = "test-pvc"

	TestVAName = "test-volume-attachment"

	TestTimeNow = "2015-01-02T00:00:00Z"

	TestDefaultDataPath   = "/var/lib/rancher/longhorn"
	TestDaemon1           = "longhorn-manager-1"
	TestDaemon2           = "longhorn-manager-2"
	TestDiskID1           = "fsid"
	TestDiskSize          = 5000000000
	TestDiskAvailableSize = 3000000000

	TestBackupTarget     = "s3://backupbucket@us-east-1/backupstore"
	TestBackupVolumeName = "test-backup-volume-for-restoration"
	TestBackupName       = "test-backup-for-restoration"
)

var (
	alwaysReady = func() bool { return true }
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	logrus.SetLevel(logrus.DebugLevel)
}

func getKey(obj interface{}, c *C) string {
	key, err := controller.KeyFunc(obj)
	c.Assert(err, IsNil)
	return key
}

func getOwnerReference(obj runtime.Object) *metav1.OwnerReference {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return nil
	}
	return &metadata.GetOwnerReferences()[0]
}

func randomIP() string {
	b := []string{}
	for i := 0; i < 4; i++ {
		b = append(b, strconv.Itoa(int(rand.Uint32()%255)))
	}
	return strings.Join(b, ".")
}

func getTestEngineImageName() string {
	return types.GetEngineImageChecksumName(TestEngineImage)
}

func randomPort() int {
	return rand.Int() % 30000
}
