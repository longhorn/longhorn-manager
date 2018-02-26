package controller

import (
	"math/rand"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/kubernetes/pkg/controller"
	"testing"

	. "gopkg.in/check.v1"
)

const (
	TestNamespace   = "default"
	TestThreadiness = 10
	TestRestoreFrom = "vfs://empty"
	TestRestoreName = "empty"
	TestIP1         = "1.2.3.4"
	TestIP2         = "5.6.7.8"
	TestNode1       = "test-node-name-1"
	TestNode2       = "test-node-name-2"
	TestOwnerID1    = TestNode1
	TestOwnerID2    = TestNode2
	TestEngineImage = "longhorn-engine:latest"

	TestReplica1Name = "replica-volumename-1"
	TestReplica2Name = "replica-volumename-2"
	TestPodName      = "test-pod-name"

	TestVolumeName         = "test-volume"
	TestVolumeSize         = "1g"
	TestVolumeStaleTimeout = 60

	TestTimeNow = "2015-01-02T00:00:00Z"
)

var (
	alwaysReady = func() bool { return true }
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
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
