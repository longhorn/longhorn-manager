package controller

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"

	. "gopkg.in/check.v1"
)

const (
	TestNamespace                 = "default"
	TestIP1                       = "1.2.3.4"
	TestIP2                       = "5.6.7.8"
	TestPort1                     = 9501
	TestNode1                     = "test-node-name-1"
	TestNode2                     = "test-node-name-2"
	TestOwnerID1                  = TestNode1
	TestOwnerID2                  = TestNode2
	TestEngineImage               = "longhorn-engine:latest"
	TestUpgradedEngineImage       = "longhorn-engine:upgraded"
	TestInstanceManagerImage      = "longhorn-instance-manager:latest"
	TestExtraInstanceManagerImage = "longhorn-instance-manager:upgraded"
	TestManagerImage              = "longhorn-manager:latest"
	TestServiceAccount            = "longhorn-service-account"

	TestBackingImage = "test-backing-image"

	TestInstanceManagerName1 = "instance-manager-engine-image-name-1"
	TestEngineManagerName    = "instance-manager-e-test-name"
	TestReplicaManagerName   = "instance-manager-r-test-name"

	TestPod1 = "test-pod-name-1"
	TestPod2 = "test-pod-name-2"

	TestVolumeName         = "test-volume"
	TestVolumeSize         = 1073741824
	TestVolumeStaleTimeout = 60
	TestEngineName         = "test-volume-engine"

	TestPVName  = "test-pv"
	TestPVCName = "test-pvc"

	TestVAName = "test-volume-attachment"

	TestTimeNow = "2015-01-02T00:00:00Z"

	TestDefaultDataPath   = "/var/lib/longhorn"
	TestDaemon1           = "longhorn-manager-1"
	TestDaemon2           = "longhorn-manager-2"
	TestDiskID1           = "fsid"
	TestDiskID2           = "fsid-2"
	TestDiskID3           = "fsid-3"
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

func newSetting(name, value string) *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Value: value,
	}
}

func newDefaultInstanceManagerImageSetting() *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: string(types.SettingNameDefaultInstanceManagerImage),
		},
		Value: TestInstanceManagerImage,
	}
}

func newEngineImage(image string, state longhorn.EngineImageState) *longhorn.EngineImage {
	return &longhorn.EngineImage{
		ObjectMeta: metav1.ObjectMeta{
			Name:       types.GetEngineImageChecksumName(image),
			Namespace:  TestNamespace,
			UID:        uuid.NewUUID(),
			Finalizers: []string{longhornFinalizerKey},
		},
		Spec: longhorn.EngineImageSpec{
			Image: image,
		},
		Status: longhorn.EngineImageStatus{
			OwnerID: TestNode1,
			State:   state,
			EngineVersionDetails: longhorn.EngineVersionDetails{
				Version:   "latest",
				GitCommit: "latest",

				CLIAPIVersion:           4,
				CLIAPIMinVersion:        3,
				ControllerAPIVersion:    3,
				ControllerAPIMinVersion: 3,
				DataFormatVersion:       1,
				DataFormatMinVersion:    1,
			},
			Conditions: map[string]longhorn.Condition{
				longhorn.EngineImageConditionTypeReady: {
					Type:   longhorn.EngineImageConditionTypeReady,
					Status: longhorn.ConditionStatusTrue,
				},
			},
			NodeDeploymentMap: map[string]bool{},
		},
	}
}

func newEngineImageDaemonSet() *appsv1.DaemonSet {
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      getTestEngineImageDaemonSetName(),
			Namespace: TestNamespace,
			Labels:    types.GetEngineImageLabels(getTestEngineImageName()),
		},
		Spec: appsv1.DaemonSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: types.GetEIDaemonSetLabelSelector(getTestEngineImageName()),
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Name:   getTestEngineImageDaemonSetName(),
					Labels: types.GetEIDaemonSetLabelSelector(getTestEngineImageName()),
				},
				Spec: corev1.PodSpec{
					ServiceAccountName: TestServiceAccount,
					Containers: []corev1.Container{
						{
							Name:  getTestEngineImageDaemonSetName(),
							Image: TestEngineImage,
						},
					},
				},
			},
		},
		Status: appsv1.DaemonSetStatus{
			DesiredNumberScheduled: 1,
			NumberAvailable:        1,
		},
	}
}

func newPod(status *corev1.PodStatus, name, namespace, nodeID string) *corev1.Pod {
	if status == nil {
		return nil
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: TestServiceAccount,
			NodeName:           nodeID,
		},
		Status: *status,
	}
}

func newInstanceManager(
	name string,
	imType longhorn.InstanceManagerType,
	currentState longhorn.InstanceManagerState,
	currentOwnerID, nodeID, ip string,
	instances map[string]longhorn.InstanceProcess,
	isDeleting bool) *longhorn.InstanceManager {

	im := &longhorn.InstanceManager{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			UID:       uuid.NewUUID(),
			Labels:    types.GetInstanceManagerLabels(nodeID, TestInstanceManagerImage, imType),
		},
		Spec: longhorn.InstanceManagerSpec{
			Image:  TestInstanceManagerImage,
			NodeID: nodeID,
			Type:   imType,
		},
		Status: longhorn.InstanceManagerStatus{
			OwnerID:      currentOwnerID,
			CurrentState: currentState,
			IP:           ip,
			Instances:    instances,
		},
	}

	if isDeleting {
		now := metav1.NewTime(time.Now())
		im.DeletionTimestamp = &now
	}
	return im
}

func getKey(obj interface{}, c *C) string {
	key, err := controller.KeyFunc(obj)
	c.Assert(err, IsNil)
	return key
}

func getTestNow() string {
	return TestTimeNow
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

func getTestEngineImageDaemonSetName() string {
	return types.GetDaemonSetNameFromEngineImageName(types.GetEngineImageChecksumName(TestEngineImage))
}

func randomPort() int {
	return rand.Int() % 30000
}

func fakeEngineBinaryChecker(image string) bool {
	return true
}

func fakeEngineImageUpdater(ei *longhorn.EngineImage) error {
	return nil
}

func (s *TestSuite) TestIsSameGuaranteedCPURequirement(c *C) {
	var (
		a, b *corev1.ResourceRequirements
		err  error
	)

	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, true)

	b = &corev1.ResourceRequirements{}
	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, true)

	b.Requests = corev1.ResourceList{}
	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, true)

	b.Requests[corev1.ResourceCPU], err = resource.ParseQuantity("0")
	c.Assert(err, IsNil)
	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, true)

	b.Requests[corev1.ResourceCPU], err = resource.ParseQuantity("0m")
	c.Assert(err, IsNil)
	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, true)

	a = &corev1.ResourceRequirements{}
	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, true)

	a.Requests = corev1.ResourceList{}
	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, true)

	a.Requests[corev1.ResourceCPU], err = resource.ParseQuantity("0")
	c.Assert(err, IsNil)
	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, true)

	a.Requests[corev1.ResourceCPU], err = resource.ParseQuantity("0m")
	c.Assert(err, IsNil)
	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, true)

	b.Requests[corev1.ResourceCPU], err = resource.ParseQuantity("250m")
	a = &corev1.ResourceRequirements{}
	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, false)

	b.Requests[corev1.ResourceCPU], err = resource.ParseQuantity("250m")
	a.Requests = corev1.ResourceList{}
	a.Requests[corev1.ResourceCPU], err = resource.ParseQuantity("0.25")
	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, true)
}
