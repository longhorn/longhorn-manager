package controller

import (
	"math/rand"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/kubernetes/pkg/controller"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/engineapi"
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
	TestShareManagerImage         = "longhorn-share-manager:latest"
	TestServiceAccount            = "longhorn-service-account"

	TestBackingImage = "test-backing-image"

	TestInstanceManagerName = "instance-manager-test-name"

	TestPod1 = "test-pod-name-1"
	TestPod2 = "test-pod-name-2"

	TestVolumeName         = "test-volume"
	TestVolumeSize         = 1073741824
	TestVolumeStaleTimeout = 60
	TestEngineName         = "test-volume-engine"
	TestReplicaName        = "test-volume-replica"

	TestPVName  = "test-pv"
	TestPVCName = "test-pvc"

	TestStorageClassName = "test-storage-class"

	TestVAName = "test-volume-attachment"

	TestClusterRoleName        = "test-cluster-role"
	TestClusterRoleBindingName = "test-cluster-role-binding"
	TestEngineImageName        = "test-engine-image"
	TestPodSecurityPolicyName  = "test-pod-security-policy"
	TestRoleName               = "test-role"
	TestRoleBindingName        = "test-role-binding"
	TestServiceName            = "test-service"
	TestServicePortName        = "test-service-port"

	TestTimeNow = "2015-01-02T00:00:00Z"

	TestDefaultDataPath   = "/var/lib/longhorn"
	TestDaemon1           = "longhorn-manager-1"
	TestDaemon2           = "longhorn-manager-2"
	TestDiskID1           = "fsid"
	TestDiskID2           = "fsid-2"
	TestDiskID3           = "fsid-3"
	TestDiskSize          = 5000000000
	TestDiskAvailableSize = 3000000000

	TestDeploymentName = "test-deployment"

	TestBackupTarget     = "s3://backupbucket@us-east-1/backupstore"
	TestBackupVolumeName = "test-backup-volume-for-restoration"
	TestBackupName       = "test-backup-for-restoration"

	TestRecurringJobName      = "test-recurring-job"
	TestRecurringJobGroupName = "test-recurring-job-group"

	TestSystemBackupName                    = "system-backup-0"
	TestSystemBackupNameGetConfigFailed     = "system-backup-get-config-failed"
	TestSystemBackupNameListFailed          = "system-backup-list-failed"
	TestSystemBackupNameUploadFailed        = "system-backup-upload-failed"
	TestSystemBackupNameUploadExceedTimeout = "system-backup-upload-exceed-timeout"

	TestSystemBackupGitCommit       = "12345abcd"
	TestSystemBackupLonghornVersion = "v1.4.0"
	TestSystemBackupURIFmt          = "backupstore/system-backups/%v/%v"

	TestCustomResourceDefinitionName = "test-crd"
	TestVolumeAttachmentName         = "test-volume"

	TestDiskPathFSType = "ext4"
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

				CLIAPIVersion:           8,
				CLIAPIMinVersion:        3,
				ControllerAPIVersion:    3,
				ControllerAPIMinVersion: 3,
				DataFormatVersion:       1,
				DataFormatMinVersion:    1,
			},
			Conditions: []longhorn.Condition{
				{
					Type:   longhorn.EngineImageConditionTypeReady,
					Status: longhorn.ConditionStatusTrue,
				},
			},
			NodeDeploymentMap: map[string]bool{},
		},
	}
}

func newOrphan(spec longhorn.OrphanSpec, status longhorn.OrphanStatus) *longhorn.Orphan {
	return &longhorn.Orphan{
		ObjectMeta: metav1.ObjectMeta{
			Name: types.GetOrphanChecksumNameForOrphanedDirectory(
				spec.NodeID,
				spec.Parameters[longhorn.OrphanDiskName],
				spec.Parameters[longhorn.OrphanDiskPath],
				spec.Parameters[longhorn.OrphanDiskUUID],
				spec.Parameters[longhorn.OrphanDataName],
			),
			Namespace: TestNamespace,
		},
		Spec:   spec,
		Status: status,
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
	currentState longhorn.InstanceManagerState,
	currentOwnerID, nodeID, ip string,
	instanceEngines map[string]longhorn.InstanceProcess,
	instanceReplicas map[string]longhorn.InstanceProcess,
	dataEngine longhorn.DataEngineType,
	isDeleting bool) *longhorn.InstanceManager {

	im := &longhorn.InstanceManager{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			UID:       uuid.NewUUID(),
			Labels:    types.GetInstanceManagerLabels(nodeID, TestInstanceManagerImage, longhorn.InstanceManagerTypeAllInOne, dataEngine),
		},
		Spec: longhorn.InstanceManagerSpec{
			Image:      TestInstanceManagerImage,
			NodeID:     nodeID,
			Type:       longhorn.InstanceManagerTypeAllInOne,
			DataEngine: dataEngine,
		},
		Status: longhorn.InstanceManagerStatus{
			OwnerID:          currentOwnerID,
			CurrentState:     currentState,
			IP:               ip,
			InstanceEngines:  instanceEngines,
			InstanceReplicas: instanceReplicas,
		},
	}

	if currentState == longhorn.InstanceManagerStateRunning {
		im.Status.APIMinVersion = engineapi.MinInstanceManagerAPIVersion
		im.Status.APIVersion = engineapi.CurrentInstanceManagerAPIVersion
	}

	if isDeleting {
		now := metav1.NewTime(time.Now())
		im.DeletionTimestamp = &now
	}
	return im
}

func newClusterRole(name string, rules []rbacv1.PolicyRule) *rbacv1.ClusterRole {
	return &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Rules: rules,
	}
}

func newClusterRoleBinding(name string, subjects []rbacv1.Subject, roleRef rbacv1.RoleRef) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Subjects: subjects,
		RoleRef:  roleRef,
	}
}

func newConfigMap(name string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Data: data,
	}
}

func newCustomResourceDefinition(name string, spec apiextensionsv1.CustomResourceDefinitionSpec) *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Spec: spec,
	}
}

func newDaemonSet(name string, spec appsv1.DaemonSetSpec, labels map[string]string) *appsv1.DaemonSet {
	if labels == nil {
		types.GetBaseLabelsForSystemManagedComponent()
	}
	return &appsv1.DaemonSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			Labels:    labels,
		},
		Spec: spec,
	}
}

func newDeployment(name string, spec appsv1.DeploymentSpec) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			Labels:    types.GetBaseLabelsForSystemManagedComponent(),
		},
		Spec: spec,
	}
}

func newPodSecurityPolicy(spec policyv1beta1.PodSecurityPolicySpec) *policyv1beta1.PodSecurityPolicy {
	return &policyv1beta1.PodSecurityPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name: TestPodSecurityPolicyName,
		},
		Spec: spec,
	}
}

func newRecurringJob(name string, spec longhorn.RecurringJobSpec) *longhorn.RecurringJob {
	return &longhorn.RecurringJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Spec: spec,
	}
}

func newRole(name string, rules []rbacv1.PolicyRule) *rbacv1.Role {
	return &rbacv1.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Rules: rules,
	}
}

func newRoleBinding(name string, subjects []rbacv1.Subject) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Subjects: subjects,
	}
}

func newSystemRestore(name, currentOwnerID string, state longhorn.SystemRestoreState) *longhorn.SystemRestore {
	return &longhorn.SystemRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Spec: longhorn.SystemRestoreSpec{
			SystemBackup: TestSystemBackupName,
		},
		Status: longhorn.SystemRestoreStatus{
			OwnerID:   currentOwnerID,
			State:     state,
			SourceURL: "",
		},
	}
}

func newSystemBackup(name, currentOwnerID, longhornVersion string,
	volumeBackupPolicy longhorn.SystemBackupCreateVolumeBackupPolicy,
	state longhorn.SystemBackupState) *longhorn.SystemBackup {
	return &longhorn.SystemBackup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			Labels: map[string]string{
				types.GetVersionLabelKey(): longhornVersion,
			},
		},
		Spec: longhorn.SystemBackupSpec{
			VolumeBackupPolicy: volumeBackupPolicy,
		},
		Status: longhorn.SystemBackupStatus{
			OwnerID: currentOwnerID,
			State:   state,
			Version: TestSystemBackupLonghornVersion,
		},
	}
}

func newService(name string, ports []corev1.ServicePort) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
		Spec: corev1.ServiceSpec{
			Ports: ports,
		},
	}
}

func newServiceAccount(name string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
	}
}

func newStorageClass(name, provisioner string) *storagev1.StorageClass {
	if provisioner == "" {
		provisioner = types.LonghornDriverName
	}
	return &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Provisioner: provisioner,
	}
}

func newBackup(name string) *longhorn.Backup {
	return &longhorn.Backup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
		},
	}
}

func newKubernetesNode(name string, readyStatus, diskPressureStatus, memoryStatus, pidStatus, networkStatus, kubeletStatus corev1.ConditionStatus) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Status: corev1.NodeStatus{
			Conditions: []corev1.NodeCondition{
				{
					Type:   corev1.NodeReady,
					Status: readyStatus,
				},
				{
					Type:   corev1.NodeDiskPressure,
					Status: diskPressureStatus,
				},
				{
					Type:   corev1.NodeMemoryPressure,
					Status: memoryStatus,
				},
				{
					Type:   corev1.NodePIDPressure,
					Status: pidStatus,
				},
				{
					Type:   corev1.NodeNetworkUnavailable,
					Status: networkStatus,
				},
			},
		},
	}
}

func newNode(name, namespace string, allowScheduling bool, status longhorn.ConditionStatus, reason string) *longhorn.Node {
	return &longhorn.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: longhorn.NodeSpec{
			AllowScheduling: allowScheduling,
			Disks: map[string]longhorn.DiskSpec{
				TestDiskID1: {
					Type:            longhorn.DiskTypeFilesystem,
					Path:            TestDefaultDataPath,
					DiskDriver:          longhorn.DiskDriverNone,
					AllowScheduling: true,
					StorageReserved: 0,
				},
			},
			Name: name,
		},
		Status: longhorn.NodeStatus{
			Conditions: []longhorn.Condition{
				newNodeCondition(longhorn.NodeConditionTypeSchedulable, status, reason),
				newNodeCondition(longhorn.NodeConditionTypeReady, status, reason),
			},
			DiskStatus: map[string]*longhorn.DiskStatus{
				TestDiskID1: {
					StorageAvailable: TestDiskAvailableSize,
					StorageScheduled: 0,
					StorageMaximum:   TestDiskSize,
					Conditions: []longhorn.Condition{
						newNodeCondition(longhorn.DiskConditionTypeSchedulable, longhorn.ConditionStatusTrue, ""),
						newNodeCondition(longhorn.DiskConditionTypeReady, longhorn.ConditionStatusTrue, ""),
					},
					DiskUUID: TestDiskID1,
					Type:     longhorn.DiskTypeFilesystem,
					FSType:   TestDiskPathFSType,
				},
			},
		},
	}
}

func newNodeCondition(conditionType string, status longhorn.ConditionStatus, reason string) longhorn.Condition {
	return longhorn.Condition{
		Type:    conditionType,
		Status:  status,
		Reason:  reason,
		Message: "",
	}
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
	c.Assert(err, IsNil)
	a = &corev1.ResourceRequirements{}
	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, false)

	b.Requests[corev1.ResourceCPU], err = resource.ParseQuantity("250m")
	c.Assert(err, IsNil)
	a.Requests = corev1.ResourceList{}
	a.Requests[corev1.ResourceCPU], _ = resource.ParseQuantity("0.25")
	c.Assert(err, IsNil)
	c.Assert(IsSameGuaranteedCPURequirement(a, b), Equals, true)
}
