package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apiextensionsinformers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhornapis "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"

	. "gopkg.in/check.v1"
)

type SystemRolloutTestCase struct {
	state longhorn.SystemRestoreState

	isInProgress      bool
	systemRestoreName string
	restoreErrors     []string

	backupClusterRoles           map[SystemRolloutCRName]*rbacv1.ClusterRole
	backupClusterRoleBindings    map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding
	backupConfigMaps             map[SystemRolloutCRName]*corev1.ConfigMap
	backupCRDVersions            map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition
	backupDaemonSets             map[SystemRolloutCRName]*appsv1.DaemonSet
	backupDeployments            map[SystemRolloutCRName]*appsv1.Deployment
	backupEngineImages           map[SystemRolloutCRName]*longhorn.EngineImage
	backupPersistentVolumes      map[SystemRolloutCRName]*corev1.PersistentVolume
	backupPersistentVolumeClaims map[SystemRolloutCRName]*corev1.PersistentVolumeClaim
	backupRecurringJobs          map[SystemRolloutCRName]*longhorn.RecurringJob
	backupRoles                  map[SystemRolloutCRName]*rbacv1.Role
	backupRoleBindings           map[SystemRolloutCRName]*rbacv1.RoleBinding
	backupServices               map[SystemRolloutCRName]*corev1.Service
	backupServiceAccounts        map[SystemRolloutCRName]*corev1.ServiceAccount
	backupSettings               map[SystemRolloutCRName]*longhorn.Setting
	backupStorageClasses         map[SystemRolloutCRName]*storagev1.StorageClass
	backupVolumes                map[SystemRolloutCRName]*longhorn.Volume
	backupBackingImages          map[SystemRolloutCRName]*longhorn.BackingImage

	existClusterRoles           map[SystemRolloutCRName]*rbacv1.ClusterRole
	existClusterRoleBindings    map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding
	existConfigMaps             map[SystemRolloutCRName]*corev1.ConfigMap
	existCRDVersions            map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition
	existDaemonSets             map[SystemRolloutCRName]*appsv1.DaemonSet
	existDeployments            map[SystemRolloutCRName]*appsv1.Deployment
	existEngineImages           map[SystemRolloutCRName]*longhorn.EngineImage
	existPersistentVolumes      map[SystemRolloutCRName]*corev1.PersistentVolume
	existPersistentVolumeClaims map[SystemRolloutCRName]*corev1.PersistentVolumeClaim
	existRecurringJobs          map[SystemRolloutCRName]*longhorn.RecurringJob
	existRoles                  map[SystemRolloutCRName]*rbacv1.Role
	existRoleBindings           map[SystemRolloutCRName]*rbacv1.RoleBinding
	existServices               map[SystemRolloutCRName]*corev1.Service
	existServiceAccounts        map[SystemRolloutCRName]*corev1.ServiceAccount
	existSettings               map[SystemRolloutCRName]*longhorn.Setting
	existStorageClasses         map[SystemRolloutCRName]*storagev1.StorageClass
	existVolumes                map[SystemRolloutCRName]*longhorn.Volume
	existBackingImages          map[SystemRolloutCRName]*longhorn.BackingImage

	expectRestoredClusterRoles           map[SystemRolloutCRName]*rbacv1.ClusterRole
	expectRestoredClusterRoleBindings    map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding
	expectRestoredConfigMaps             map[SystemRolloutCRName]*corev1.ConfigMap
	expectRestoredCRDVersions            map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition
	expectRestoredDaemonSets             map[SystemRolloutCRName]*appsv1.DaemonSet
	expectRestoredDeployments            map[SystemRolloutCRName]*appsv1.Deployment
	expectRestoredEngineImages           map[SystemRolloutCRName]*longhorn.EngineImage
	expectRestoredPersistentVolumes      map[SystemRolloutCRName]*corev1.PersistentVolume
	expectRestoredPersistentVolumeClaims map[SystemRolloutCRName]*corev1.PersistentVolumeClaim
	expectRestoredRecurringJobs          map[SystemRolloutCRName]*longhorn.RecurringJob
	expectRestoredRoles                  map[SystemRolloutCRName]*rbacv1.Role
	expectRestoredRoleBindings           map[SystemRolloutCRName]*rbacv1.RoleBinding
	expectRestoredServices               map[SystemRolloutCRName]*corev1.Service
	expectRestoredServiceAccounts        map[SystemRolloutCRName]*corev1.ServiceAccount
	expectRestoredSettings               map[SystemRolloutCRName]*longhorn.Setting
	expectRestoredStorageClasses         map[SystemRolloutCRName]*storagev1.StorageClass
	expectRestoredVolumes                map[SystemRolloutCRName]*longhorn.Volume
	expectRestoredBackingImages          map[SystemRolloutCRName]*longhorn.BackingImage

	expectError                 string
	expectErrorConditionMessage string
	expectState                 longhorn.SystemRestoreState
}

func (s *TestSuite) TestSystemRollout(c *C) {
	datastore.SystemRestoreTimeout = 10 * time.Second

	controllerID := TestNode1
	systemRolloutOwnerID := controllerID
	testStorageClassName := TestStorageClassName

	request100Mi, err := resource.ParseQuantity("100Mi")
	c.Assert(err, IsNil)
	request200Mi, err := resource.ParseQuantity("200Mi")
	c.Assert(err, IsNil)

	tempDirs := []string{}
	defer func() {
		for _, dir := range tempDirs {
			err := os.RemoveAll(dir)
			c.Assert(err, IsNil)
		}
	}()

	doneChs := []chan struct{}{}
	defer func() {
		for _, doneCh := range doneChs {
			close(doneCh)
		}
	}()

	testCases := map[string]SystemRolloutTestCase{
		"system rollout state pending": {
			state:       longhorn.SystemRestoreStatePending,
			expectState: longhorn.SystemRestoreStateDownloading,
		},
		"system rollout state download": {
			state:        longhorn.SystemRestoreStateDownloading,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateUnpacking,
		},
		"system rollout state unpack": {
			state:        longhorn.SystemRestoreStateUnpacking,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateRestoring,
		},
		"system rollout state unpack failed": {
			state:                       longhorn.SystemRestoreStateUnpacking,
			isInProgress:                true,
			expectState:                 longhorn.SystemRestoreStateError,
			expectErrorConditionMessage: longhorn.SystemRestoreConditionMessageUnpackFailed,
		},
		"system rollout state restore": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,
		},
		"system rollout state restore multiple failures": {
			systemRestoreName:           TestSystemRestoreNameRestoreMultipleFailures,
			state:                       longhorn.SystemRestoreStateRestoring,
			isInProgress:                true,
			expectState:                 longhorn.SystemRestoreStateError,
			expectErrorConditionMessage: longhorn.SystemRestoreConditionMessageFailed,
		},
		"system rollout ClusterRole exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,
			existClusterRoles: map[SystemRolloutCRName]*rbacv1.ClusterRole{
				SystemRolloutCRName(TestClusterRoleName): {
					Rules: []rbacv1.PolicyRule{
						{
							APIGroups: []string{"test.io"},
							Resources: []string{"volumes"},
							Verbs:     []string{"list"},
						},
					},
				},
			},
			expectRestoredClusterRoles: map[SystemRolloutCRName]*rbacv1.ClusterRole{
				SystemRolloutCRName(TestClusterRoleName): {
					Rules: []rbacv1.PolicyRule{
						{
							APIGroups: []string{"test.io"},
							Resources: []string{"engine"},
							Verbs:     []string{"get"},
						},
					},
				},
			},
		},
		"system rollout ClusterRole not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existClusterRoles: map[SystemRolloutCRName]*rbacv1.ClusterRole{},
			expectRestoredClusterRoles: map[SystemRolloutCRName]*rbacv1.ClusterRole{
				SystemRolloutCRName(TestClusterRoleName): {
					Rules: []rbacv1.PolicyRule{
						{
							APIGroups: []string{"test.io"},
							Resources: []string{"engine"},
							Verbs:     []string{"get"},
						},
					},
				},
			},
		},
		"system rollout ClusterRoleBinding exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,
			existClusterRoleBindings: map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding{
				SystemRolloutCRName(TestClusterRoleBindingName): {
					Subjects: []rbacv1.Subject{
						{
							Kind:      types.KubernetesKindServiceAccount,
							Name:      TestServiceAccount,
							Namespace: TestNamespace,
						},
					},
					RoleRef: rbacv1.RoleRef{Name: TestClusterRoleName},
				},
			},
			expectRestoredClusterRoleBindings: map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding{
				SystemRolloutCRName(TestClusterRoleBindingName): {
					Subjects: []rbacv1.Subject{
						{
							Kind:      types.KubernetesKindServiceAccount,
							Name:      TestServiceAccount + TestDiffSuffix,
							Namespace: TestNamespace,
						},
					},
					RoleRef: rbacv1.RoleRef{Name: TestClusterRoleName},
				},
			},
			expectRestoredServiceAccounts: map[SystemRolloutCRName]*corev1.ServiceAccount{
				SystemRolloutCRName(TestServiceAccount + TestDiffSuffix): {
					ObjectMeta: metav1.ObjectMeta{
						Name:      TestServiceAccount + TestDiffSuffix,
						Namespace: TestNamespace,
					}},
				SystemRolloutCRName(TestServiceAccount): {
					ObjectMeta: metav1.ObjectMeta{
						Name:      TestServiceAccount,
						Namespace: TestNamespace,
					}},
			},
		},
		"system rollout ClusterRoleBinding not exist in cluster": {
			state:                    longhorn.SystemRestoreStateRestoring,
			isInProgress:             true,
			expectState:              longhorn.SystemRestoreStateCompleted,
			existClusterRoleBindings: map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding{},
			expectRestoredClusterRoleBindings: map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding{
				SystemRolloutCRName(TestClusterRoleBindingName + TestDiffSuffix): {
					Subjects: []rbacv1.Subject{
						{
							Kind:      types.KubernetesKindServiceAccount,
							Name:      TestServiceAccount + TestDiffSuffix,
							Namespace: TestNamespace,
						},
					},
					RoleRef: rbacv1.RoleRef{Name: TestClusterRoleName},
				},
			},
			expectRestoredServiceAccounts: map[SystemRolloutCRName]*corev1.ServiceAccount{
				SystemRolloutCRName(TestServiceAccount + TestDiffSuffix): {
					ObjectMeta: metav1.ObjectMeta{
						Name:      TestServiceAccount + TestDiffSuffix,
						Namespace: TestNamespace,
					}},
				SystemRolloutCRName(TestServiceAccount): {
					ObjectMeta: metav1.ObjectMeta{
						Name:      TestServiceAccount,
						Namespace: TestNamespace,
					}},
			},
		},
		"system rollout StorageClass not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existStorageClasses: map[SystemRolloutCRName]*storagev1.StorageClass{
				SystemRolloutCRName(TestStorageClassName + TestIgnoreSuffix): {},
			},
			expectRestoredStorageClasses: map[SystemRolloutCRName]*storagev1.StorageClass{
				SystemRolloutCRName(TestStorageClassName + TestDiffSuffix): {},
			},
		},
		"system rollout ConfigMap exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existConfigMaps: map[SystemRolloutCRName]*corev1.ConfigMap{
				SystemRolloutCRName(types.DefaultStorageClassConfigMapName): {
					Data: map[string]string{
						"test": "data",
					},
				},
			},
			expectRestoredConfigMaps: map[SystemRolloutCRName]*corev1.ConfigMap{
				SystemRolloutCRName(types.DefaultStorageClassConfigMapName): {
					Data: map[string]string{
						"test": "data" + TestDiffSuffix,
					},
				},
			},
		},
		"system rollout ConfigMap not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existConfigMaps: map[SystemRolloutCRName]*corev1.ConfigMap{
				SystemRolloutCRName(types.DefaultStorageClassConfigMapName + TestIgnoreSuffix): {},
			},
			expectRestoredConfigMaps: map[SystemRolloutCRName]*corev1.ConfigMap{
				SystemRolloutCRName(types.DefaultStorageClassConfigMapName): {
					Data: map[string]string{
						"test": "data",
					},
				},
			},
		},
		"system rollout CustomResourceDefinition version exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existCRDVersions: map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition{
				SystemRolloutCRName(types.DefaultStorageClassConfigMapName): {
					Spec: apiextensionsv1.CustomResourceDefinitionSpec{
						Group: longhornapis.GroupName,
						Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
							{Name: "v1beta1"},
						},
					},
				},
			},
			expectRestoredCRDVersions: map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition{
				SystemRolloutCRName(types.DefaultStorageClassConfigMapName): {
					Spec: apiextensionsv1.CustomResourceDefinitionSpec{
						Group: longhornapis.GroupName,
						Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
							{Name: "v1beta1"},
							{Name: "v1beta2"},
						},
					},
				},
			},
		},
		"system rollout CustomResourceDefinition version not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existCRDVersions: nil,
			expectRestoredCRDVersions: map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition{
				SystemRolloutCRName(types.DefaultStorageClassConfigMapName): {
					Spec: apiextensionsv1.CustomResourceDefinitionSpec{
						Group: longhornapis.GroupName,
						Versions: []apiextensionsv1.CustomResourceDefinitionVersion{
							{Name: "v1beta1"},
							{Name: "v1beta2"},
						},
					},
				},
			},
		},
		"system rollout DaemonSet exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existDaemonSets: map[SystemRolloutCRName]*appsv1.DaemonSet{
				SystemRolloutCRName(TestDaemon1): {
					Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name: TestDaemon1,
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  TestDaemon1,
									Image: TestManagerImage,
								},
							},
						},
					}},
				},
			},
			expectRestoredDaemonSets: map[SystemRolloutCRName]*appsv1.DaemonSet{
				SystemRolloutCRName(TestDaemon1): {
					Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name: TestDaemon1,
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  TestDaemon1,
									Image: TestManagerImage + TestDiffSuffix,
								},
							},
						},
					}},
				},
			},
		},
		"system rollout DaemonSet not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existDaemonSets: nil,
			expectRestoredDaemonSets: map[SystemRolloutCRName]*appsv1.DaemonSet{
				SystemRolloutCRName(TestDaemon1): {Spec: appsv1.DaemonSetSpec{
					Template: corev1.PodTemplateSpec{
						ObjectMeta: metav1.ObjectMeta{
							Name: TestDaemon1,
						},
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  TestDaemon1,
									Image: TestManagerImage,
								},
							},
						},
					},
				}},
			},
		},
		"system rollout Deployment exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existDeployments: map[SystemRolloutCRName]*appsv1.Deployment{
				SystemRolloutCRName(TestDaemon1): {Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  TestDeploymentName,
									Image: TestInstanceManagerImage,
								},
							},
						},
					},
				}},
			},
			expectRestoredDeployments: map[SystemRolloutCRName]*appsv1.Deployment{
				SystemRolloutCRName(TestDaemon1): {Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  TestDeploymentName + TestDiffSuffix,
									Image: TestInstanceManagerImage,
								},
							},
						},
					},
				}},
			},
		},
		"system rollout Deployment not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existDeployments: nil,
			expectRestoredDeployments: map[SystemRolloutCRName]*appsv1.Deployment{
				SystemRolloutCRName(TestDaemon1): {Spec: appsv1.DeploymentSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  TestDeploymentName,
									Image: TestInstanceManagerImage,
								},
							},
						},
					},
				}},
			},
		},
		"system rollout EngineImage exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existEngineImages: map[SystemRolloutCRName]*longhorn.EngineImage{
				SystemRolloutCRName(TestEngineImageName + TestDiffSuffix): {
					Spec: longhorn.EngineImageSpec{
						Image: TestEngineImage + TestDiffSuffix + TestDiffSuffix,
					},
					Status: longhorn.EngineImageStatus{
						State: longhorn.EngineImageStateDeployed,
					},
				},
			},
			expectRestoredEngineImages: map[SystemRolloutCRName]*longhorn.EngineImage{
				SystemRolloutCRName(TestEngineImageName + TestDiffSuffix): {
					Spec: longhorn.EngineImageSpec{
						Image: TestEngineImage + TestDiffSuffix,
					},
					Status: longhorn.EngineImageStatus{
						State: longhorn.EngineImageStateDeployed,
					},
				},
			},
		},
		"system rollout EngineImage not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existEngineImages: nil,
			expectRestoredEngineImages: map[SystemRolloutCRName]*longhorn.EngineImage{
				SystemRolloutCRName(TestEngineImageName + TestDiffSuffix): {
					Spec: longhorn.EngineImageSpec{
						Image: TestEngineImage + TestDiffSuffix,
					},
					Status: longhorn.EngineImageStatus{
						State: longhorn.EngineImageStateDeployed,
					},
				},
			},
		},
		"system rollout PersistentVolume exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existPersistentVolumes: map[SystemRolloutCRName]*corev1.PersistentVolume{
				SystemRolloutCRName(TestPVName): {
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      TestPVCName,
							Namespace: TestNamespace,
						},
						StorageClassName:       TestStorageClassName,
						PersistentVolumeSource: newPVSourceCSI(),
					},
				},
			},
			backupPersistentVolumes: map[SystemRolloutCRName]*corev1.PersistentVolume{
				SystemRolloutCRName(TestPVName): {
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      TestPVCName + TestDiffSuffix,
							Namespace: TestNamespace,
						},
						StorageClassName:       TestStorageClassName,
						PersistentVolumeSource: newPVSourceCSI(),
					},
				},
			},
			expectRestoredPersistentVolumes: map[SystemRolloutCRName]*corev1.PersistentVolume{
				SystemRolloutCRName(TestPVName): {
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      TestPVCName,
							Namespace: TestNamespace,
						},
						StorageClassName:       TestStorageClassName,
						PersistentVolumeSource: newPVSourceCSI(),
					},
				},
			},
		},
		"system rollout PersistentVolume not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existPersistentVolumes: nil,
			expectRestoredPersistentVolumes: map[SystemRolloutCRName]*corev1.PersistentVolume{
				SystemRolloutCRName(TestPVName): {
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      TestPVCName,
							Namespace: TestNamespace,
						},
						StorageClassName:       TestStorageClassName,
						PersistentVolumeSource: newPVSourceCSI(),
					},
				},
			},
		},
		"system rollout PersistentVolumeClaim exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existPersistentVolumeClaims: map[SystemRolloutCRName]*corev1.PersistentVolumeClaim{
				SystemRolloutCRName(TestPVCName): {
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: &testStorageClassName,
						VolumeName:       TestVolumeName,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: request100Mi,
							},
						},
					},
				},
			},
			backupPersistentVolumeClaims: map[SystemRolloutCRName]*corev1.PersistentVolumeClaim{
				SystemRolloutCRName(TestPVCName): {
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: &testStorageClassName,
						VolumeName:       TestVolumeName,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: request200Mi,
							},
						},
					},
				},
			},
			expectRestoredPersistentVolumeClaims: map[SystemRolloutCRName]*corev1.PersistentVolumeClaim{
				SystemRolloutCRName(TestPVCName): {
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: &testStorageClassName,
						VolumeName:       TestVolumeName,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: request100Mi,
							},
						},
					},
				},
			},
		},
		"system rollout PersistentVolumeClaim not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existPersistentVolumeClaims: nil,
			expectRestoredPersistentVolumeClaims: map[SystemRolloutCRName]*corev1.PersistentVolumeClaim{
				SystemRolloutCRName(TestPVCName): {
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: &testStorageClassName,
						VolumeName:       TestVolumeName,
						Resources: corev1.VolumeResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: request100Mi,
							},
						},
					},
				},
			},
		},
		"system rollout RecurringJobs exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existRecurringJobs: map[SystemRolloutCRName]*longhorn.RecurringJob{
				SystemRolloutCRName(TestRecurringJobName): {
					Spec: longhorn.RecurringJobSpec{
						Name:   TestRecurringJobName,
						Groups: []string{TestRecurringJobGroupName},
					},
				},
			},
			expectRestoredRecurringJobs: map[SystemRolloutCRName]*longhorn.RecurringJob{
				SystemRolloutCRName(TestRecurringJobName): {
					Spec: longhorn.RecurringJobSpec{
						Name:   TestRecurringJobName,
						Groups: []string{TestRecurringJobGroupName + TestDiffSuffix},
					},
				},
			},
		},
		"system rollout RecurringJobs not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existRecurringJobs: nil,
			expectRestoredRecurringJobs: map[SystemRolloutCRName]*longhorn.RecurringJob{
				SystemRolloutCRName(TestRecurringJobName): {
					Spec: longhorn.RecurringJobSpec{
						Name: TestRecurringJobName,
					},
				},
			},
		},
		"system rollout Roles exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existRoles: map[SystemRolloutCRName]*rbacv1.Role{
				SystemRolloutCRName(TestRoleName): {
					Rules: []rbacv1.PolicyRule{
						{ResourceNames: []string{TestPodSecurityPolicyName}},
					},
				},
			},
			expectRestoredRoles: map[SystemRolloutCRName]*rbacv1.Role{
				SystemRolloutCRName(TestRoleName): {
					Rules: []rbacv1.PolicyRule{
						{ResourceNames: []string{TestPodSecurityPolicyName + TestDiffSuffix}},
					},
				},
			},
		},
		"system rollout Roles not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existRoles: nil,
			expectRestoredRoles: map[SystemRolloutCRName]*rbacv1.Role{
				SystemRolloutCRName(TestRoleName): {
					Rules: []rbacv1.PolicyRule{
						{ResourceNames: []string{TestPodSecurityPolicyName}},
					},
				},
			},
		},
		"system rollout RoleBindings exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existRoleBindings: map[SystemRolloutCRName]*rbacv1.RoleBinding{
				SystemRolloutCRName(TestRoleBindingName): {
					Subjects: []rbacv1.Subject{{
						Kind:      types.KubernetesKindServiceAccount,
						Name:      TestServiceAccount,
						Namespace: TestNamespace,
					}},
				},
			},
			expectRestoredRoleBindings: map[SystemRolloutCRName]*rbacv1.RoleBinding{
				SystemRolloutCRName(TestRoleBindingName): {
					Subjects: []rbacv1.Subject{{
						Kind:      types.KubernetesKindServiceAccount,
						Name:      TestServiceAccount + TestDiffSuffix,
						Namespace: TestNamespace,
					}},
				},
			},
		},
		"system rollout RoleBindings not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existRoleBindings: nil,
			expectRestoredRoleBindings: map[SystemRolloutCRName]*rbacv1.RoleBinding{
				SystemRolloutCRName(TestRoleBindingName): {
					Subjects: []rbacv1.Subject{
						{
							Kind:      types.KubernetesKindServiceAccount,
							Name:      TestServiceAccount,
							Namespace: TestNamespace,
						}},
				},
			},
		},
		"system rollout ServiceAccount not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existServiceAccounts: map[SystemRolloutCRName]*corev1.ServiceAccount{},
			expectRestoredServiceAccounts: map[SystemRolloutCRName]*corev1.ServiceAccount{
				SystemRolloutCRName(types.SettingNameDefaultReplicaCount): {ObjectMeta: metav1.ObjectMeta{
					Name: TestServiceAccount,
				}},
			},
		},
		"system rollout Setting exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existSettings: map[SystemRolloutCRName]*longhorn.Setting{
				SystemRolloutCRName(types.SettingNameDefaultReplicaCount): {Value: "2"},
			},
			expectRestoredSettings: map[SystemRolloutCRName]*longhorn.Setting{
				SystemRolloutCRName(types.SettingNameDefaultReplicaCount): {Value: "3"},
			},
		},
		"system rollout Setting not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existSettings: map[SystemRolloutCRName]*longhorn.Setting{},
			expectRestoredSettings: map[SystemRolloutCRName]*longhorn.Setting{
				SystemRolloutCRName(types.SettingNameDefaultReplicaCount): {Value: "3"},
			},
		},
		"system rollout Settings ignored:": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existSettings: map[SystemRolloutCRName]*longhorn.Setting{
				SystemRolloutCRName(types.SettingNameConcurrentReplicaRebuildPerNodeLimit):        {Value: "4"},
				SystemRolloutCRName(types.SettingNameConcurrentBackupRestorePerNodeLimit):         {Value: "5"},
				SystemRolloutCRName(types.SettingNameConcurrentBackingImageReplenishPerNodeLimit): {Value: "5"},
			},
			backupSettings: map[SystemRolloutCRName]*longhorn.Setting{
				SystemRolloutCRName(types.SettingNameConcurrentReplicaRebuildPerNodeLimit):        {Value: "6"},
				SystemRolloutCRName(types.SettingNameConcurrentBackupRestorePerNodeLimit):         {Value: "7"},
				SystemRolloutCRName(types.SettingNameConcurrentBackingImageReplenishPerNodeLimit): {Value: "8"},
			},
			expectRestoredSettings: map[SystemRolloutCRName]*longhorn.Setting{
				SystemRolloutCRName(types.SettingNameConcurrentReplicaRebuildPerNodeLimit):        {Value: "4"},
				SystemRolloutCRName(types.SettingNameConcurrentBackupRestorePerNodeLimit):         {Value: "5"},
				SystemRolloutCRName(types.SettingNameConcurrentBackingImageReplenishPerNodeLimit): {Value: "5"},
			},
		},
		"system rollout Volume exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Spec: longhorn.VolumeSpec{
						NumberOfReplicas: 3,
					},
				},
			},
			backupVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Spec: longhorn.VolumeSpec{
						NumberOfReplicas: 5,
					},
				},
			},
			expectRestoredVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Spec: longhorn.VolumeSpec{
						NumberOfReplicas: 3,
					},
				},
			},
		},
		"system rollout Volume not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existVolumes: nil,
			expectRestoredVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Spec: longhorn.VolumeSpec{
						NumberOfReplicas: 3,
					},
				},
			},
		},
		"system rollout BackingImage exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existBackingImages: map[SystemRolloutCRName]*longhorn.BackingImage{
				SystemRolloutCRName(TestBackingImage): {
					Spec: longhorn.BackingImageSpec{
						SourceType: longhorn.BackingImageDataSourceTypeDownload,
					},
				},
			},
			backupBackingImages: map[SystemRolloutCRName]*longhorn.BackingImage{
				SystemRolloutCRName(TestBackingImage): {
					Spec: longhorn.BackingImageSpec{
						SourceType: longhorn.BackingImageDataSourceTypeUpload,
					},
				},
			},
			expectRestoredBackingImages: map[SystemRolloutCRName]*longhorn.BackingImage{
				SystemRolloutCRName(TestBackingImage): {
					Spec: longhorn.BackingImageSpec{
						SourceType: longhorn.BackingImageDataSourceTypeDownload,
					},
				},
			},
		},
		"system rollout BackingImage not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existBackingImages: nil,
			// The original sourceType is upload,
			// but when restoring, we recreate the BackingImage with type restore.
			backupBackingImages: map[SystemRolloutCRName]*longhorn.BackingImage{
				SystemRolloutCRName(TestBackingImage): {
					Spec: longhorn.BackingImageSpec{
						SourceType: longhorn.BackingImageDataSourceTypeUpload,
					},
				},
			},
			expectRestoredBackingImages: map[SystemRolloutCRName]*longhorn.BackingImage{
				SystemRolloutCRName(TestBackingImage): {
					Spec: longhorn.BackingImageSpec{
						SourceType: longhorn.BackingImageDataSourceTypeRestore,
					},
				},
			},
		},

		"system rollout Service exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existServices: map[SystemRolloutCRName]*corev1.Service{
				SystemRolloutCRName(TestServiceName): {
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{
								Name: TestServicePortName + TestDiffSuffix,
								Port: 123,
							},
							{
								Name: TestServicePortName,
								Port: 456,
							},
						},
					},
				},
			},
			expectRestoredServices: map[SystemRolloutCRName]*corev1.Service{
				SystemRolloutCRName(TestServiceName): {
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{
								Name: TestServicePortName,
								Port: 123,
							},
						},
					},
				},
			},
		},
		"system rollout Service not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existServices: nil,
			expectRestoredServices: map[SystemRolloutCRName]*corev1.Service{
				SystemRolloutCRName(TestServiceName): {
					Spec: corev1.ServiceSpec{
						Ports: []corev1.ServicePort{
							{
								Name: TestServicePortName,
								Port: 123,
							},
						},
					},
				},
			},
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		tc.initTestCase()

		kubeClient := fake.NewSimpleClientset()
		lhClient := lhfake.NewSimpleClientset()
		extensionsClient := apiextensionsfake.NewSimpleClientset()

		extensionsInformerFactory := apiextensionsinformers.NewSharedInformerFactory(extensionsClient, controller.NoResyncPeriodFunc())

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		fakeSystemRolloutBackupTargetDefault(c, informerFactories.LhInformerFactory, lhClient)

		fakeSystemRolloutSettings(tc.backupSettings, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutCustomResourceDefinitions(tc.backupCRDVersions, c, extensionsInformerFactory, extensionsClient)
		fakeSystemRolloutClusterRoles(tc.backupClusterRoles, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutClusterRoleBindings(tc.backupClusterRoleBindings, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutConfigMaps(tc.backupConfigMaps, c, informerFactories.KubeNamespaceFilteredInformerFactory, kubeClient)
		fakeSystemRolloutDaemonSets(tc.backupDaemonSets, c, informerFactories.KubeNamespaceFilteredInformerFactory, kubeClient)
		fakeSystemRolloutDeployments(tc.backupDeployments, c, informerFactories.KubeNamespaceFilteredInformerFactory, kubeClient)
		fakeSystemRolloutEngineImages(tc.backupEngineImages, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutPersistentVolumes(tc.backupPersistentVolumes, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutPersistentVolumeClaims(tc.backupPersistentVolumeClaims, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutRecurringJobs(tc.backupRecurringJobs, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutRoles(tc.backupRoles, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutRoleBindings(tc.backupRoleBindings, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutServices(tc.backupServices, c, informerFactories.KubeNamespaceFilteredInformerFactory, kubeClient)
		fakeSystemRolloutServiceAccounts(tc.backupServiceAccounts, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutStorageClasses(tc.backupStorageClasses, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutVolumes(tc.backupVolumes, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutBackingImages(tc.backupBackingImages, c, informerFactories.LhInformerFactory, lhClient)

		ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
		doneCh := make(chan struct{})
		if tc.expectState != longhorn.SystemRestoreStateCompleted && tc.expectState != longhorn.SystemRestoreStateError {
			doneChs = append(doneChs, doneCh)
		}

		controller, err := newFakeSystemRolloutController(tc.systemRestoreName, controllerID, ds, doneCh, kubeClient, extensionsClient)
		c.Assert(err, IsNil)
		controller.systemRestoreVersion = TestSystemBackupLonghornVersion
		controller.cacheErrors = util.MultiError{}

		fakeSystemRestore(tc.systemRestoreName, systemRolloutOwnerID, tc.isInProgress, false, tc.state, c, informerFactories.LhInformerFactory, lhClient, controller.ds)

		controller.systemRestore, err = lhClient.LonghornV1beta2().SystemRestores(TestNamespace).Get(context.TODO(), tc.systemRestoreName, metav1.GetOptions{})
		c.Assert(err, IsNil)

		rolloutTempDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("*-%v", TestSystemRestoreName))
		c.Assert(err, IsNil)
		tempDirs = append(tempDirs, rolloutTempDir)

		controller.downloadPath = filepath.Join(rolloutTempDir, TestSystemBackupName+types.SystemBackupExtension)
		tempDir := filepath.Join(rolloutTempDir, TestSystemBackupName)

		if tc.state != longhorn.SystemRestoreStateUnpacking || (tc.state == longhorn.SystemRestoreStateUnpacking && tc.expectState != longhorn.SystemRestoreStateError) {
			fakeSystemBackupArchieve(c, TestSystemBackupName, controllerID, systemRolloutOwnerID, tempDir, controller.downloadPath,
				kubeClient, lhClient, extensionsClient, informerFactories)
		}

		err = os.RemoveAll(tempDir)
		c.Assert(err, IsNil)

		fakeSystemRolloutClusterRoles(tc.existClusterRoles, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutClusterRoleBindings(tc.existClusterRoleBindings, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutConfigMaps(tc.existConfigMaps, c, informerFactories.KubeNamespaceFilteredInformerFactory, kubeClient)
		fakeSystemRolloutCustomResourceDefinitions(tc.existCRDVersions, c, extensionsInformerFactory, extensionsClient)
		fakeSystemRolloutDaemonSets(tc.existDaemonSets, c, informerFactories.KubeNamespaceFilteredInformerFactory, kubeClient)
		fakeSystemRolloutDeployments(tc.existDeployments, c, informerFactories.KubeNamespaceFilteredInformerFactory, kubeClient)
		fakeSystemRolloutEngineImages(tc.existEngineImages, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutPersistentVolumes(tc.existPersistentVolumes, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutPersistentVolumeClaims(tc.existPersistentVolumeClaims, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutRecurringJobs(tc.existRecurringJobs, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutRoles(tc.existRoles, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutRoleBindings(tc.existRoleBindings, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutServices(tc.existServices, c, informerFactories.KubeNamespaceFilteredInformerFactory, kubeClient)
		fakeSystemRolloutServiceAccounts(tc.existServiceAccounts, c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutSettings(tc.existSettings, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutVolumes(tc.existVolumes, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutBackingImages(tc.existBackingImages, c, informerFactories.LhInformerFactory, lhClient)

		if tc.state == longhorn.SystemRestoreStateRestoring {
			err := controller.Unpack(controller.logger)
			c.Assert(err, IsNil)
		}

		if tc.systemRestoreName == TestSystemRestoreNameRestoreMultipleFailures {
			tc.restoreErrors = []string{"err-1", "err-2"}
			controller.cacheErrors = util.NewMultiError(tc.restoreErrors...)
		}

		err = controller.systemRollout()
		if tc.expectError != "" {
			c.Assert(err, NotNil)
			c.Assert(strings.HasPrefix(err.Error(), tc.expectError), Equals, true)
			continue
		} else {
			c.Assert(err, IsNil)
		}

		systemRestore, err := lhClient.LonghornV1beta2().SystemRestores(TestNamespace).Get(context.TODO(), tc.systemRestoreName, metav1.GetOptions{})
		c.Assert(err, IsNil)
		c.Assert(systemRestore.Status.State, Equals, tc.expectState)

		if tc.expectState == longhorn.SystemRestoreStateCompleted {
			assertRolloutClusterRoles(tc.expectRestoredClusterRoles, c, kubeClient)
			assertRolloutClusterRoleBindings(tc.expectRestoredClusterRoleBindings, c, kubeClient)
			assertRolloutCustomResourceDefinition(tc.expectRestoredCRDVersions, c, extensionsClient)
			assertRolloutConfigMaps(tc.expectRestoredConfigMaps, c, kubeClient)
			assertRolloutDaemonSets(tc.expectRestoredDaemonSets, c, kubeClient)
			assertRolloutDeployments(tc.expectRestoredDeployments, c, kubeClient)
			assertRolloutEngineImages(tc.expectRestoredEngineImages, c, lhClient)
			assertRolloutPersistentVolumes(tc.expectRestoredPersistentVolumes, tc.backupPersistentVolumes, c, kubeClient)
			assertRolloutPersistentVolumeClaims(tc.expectRestoredPersistentVolumeClaims, tc.backupPersistentVolumeClaims, c, kubeClient)
			assertRolloutRecurringJobs(tc.expectRestoredRecurringJobs, c, lhClient)
			assertRolloutRoles(tc.expectRestoredRoles, c, kubeClient)
			assertRolloutRoleBindings(tc.expectRestoredRoleBindings, c, kubeClient)
			assertRolloutServices(tc.expectRestoredServices, c, kubeClient)
			assertRolloutServiceAccounts(tc.expectRestoredServiceAccounts, c, kubeClient)
			assertRolloutVolumes(tc.expectRestoredVolumes, tc.backupVolumes, c, lhClient)
			assertRolloutBackingImages(tc.expectRestoredBackingImages, tc.backupBackingImages, c, lhClient)
			assertRolloutSettings(tc.expectRestoredSettings, tc.existSettings, c, lhClient)
		}

		if tc.expectState == longhorn.SystemRestoreStateError {
			errCondition := types.GetCondition(systemRestore.Status.Conditions, longhorn.SystemRestoreConditionTypeError)
			c.Assert(errCondition.Status, Equals, longhorn.ConditionStatusTrue)
			errMessageSplit := strings.Split(errCondition.Message, ":")
			if len(tc.restoreErrors) != 0 {
				errSplit := strings.Split(errMessageSplit[1], ";")
				c.Assert(len(errSplit), Equals, len(tc.restoreErrors))
			}
			errConditionError := strings.Join(errMessageSplit[1:], ":")
			c.Assert(errCondition.Message, Equals, fmt.Sprintf("%v:%v", tc.expectErrorConditionMessage, errConditionError))
		}
	}
}

func newFakeSystemRolloutController(
	systemRestoreName, controllerID string,
	ds *datastore.DataStore,
	stopCh chan struct{},
	kubeClient *fake.Clientset,
	extensionsClient *apiextensionsfake.Clientset) (*SystemRolloutController, error) {
	logger := logrus.StandardLogger()
	logrus.SetLevel(logrus.DebugLevel)

	c, err := NewSystemRolloutController(systemRestoreName, logger, controllerID, ds, scheme.Scheme, stopCh, kubeClient, extensionsClient)
	if err != nil {
		return nil, err
	}
	c.eventRecorder = record.NewFakeRecorder(100)
	for index := range c.cacheSyncs {
		c.cacheSyncs[index] = alwaysReady
	}

	c.backupTargetClient = &FakeSystemBackupTargetClient{
		name:    TestSystemBackupName,
		version: TestSystemBackupLonghornVersion,
	}

	return c, nil
}

func (tc *SystemRolloutTestCase) initTestCase() {
	if tc.systemRestoreName == "" {
		tc.systemRestoreName = TestSystemRestoreName
	}

	// init ClusterRoles
	if tc.existClusterRoles == nil {
		tc.existClusterRoles = map[SystemRolloutCRName]*rbacv1.ClusterRole{
			SystemRolloutCRName(TestClusterRoleName): {
				ObjectMeta: metav1.ObjectMeta{
					Name: TestClusterRoleName,
				},
			},
		}
	}
	if tc.expectRestoredClusterRoles == nil {
		tc.expectRestoredClusterRoles = tc.existClusterRoles
	}
	if tc.backupClusterRoles == nil {
		tc.backupClusterRoles = tc.expectRestoredClusterRoles
	}

	// init ClusterRoleBindings
	if tc.existClusterRoleBindings == nil {
		tc.existClusterRoleBindings = map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding{
			SystemRolloutCRName(TestClusterRoleBindingName): {
				Subjects: []rbacv1.Subject{
					{
						Kind:      types.KubernetesKindServiceAccount,
						Name:      TestServiceAccount,
						Namespace: TestNamespace,
					},
				},
				RoleRef: rbacv1.RoleRef{Name: TestClusterRoleName},
			},
		}
	}
	if tc.expectRestoredClusterRoleBindings == nil {
		tc.expectRestoredClusterRoleBindings = tc.existClusterRoleBindings
	}
	if tc.backupClusterRoleBindings == nil {
		tc.backupClusterRoleBindings = tc.expectRestoredClusterRoleBindings
	}

	// init ConfigMaps
	if (tc.existConfigMaps) == nil {
		tc.existConfigMaps = map[SystemRolloutCRName]*corev1.ConfigMap{
			SystemRolloutCRName(types.DefaultStorageClassConfigMapName): {
				Data: map[string]string{
					"test": "data",
				},
			},
		}
	}
	if tc.expectRestoredConfigMaps == nil {
		tc.expectRestoredConfigMaps = tc.existConfigMaps
	}
	if tc.backupConfigMaps == nil {
		tc.backupConfigMaps = tc.expectRestoredConfigMaps
	}

	// init CustomResourceDefinitions
	if len(tc.expectRestoredCRDVersions) == 0 && len(tc.existCRDVersions) != 0 {
		tc.expectRestoredCRDVersions = tc.backupCRDVersions
	}
	if len(tc.backupCRDVersions) == 0 {
		tc.backupCRDVersions = tc.expectRestoredCRDVersions
	}

	// init DaemonSets
	if tc.existDaemonSets == nil {
		tc.existDaemonSets = map[SystemRolloutCRName]*appsv1.DaemonSet{}
	}
	if tc.expectRestoredDaemonSets == nil {
		tc.expectRestoredDaemonSets = tc.existDaemonSets
	}
	if tc.backupDaemonSets == nil {
		tc.backupDaemonSets = tc.expectRestoredDaemonSets
	}

	// init Deployments
	if tc.existDeployments == nil {
		tc.existDeployments = map[SystemRolloutCRName]*appsv1.Deployment{}
	}
	if tc.expectRestoredDeployments == nil {
		tc.expectRestoredDeployments = tc.existDeployments
	}
	if tc.backupDeployments == nil {
		tc.backupDeployments = tc.expectRestoredDeployments
	}

	// init EngineImages
	if tc.existEngineImages == nil {
		tc.existEngineImages = map[SystemRolloutCRName]*longhorn.EngineImage{}
	}
	if tc.expectRestoredEngineImages == nil {
		tc.expectRestoredEngineImages = tc.existEngineImages
	}
	if tc.backupEngineImages == nil {
		tc.backupEngineImages = tc.expectRestoredEngineImages
	}

	// init PersistentVolumes
	if tc.existPersistentVolumes == nil {
		tc.existPersistentVolumes = map[SystemRolloutCRName]*corev1.PersistentVolume{}
	}
	if tc.expectRestoredPersistentVolumes == nil {
		tc.expectRestoredPersistentVolumes = tc.existPersistentVolumes
	}
	if tc.backupPersistentVolumes == nil {
		tc.backupPersistentVolumes = tc.expectRestoredPersistentVolumes
	}
	for _, pvc := range tc.expectRestoredPersistentVolumeClaims {
		volumeName := SystemRolloutCRName(pvc.Spec.VolumeName)
		_, found := tc.backupPersistentVolumes[volumeName]
		if found {
			continue
		}

		tc.expectRestoredPersistentVolumes[volumeName] = &corev1.PersistentVolume{
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Name:      pvc.Name,
					Namespace: TestNamespace,
				},
				StorageClassName:       *pvc.Spec.StorageClassName,
				PersistentVolumeSource: newPVSourceCSI(),
			},
		}
		tc.backupPersistentVolumes[volumeName] = tc.expectRestoredPersistentVolumes[volumeName]
	}

	// init PersistentVolumeClaims
	if tc.existPersistentVolumeClaims == nil {
		tc.existPersistentVolumeClaims = map[SystemRolloutCRName]*corev1.PersistentVolumeClaim{}
	}
	if tc.expectRestoredPersistentVolumeClaims == nil {
		tc.expectRestoredPersistentVolumeClaims = tc.existPersistentVolumeClaims
	}
	if tc.backupPersistentVolumeClaims == nil {
		tc.backupPersistentVolumeClaims = tc.expectRestoredPersistentVolumeClaims
	}

	// init RecurringJobs
	if tc.existRecurringJobs == nil {
		tc.existRecurringJobs = map[SystemRolloutCRName]*longhorn.RecurringJob{}
	}
	if tc.expectRestoredRecurringJobs == nil {
		tc.expectRestoredRecurringJobs = tc.existRecurringJobs
	}
	if tc.backupRecurringJobs == nil {
		tc.backupRecurringJobs = tc.expectRestoredRecurringJobs
	}

	// init Roles
	if tc.existRoles == nil {
		tc.existRoles = map[SystemRolloutCRName]*rbacv1.Role{}
	}
	if tc.expectRestoredRoles == nil {
		tc.expectRestoredRoles = tc.existRoles
	}
	if tc.backupRoles == nil {
		tc.backupRoles = tc.expectRestoredRoles
	}
	addRoleRuleResources := map[string]struct{}{}
	addRoleRuleResources[TestPodSecurityPolicyName] = struct{}{}
	for _, role := range tc.expectRestoredRoles {
		for _, rule := range role.Rules {
			for _, resourceName := range rule.ResourceNames {
				delete(addRoleRuleResources, resourceName)
			}
		}
	}

	addPolicyRules := []string{}
	for roleRuleResources := range addRoleRuleResources {
		addPolicyRules = append(addPolicyRules, roleRuleResources)
	}
	if len(addPolicyRules) != 0 {
		name := TestRoleName + SystemRolloutCRName("-auto")
		policyRule := rbacv1.PolicyRule{
			ResourceNames: addPolicyRules,
		}
		tc.backupRoles[name] = newRole(string(name), []rbacv1.PolicyRule{policyRule})
		tc.expectRestoredRoles[name] = newRole(string(name), []rbacv1.PolicyRule{policyRule})
	}

	// init RoleBindings
	if tc.existRoleBindings == nil {
		tc.existRoleBindings = map[SystemRolloutCRName]*rbacv1.RoleBinding{}
	}
	if tc.expectRestoredRoleBindings == nil {
		tc.expectRestoredRoleBindings = tc.existRoleBindings
	}
	if tc.backupRoleBindings == nil {
		tc.backupRoleBindings = tc.expectRestoredRoleBindings
	}

	// init Services
	if tc.existServices == nil {
		tc.existServices = map[SystemRolloutCRName]*corev1.Service{}
	}
	if tc.expectRestoredServices == nil {
		tc.expectRestoredServices = tc.existServices
	}
	if tc.backupServices == nil {
		tc.backupServices = tc.expectRestoredServices
	}

	// init ServiceAccounts
	if tc.existServiceAccounts == nil {
		tc.existServiceAccounts = map[SystemRolloutCRName]*corev1.ServiceAccount{
			SystemRolloutCRName(TestServiceAccount): {
				ObjectMeta: metav1.ObjectMeta{
					Name:      TestServiceAccount,
					Namespace: TestNamespace,
				},
			},
		}
	}
	if tc.expectRestoredServiceAccounts == nil {
		tc.expectRestoredServiceAccounts = tc.existServiceAccounts
	}
	if tc.backupServiceAccounts == nil {
		tc.backupServiceAccounts = tc.expectRestoredServiceAccounts
	}

	// init Settings
	if tc.existSettings == nil {
		tc.existSettings = map[SystemRolloutCRName]*longhorn.Setting{}
	}
	if tc.expectRestoredSettings == nil {
		tc.expectRestoredSettings = tc.existSettings
	}
	if tc.backupSettings == nil {
		tc.backupSettings = tc.expectRestoredSettings
	}
	tc.existSettings[SystemRolloutCRName(types.SettingNameDefaultEngineImage)] = &longhorn.Setting{Value: TestEngineImage}
	tc.expectRestoredSettings[SystemRolloutCRName(types.SettingNameDefaultEngineImage)] = &longhorn.Setting{Value: TestEngineImage}
	tc.backupSettings[SystemRolloutCRName(types.SettingNameDefaultEngineImage)] = &longhorn.Setting{Value: TestEngineImage}

	// init StorageClasses
	if tc.existStorageClasses == nil {
		tc.existStorageClasses = map[SystemRolloutCRName]*storagev1.StorageClass{}
	}
	if tc.expectRestoredStorageClasses == nil {
		tc.expectRestoredStorageClasses = tc.existStorageClasses
	}
	if tc.backupStorageClasses == nil {
		tc.backupStorageClasses = tc.expectRestoredStorageClasses
	}
	tc.existStorageClasses[SystemRolloutCRName(TestStorageClassName)] = &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: TestStorageClassName,
		},
	}
	tc.expectRestoredStorageClasses[SystemRolloutCRName(TestStorageClassName)] = tc.existStorageClasses[SystemRolloutCRName(TestStorageClassName)]
	tc.backupStorageClasses[SystemRolloutCRName(TestStorageClassName)] = tc.expectRestoredStorageClasses[SystemRolloutCRName(TestStorageClassName)]

	// init Volumes
	if tc.existVolumes == nil {
		tc.existVolumes = map[SystemRolloutCRName]*longhorn.Volume{}
	}
	if tc.expectRestoredVolumes == nil {
		tc.expectRestoredVolumes = tc.existVolumes
	}
	if tc.backupVolumes == nil {
		tc.backupVolumes = tc.expectRestoredVolumes
	}
	for _, pv := range tc.expectRestoredPersistentVolumes {
		volumeName := SystemRolloutCRName(pv.Spec.CSI.VolumeHandle)
		_, found := tc.backupVolumes[volumeName]
		if found {
			continue
		}

		tc.expectRestoredVolumes[volumeName] = &longhorn.Volume{
			Spec: longhorn.VolumeSpec{
				NumberOfReplicas: 3,
			},
		}
		tc.backupVolumes[volumeName] = tc.expectRestoredVolumes[volumeName]
	}

	// init BackingImages
	if tc.existBackingImages == nil {
		tc.existBackingImages = map[SystemRolloutCRName]*longhorn.BackingImage{}
	}
	if tc.expectRestoredBackingImages == nil {
		tc.expectRestoredBackingImages = tc.existBackingImages
	}
}

func fakeSystemBackupArchieve(c *C, systemBackupName, systemRolloutOwnerID, rolloutControllerID, tempDir, downloadPath string,
	kubeClient *fake.Clientset, lhClient *lhfake.Clientset, extensionsClient *apiextensionsfake.Clientset, informerFactories *util.InformerFactories) {
	fakeSystemRolloutNamespace(c, informerFactories.KubeInformerFactory, kubeClient)

	systemBackupController, err := newFakeSystemBackupController(lhClient, kubeClient, extensionsClient, informerFactories, rolloutControllerID)
	c.Assert(err, IsNil)

	systemBackup := fakeSystemBackup(systemBackupName, systemRolloutOwnerID, "", false, "", longhorn.SystemBackupStateGenerating, c, informerFactories.LhInformerFactory, lhClient)

	systemBackupController.GenerateSystemBackup(systemBackup, downloadPath, tempDir)
	systemBackup, err = lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackupName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(systemBackup.Status.State, Equals, longhorn.SystemBackupStateUploading)
}

func assertRolloutCustomResourceDefinition(expectRestored map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition, c *C, client *apiextensionsfake.Clientset) {
	objList, err := client.ApiextensionsV1().CustomResourceDefinitions().List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]apiextensionsv1.CustomResourceDefinition{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec, restored.Spec), Equals, true)
	}
}

func assertRolloutClusterRoles(expectRestored map[SystemRolloutCRName]*rbacv1.ClusterRole, c *C, client *fake.Clientset) {
	objList, err := client.RbacV1().ClusterRoles().List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]rbacv1.ClusterRole{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Rules, restored.Rules), Equals, true)
	}
}

func assertRolloutClusterRoleBindings(expectRestored map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding, c *C, client *fake.Clientset) {
	objList, err := client.RbacV1().ClusterRoleBindings().List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]rbacv1.ClusterRoleBinding{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Subjects, restored.Subjects), Equals, true)
		c.Assert(reflect.DeepEqual(exist.RoleRef, restored.RoleRef), Equals, true)
	}
}

func assertRolloutConfigMaps(expectRestored map[SystemRolloutCRName]*corev1.ConfigMap, c *C, client *fake.Clientset) {
	objList, err := client.CoreV1().ConfigMaps(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]corev1.ConfigMap{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Data, restored.Data), Equals, true)
	}
}

func assertRolloutDaemonSets(expectRestored map[SystemRolloutCRName]*appsv1.DaemonSet, c *C, client *fake.Clientset) {
	objList, err := client.AppsV1().DaemonSets(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]appsv1.DaemonSet{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec.Template.Spec.Containers, restored.Spec.Template.Spec.Containers), Equals, true)
	}
}

func assertRolloutDeployments(expectRestored map[SystemRolloutCRName]*appsv1.Deployment, c *C, client *fake.Clientset) {
	objList, err := client.AppsV1().Deployments(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]appsv1.Deployment{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec.Template.Spec.Containers, restored.Spec.Template.Spec.Containers), Equals, true)
	}
}

func assertRolloutEngineImages(expectRestored map[SystemRolloutCRName]*longhorn.EngineImage, c *C, client *lhfake.Clientset) {
	objList, err := client.LonghornV1beta2().EngineImages(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]longhorn.EngineImage{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec.Image, restored.Spec.Image), Equals, true)
	}
}

func assertRolloutPersistentVolumes(expectRestored map[SystemRolloutCRName]*corev1.PersistentVolume, backups map[SystemRolloutCRName]*corev1.PersistentVolume, c *C, client *fake.Clientset) {
	objList, err := client.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]corev1.PersistentVolume{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(exist.Spec.StorageClassName, Equals, restored.Spec.StorageClassName)
	}

	for name, backup := range backups {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		if exist.Spec.StorageClassName != backup.Spec.StorageClassName {
			assertAnnotateSkippedLastSystemRestore(exist.Annotations, c)
		}
	}
}

func assertRolloutPersistentVolumeClaims(expectRestored map[SystemRolloutCRName]*corev1.PersistentVolumeClaim, backups map[SystemRolloutCRName]*corev1.PersistentVolumeClaim, c *C, client *fake.Clientset) {
	objList, err := client.CoreV1().PersistentVolumeClaims(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]corev1.PersistentVolumeClaim{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec.Resources.Requests, restored.Spec.Resources.Requests), Equals, true)
	}

	for name, backup := range backups {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		if !reflect.DeepEqual(exist.Spec.Resources.Requests, backup.Spec.Resources.Requests) {
			assertAnnotateSkippedLastSystemRestore(exist.Annotations, c)
		}
	}
}

func assertRolloutRecurringJobs(expectRestored map[SystemRolloutCRName]*longhorn.RecurringJob, c *C, client *lhfake.Clientset) {
	objList, err := client.LonghornV1beta2().RecurringJobs(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]longhorn.RecurringJob{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec, restored.Spec), Equals, true)
	}
}

func assertRolloutRoles(expectRestored map[SystemRolloutCRName]*rbacv1.Role, c *C, client *fake.Clientset) {
	objList, err := client.RbacV1().Roles(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]rbacv1.Role{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Rules, restored.Rules), Equals, true)
	}
}

func assertRolloutRoleBindings(expectRestored map[SystemRolloutCRName]*rbacv1.RoleBinding, c *C, client *fake.Clientset) {
	objList, err := client.RbacV1().RoleBindings(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]rbacv1.RoleBinding{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Subjects, restored.Subjects), Equals, true)
	}
}

func assertRolloutServices(expectRestored map[SystemRolloutCRName]*corev1.Service, c *C, client *fake.Clientset) {
	objList, err := client.CoreV1().Services(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]corev1.Service{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec, restored.Spec), Equals, true)
	}
}

func assertRolloutServiceAccounts(expectRestored map[SystemRolloutCRName]*corev1.ServiceAccount, c *C, client *fake.Clientset) {
	objList, err := client.CoreV1().ServiceAccounts(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]corev1.ServiceAccount{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name := range expectRestored {
		_, found := exists[name]
		c.Assert(found, Equals, true)
	}
}

func assertRolloutSettings(expectRestored map[SystemRolloutCRName]*longhorn.Setting, tcExists map[SystemRolloutCRName]*longhorn.Setting, c *C, client *lhfake.Clientset) {
	objList, err := client.LonghornV1beta2().Settings(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]longhorn.Setting{}
	for _, setting := range objList.Items {
		exists[SystemRolloutCRName(setting.Name)] = setting
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)

		expectValue := restored.Value
		if isSystemRolloutIgnoredSetting(string(name)) {
			expectValue = tcExists[name].Value
		}

		c.Assert(exist.Value, Equals, expectValue)
	}
}

func assertRolloutVolumes(expectRestored map[SystemRolloutCRName]*longhorn.Volume, backups map[SystemRolloutCRName]*longhorn.Volume, c *C, client *lhfake.Clientset) {
	objList, err := client.LonghornV1beta2().Volumes(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]longhorn.Volume{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(exist.Spec.NumberOfReplicas, Equals, restored.Spec.NumberOfReplicas)
	}

	for name, backup := range backups {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		if exist.Spec.NumberOfReplicas != backup.Spec.NumberOfReplicas {
			assertAnnotateSkippedLastSystemRestore(exist.Annotations, c)
		}
	}
}

func assertRolloutBackingImages(expectRestored map[SystemRolloutCRName]*longhorn.BackingImage, backups map[SystemRolloutCRName]*longhorn.BackingImage, c *C, client *lhfake.Clientset) {
	objList, err := client.LonghornV1beta2().BackingImages(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expectRestored))

	exists := map[SystemRolloutCRName]longhorn.BackingImage{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for name, restored := range expectRestored {
		exist, found := exists[name]
		c.Assert(found, Equals, true)
		c.Assert(exist.Spec.SourceType, Equals, restored.Spec.SourceType)
	}

	for name := range backups {
		_, found := exists[name]
		c.Assert(found, Equals, true)
	}
}

func assertAnnotateSkippedLastSystemRestore(annos map[string]string, c *C) {
	_, ok := annos[types.GetLastSkippedSystemRestoreLabelKey()]
	c.Assert(ok, Equals, true)

	_, ok = annos[types.GetLastSkippedSystemRestoreAtLabelKey()]
	c.Assert(ok, Equals, true)
}
