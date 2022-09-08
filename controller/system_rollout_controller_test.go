package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	policyv1beta1 "k8s.io/api/policy/v1beta1"
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
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

type SystemRolloutTestCase struct {
	state longhorn.SystemRestoreState

	isInProgress      bool
	systemRestoreName string
	restoreErrors     []string

	existClusterRoles           map[SystemRolloutCRName]*rbacv1.ClusterRole
	existClusterRoleBindings    map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding
	existConfigMaps             map[SystemRolloutCRName]*corev1.ConfigMap
	existCRDVersions            map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition
	existDaemonSets             map[SystemRolloutCRName]*appsv1.DaemonSet
	existDeployments            map[SystemRolloutCRName]*appsv1.Deployment
	existEngineImages           map[SystemRolloutCRName]*longhorn.EngineImage
	existPersistentVolumes      map[SystemRolloutCRName]*corev1.PersistentVolume
	existPersistentVolumeClaims map[SystemRolloutCRName]*corev1.PersistentVolumeClaim
	existPodSecurityPolicies    map[SystemRolloutCRName]*policyv1beta1.PodSecurityPolicy
	existRecurringJobs          map[SystemRolloutCRName]*longhorn.RecurringJob
	existRoles                  map[SystemRolloutCRName]*rbacv1.Role
	existRoleBindings           map[SystemRolloutCRName]*rbacv1.RoleBinding
	existServiceAccounts        map[SystemRolloutCRName]*corev1.ServiceAccount
	existSettings               map[SystemRolloutCRName]*longhorn.Setting
	existStorageClasses         map[SystemRolloutCRName]*storagev1.StorageClass
	existVolumes                map[SystemRolloutCRName]*longhorn.Volume

	expectError                 string
	expectErrorConditionMessage string
	expectState                 longhorn.SystemRestoreState

	expectClusterRoles           map[SystemRolloutCRName]*rbacv1.ClusterRole
	expectClusterRoleBindings    map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding
	expectConfigMaps             map[SystemRolloutCRName]*corev1.ConfigMap
	expectCRDVersions            map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition
	expectDaemonSets             map[SystemRolloutCRName]*appsv1.DaemonSet
	expectDeployments            map[SystemRolloutCRName]*appsv1.Deployment
	expectEngineImages           map[SystemRolloutCRName]*longhorn.EngineImage
	expectPersistentVolumes      map[SystemRolloutCRName]*corev1.PersistentVolume
	expectPersistentVolumeClaims map[SystemRolloutCRName]*corev1.PersistentVolumeClaim
	expectPodSecurityPolicies    map[SystemRolloutCRName]*policyv1beta1.PodSecurityPolicy
	expectRecurringJobs          map[SystemRolloutCRName]*longhorn.RecurringJob
	expectRoles                  map[SystemRolloutCRName]*rbacv1.Role
	expectRoleBindings           map[SystemRolloutCRName]*rbacv1.RoleBinding
	expectServiceAccounts        map[SystemRolloutCRName]*corev1.ServiceAccount
	expectSettings               map[SystemRolloutCRName]*longhorn.Setting
	expectStorageClasses         map[SystemRolloutCRName]*storagev1.StorageClass
	expectVolumes                map[SystemRolloutCRName]*longhorn.Volume
}

func (s *TestSuite) TestSystemRollout(c *C) {
	datastore.SystemRestoreTimeout = 10 // 10 seconds

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
			expectClusterRoles: map[SystemRolloutCRName]*rbacv1.ClusterRole{
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
			expectClusterRoles: map[SystemRolloutCRName]*rbacv1.ClusterRole{
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
			expectClusterRoleBindings: map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding{
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
			expectServiceAccounts: map[SystemRolloutCRName]*corev1.ServiceAccount{
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
			expectClusterRoleBindings: map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding{
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
			expectServiceAccounts: map[SystemRolloutCRName]*corev1.ServiceAccount{
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
			expectStorageClasses: map[SystemRolloutCRName]*storagev1.StorageClass{
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
			expectConfigMaps: map[SystemRolloutCRName]*corev1.ConfigMap{
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
			expectConfigMaps: map[SystemRolloutCRName]*corev1.ConfigMap{
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
			expectCRDVersions: map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition{
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
			expectCRDVersions: map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition{
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
			expectDaemonSets: map[SystemRolloutCRName]*appsv1.DaemonSet{
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
			expectDaemonSets: map[SystemRolloutCRName]*appsv1.DaemonSet{
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
			expectDeployments: map[SystemRolloutCRName]*appsv1.Deployment{
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
			expectDeployments: map[SystemRolloutCRName]*appsv1.Deployment{
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
			expectEngineImages: map[SystemRolloutCRName]*longhorn.EngineImage{
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
			expectEngineImages: map[SystemRolloutCRName]*longhorn.EngineImage{
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
							Name:      TestPVCName + TestDiffSuffix,
							Namespace: TestNamespace,
						},
						StorageClassName: TestStorageClassName,
					},
				},
			},
			expectPersistentVolumes: map[SystemRolloutCRName]*corev1.PersistentVolume{
				SystemRolloutCRName(TestPVName): {
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      TestPVCName,
							Namespace: TestNamespace,
						},
						StorageClassName: TestStorageClassName,
					},
				},
			},
		},
		"system rollout PersistentVolume not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existPersistentVolumes: nil,
			expectPersistentVolumes: map[SystemRolloutCRName]*corev1.PersistentVolume{
				SystemRolloutCRName(TestPVName): {
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      TestPVCName,
							Namespace: TestNamespace,
						},
						StorageClassName: TestStorageClassName,
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
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: request100Mi,
							},
						},
					},
				},
			},
			expectPersistentVolumeClaims: map[SystemRolloutCRName]*corev1.PersistentVolumeClaim{
				SystemRolloutCRName(TestPVCName): {
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: &testStorageClassName,
						VolumeName:       TestVolumeName,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: request200Mi,
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
			expectPersistentVolumeClaims: map[SystemRolloutCRName]*corev1.PersistentVolumeClaim{
				SystemRolloutCRName(TestPVCName): {
					Spec: corev1.PersistentVolumeClaimSpec{
						StorageClassName: &testStorageClassName,
						VolumeName:       TestVolumeName,
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: request100Mi,
							},
						},
					},
				},
			},
		},
		"system rollout PodSecurityPolicies exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existPodSecurityPolicies: map[SystemRolloutCRName]*policyv1beta1.PodSecurityPolicy{
				SystemRolloutCRName(TestPodSecurityPolicyName): {
					Spec: policyv1beta1.PodSecurityPolicySpec{
						Privileged: false,
					},
				},
			},
			expectPodSecurityPolicies: map[SystemRolloutCRName]*policyv1beta1.PodSecurityPolicy{
				SystemRolloutCRName(TestPodSecurityPolicyName): {
					Spec: policyv1beta1.PodSecurityPolicySpec{
						Privileged: true,
					},
				},
			},
		},
		"system rollout PodSecurityPolicies not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existPodSecurityPolicies: nil,
			expectPodSecurityPolicies: map[SystemRolloutCRName]*policyv1beta1.PodSecurityPolicy{
				SystemRolloutCRName(TestPodSecurityPolicyName): {
					Spec: policyv1beta1.PodSecurityPolicySpec{
						Privileged: true,
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
			expectRecurringJobs: map[SystemRolloutCRName]*longhorn.RecurringJob{
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
			expectRecurringJobs: map[SystemRolloutCRName]*longhorn.RecurringJob{
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
			expectRoles: map[SystemRolloutCRName]*rbacv1.Role{
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
			expectRoles: map[SystemRolloutCRName]*rbacv1.Role{
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
			expectRoleBindings: map[SystemRolloutCRName]*rbacv1.RoleBinding{
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
			expectRoleBindings: map[SystemRolloutCRName]*rbacv1.RoleBinding{
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
			expectServiceAccounts: map[SystemRolloutCRName]*corev1.ServiceAccount{
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
			expectSettings: map[SystemRolloutCRName]*longhorn.Setting{
				SystemRolloutCRName(types.SettingNameDefaultReplicaCount): {Value: "3"},
			},
		},
		"system rollout Setting not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existSettings: map[SystemRolloutCRName]*longhorn.Setting{},
			expectSettings: map[SystemRolloutCRName]*longhorn.Setting{
				SystemRolloutCRName(types.SettingNameDefaultReplicaCount): {Value: "3"},
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
			expectVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Spec: longhorn.VolumeSpec{
						NumberOfReplicas: 5,
					},
				},
			},
		},
		"system rollout Volume not exist in cluster": {
			state:        longhorn.SystemRestoreStateRestoring,
			isInProgress: true,
			expectState:  longhorn.SystemRestoreStateCompleted,

			existVolumes: nil,
			expectVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Spec: longhorn.VolumeSpec{
						NumberOfReplicas: 3,
					},
				},
			},
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		tc.initTestCase()

		extensionsClient := apiextensionsfake.NewSimpleClientset()
		extensionsInformerFactory := apiextensionsinformers.NewSharedInformerFactory(extensionsClient, controller.NoResyncPeriodFunc())

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformers.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		fakeSystemRolloutBackupTargetDefault(c, lhInformerFactory, lhClient)

		fakeSystemRolloutSettings(tc.expectSettings, c, lhInformerFactory, lhClient)
		fakeSystemRolloutCustomResourceDefinitions(tc.expectCRDVersions, c, extensionsInformerFactory, extensionsClient)
		fakeSystemRolloutClusterRoles(tc.expectClusterRoles, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutClusterRoleBindings(tc.expectClusterRoleBindings, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutConfigMaps(tc.expectConfigMaps, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutDaemonSets(tc.expectDaemonSets, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutDeployments(tc.expectDeployments, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutEngineImages(tc.expectEngineImages, c, lhInformerFactory, lhClient)
		fakeSystemRolloutPersistentVolumes(tc.expectPersistentVolumes, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutPersistentVolumeClaims(tc.expectPersistentVolumeClaims, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutPodSecurityPolicies(tc.expectPodSecurityPolicies, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutRecurringJobs(tc.expectRecurringJobs, c, lhInformerFactory, lhClient)
		fakeSystemRolloutRoles(tc.expectRoles, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutRoleBindings(tc.expectRoleBindings, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutServiceAccounts(tc.expectServiceAccounts, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutStorageClasses(tc.expectStorageClasses, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutVolumes(tc.expectVolumes, c, lhInformerFactory, lhClient)

		ds := datastore.NewDataStore(lhInformerFactory, lhClient, kubeInformerFactory, kubeClient, extensionsClient, TestNamespace)
		doneCh := make(chan struct{})
		if tc.expectState != longhorn.SystemRestoreStateCompleted && tc.expectState != longhorn.SystemRestoreStateError {
			doneChs = append(doneChs, doneCh)
		}

		controller := newFakeSystemRolloutController(tc.systemRestoreName, controllerID, ds, doneCh, kubeClient, extensionsClient)
		controller.systemRestoreVersion = TestSystemBackupLonghornVersion
		controller.cacheErrors = util.MultiError{}

		fakeSystemRestore(tc.systemRestoreName, systemRolloutOwnerID, tc.isInProgress, false, tc.state, c, lhInformerFactory, lhClient, controller.ds)

		var err error
		controller.systemRestore, err = lhClient.LonghornV1beta2().SystemRestores(TestNamespace).Get(context.TODO(), tc.systemRestoreName, metav1.GetOptions{})
		c.Assert(err, IsNil)

		rolloutTempDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("*-%v", TestSystemRestoreName))
		c.Assert(err, IsNil)
		tempDirs = append(tempDirs, rolloutTempDir)

		controller.downloadPath = filepath.Join(rolloutTempDir, TestSystemBackupName+types.SystemBackupExtension)
		tempDir := filepath.Join(rolloutTempDir, TestSystemBackupName)

		if tc.state != longhorn.SystemRestoreStateUnpacking || (tc.state == longhorn.SystemRestoreStateUnpacking && tc.expectState != longhorn.SystemRestoreStateError) {
			fakeSystemBackupArchieve(c, TestSystemBackupName, controllerID, systemRolloutOwnerID, tempDir, controller.downloadPath,
				kubeInformerFactory, kubeClient, lhInformerFactory, lhClient, extensionsClient)
		}

		err = os.RemoveAll(tempDir)
		c.Assert(err, IsNil)

		fakeSystemRolloutClusterRoles(tc.existClusterRoles, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutClusterRoleBindings(tc.existClusterRoleBindings, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutConfigMaps(tc.existConfigMaps, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutCustomResourceDefinitions(tc.existCRDVersions, c, extensionsInformerFactory, extensionsClient)
		fakeSystemRolloutDaemonSets(tc.existDaemonSets, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutDeployments(tc.existDeployments, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutEngineImages(tc.existEngineImages, c, lhInformerFactory, lhClient)
		fakeSystemRolloutPersistentVolumes(tc.existPersistentVolumes, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutPersistentVolumeClaims(tc.existPersistentVolumeClaims, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutPodSecurityPolicies(tc.existPodSecurityPolicies, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutRecurringJobs(tc.existRecurringJobs, c, lhInformerFactory, lhClient)
		fakeSystemRolloutRoles(tc.existRoles, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutRoleBindings(tc.existRoleBindings, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutServiceAccounts(tc.existServiceAccounts, c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutSettings(tc.existSettings, c, lhInformerFactory, lhClient)
		fakeSystemRolloutVolumes(tc.existVolumes, c, lhInformerFactory, lhClient)

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
			assertRolloutClusterRoles(tc.expectClusterRoles, c, kubeClient)
			assertRolloutClusterRoleBindings(tc.expectClusterRoleBindings, c, kubeClient)
			assertRolloutCustomResourceDefinition(tc.expectCRDVersions, c, extensionsClient)
			assertRolloutConfigMaps(tc.expectConfigMaps, c, kubeClient)
			assertRolloutDaemonSets(tc.expectDaemonSets, c, kubeClient)
			assertRolloutDeployments(tc.expectDeployments, c, kubeClient)
			assertRolloutEngineImages(tc.expectEngineImages, c, lhClient)
			assertRolloutPersistentVolumes(tc.expectPersistentVolumes, tc.existPersistentVolumes, c, kubeClient)
			assertRolloutPersistentVolumeClaims(tc.expectPersistentVolumeClaims, tc.existPersistentVolumeClaims, c, kubeClient)
			assertRolloutPodSecurityPolicies(tc.expectPodSecurityPolicies, c, kubeClient)
			assertRolloutRecurringJobs(tc.expectRecurringJobs, c, lhClient)
			assertRolloutRoles(tc.expectRoles, c, kubeClient)
			assertRolloutRoleBindings(tc.expectRoleBindings, c, kubeClient)
			assertRolloutServiceAccounts(tc.expectServiceAccounts, c, kubeClient)
			assertRolloutSettings(tc.expectSettings, c, lhClient)
			assertRolloutVolumes(tc.expectVolumes, c, lhClient)
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
	extensionsClient *apiextensionsfake.Clientset) *SystemRolloutController {
	logger := logrus.StandardLogger()
	logrus.SetLevel(logrus.DebugLevel)

	c := NewSystemRolloutController(systemRestoreName, logger, controllerID, ds, scheme.Scheme, stopCh, kubeClient, extensionsClient)
	c.eventRecorder = record.NewFakeRecorder(100)
	for index := range c.cacheSyncs {
		c.cacheSyncs[index] = alwaysReady
	}

	c.backupTargetClient = &FakeSystemBackupTargetClient{
		name:    TestSystemBackupName,
		version: TestSystemBackupLonghornVersion,
	}

	return c
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
	if tc.expectClusterRoles == nil {
		tc.expectClusterRoles = tc.existClusterRoles
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
	if tc.expectClusterRoleBindings == nil {
		tc.expectClusterRoleBindings = tc.existClusterRoleBindings
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
	if tc.expectConfigMaps == nil {
		tc.expectConfigMaps = tc.existConfigMaps
	}

	// init CustomResourceDefinitions
	if len(tc.expectCRDVersions) == 0 && len(tc.existCRDVersions) != 0 {
		tc.expectCRDVersions = tc.existCRDVersions
	}

	// init DaemonSets
	if tc.existDaemonSets == nil {
		tc.existDaemonSets = map[SystemRolloutCRName]*appsv1.DaemonSet{}
	}
	if tc.expectDaemonSets == nil {
		tc.expectDaemonSets = tc.existDaemonSets
	}

	// init Deployments
	if tc.existDeployments == nil {
		tc.existDeployments = map[SystemRolloutCRName]*appsv1.Deployment{}
	}
	if tc.expectDeployments == nil {
		tc.expectDeployments = tc.existDeployments
	}

	// init EngineImages
	if tc.existEngineImages == nil {
		tc.existEngineImages = map[SystemRolloutCRName]*longhorn.EngineImage{}
	}
	if tc.expectEngineImages == nil {
		tc.expectEngineImages = tc.existEngineImages
	}

	// init PersistentVolumes
	if tc.existPersistentVolumes == nil {
		tc.existPersistentVolumes = map[SystemRolloutCRName]*corev1.PersistentVolume{}
	}
	if tc.expectPersistentVolumes == nil {
		tc.expectPersistentVolumes = tc.existPersistentVolumes
	}
	for _, pvc := range tc.expectPersistentVolumeClaims {
		volumeName := SystemRolloutCRName(pvc.Spec.VolumeName)
		_, found := tc.expectPersistentVolumes[volumeName]
		if found {
			continue
		}

		tc.expectPersistentVolumes[volumeName] = &corev1.PersistentVolume{
			Spec: corev1.PersistentVolumeSpec{
				ClaimRef: &corev1.ObjectReference{
					Name:      string(volumeName),
					Namespace: TestNamespace,
				},
				StorageClassName: *pvc.Spec.StorageClassName,
			},
		}
	}

	// init PersistentVolumeClaims
	if tc.existPersistentVolumeClaims == nil {
		tc.existPersistentVolumeClaims = map[SystemRolloutCRName]*corev1.PersistentVolumeClaim{}
	}
	if tc.expectPersistentVolumeClaims == nil {
		tc.expectPersistentVolumeClaims = tc.existPersistentVolumeClaims
	}

	// init PodSecurityPolicies
	if tc.existPodSecurityPolicies == nil {
		tc.existPodSecurityPolicies = map[SystemRolloutCRName]*policyv1beta1.PodSecurityPolicy{}
	}
	if tc.expectPodSecurityPolicies == nil {
		tc.expectPodSecurityPolicies = tc.existPodSecurityPolicies
	}

	// init RecurringJobs
	if tc.existRecurringJobs == nil {
		tc.existRecurringJobs = map[SystemRolloutCRName]*longhorn.RecurringJob{}
	}
	if tc.expectRecurringJobs == nil {
		tc.expectRecurringJobs = tc.existRecurringJobs
	}

	// init Roles
	if tc.existRoles == nil {
		tc.existRoles = map[SystemRolloutCRName]*rbacv1.Role{}
	}
	if tc.expectRoles == nil {
		tc.expectRoles = tc.existRoles
	}
	addRoleRuleResources := map[string]struct{}{}
	for pspName := range tc.expectPodSecurityPolicies {
		addRoleRuleResources[string(pspName)] = struct{}{}
	}
	for _, role := range tc.expectRoles {
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
		tc.expectRoles[name] = newRole(string(name), []rbacv1.PolicyRule{policyRule})
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

	if tc.expectServiceAccounts == nil {
		tc.expectServiceAccounts = tc.existServiceAccounts
	}

	// init Settings
	if tc.existSettings == nil {
		tc.existSettings = map[SystemRolloutCRName]*longhorn.Setting{}
	}
	if tc.expectSettings == nil {
		tc.expectSettings = tc.existSettings
	}
	tc.expectSettings[SystemRolloutCRName(types.SettingNameDefaultEngineImage)] = &longhorn.Setting{Value: TestEngineImage}
	tc.existSettings[SystemRolloutCRName(types.SettingNameDefaultEngineImage)] = &longhorn.Setting{Value: TestEngineImage}

	// init StorageClasses
	if tc.existStorageClasses == nil {
		tc.existStorageClasses = map[SystemRolloutCRName]*storagev1.StorageClass{}
	}
	if tc.expectStorageClasses == nil {
		tc.expectStorageClasses = tc.existStorageClasses
	}
	tc.expectStorageClasses[SystemRolloutCRName(TestStorageClassName)] = &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: TestStorageClassName,
		},
	}
	tc.existStorageClasses[SystemRolloutCRName(TestStorageClassName)] = &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: TestStorageClassName,
		},
	}

	// init Volumes
	if tc.existVolumes == nil {
		tc.existVolumes = map[SystemRolloutCRName]*longhorn.Volume{}
	}
	if tc.expectVolumes == nil {
		tc.expectVolumes = tc.existVolumes
	}
	for pvName := range tc.expectPersistentVolumes {
		_, found := tc.expectVolumes[pvName]
		if found {
			continue
		}

		tc.expectVolumes[pvName] = &longhorn.Volume{
			Spec: longhorn.VolumeSpec{
				NumberOfReplicas: 3,
			},
		}
	}
}

func fakeSystemBackupArchieve(c *C, systemBackupName, systemRolloutOwnerID, rolloutControllerID, tempDir, downloadPath string,
	kubeInformerFactory informers.SharedInformerFactory, kubeClient *fake.Clientset,
	lhInformerFactory lhinformers.SharedInformerFactory, lhClient *lhfake.Clientset,
	extensionsClient *apiextensionsfake.Clientset) {
	fakeSystemRolloutNamespace(c, kubeInformerFactory, kubeClient)

	systemBackupController := newFakeSystemBackupController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient, extensionsClient, rolloutControllerID)
	systemBackup := fakeSystemBackup(systemBackupName, systemRolloutOwnerID, "", false, longhorn.SystemBackupStateGenerating, c, lhInformerFactory, lhClient)

	systemBackupController.GenerateSystemBackup(systemBackup, downloadPath, tempDir)
	systemBackup, err := lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackupName, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(systemBackup.Status.State, Equals, longhorn.SystemBackupStateUploading)
}

func assertRolloutCustomResourceDefinition(expects map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition, c *C, client *apiextensionsfake.Clientset) {
	objList, err := client.ApiextensionsV1().CustomResourceDefinitions().List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]apiextensionsv1.CustomResourceDefinition{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec, expect.Spec), Equals, true)
	}
}

func assertRolloutClusterRoles(expects map[SystemRolloutCRName]*rbacv1.ClusterRole, c *C, client *fake.Clientset) {
	objList, err := client.RbacV1().ClusterRoles().List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]rbacv1.ClusterRole{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Rules, expect.Rules), Equals, true)
	}
}

func assertRolloutClusterRoleBindings(expects map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding, c *C, client *fake.Clientset) {
	objList, err := client.RbacV1().ClusterRoleBindings().List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]rbacv1.ClusterRoleBinding{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Subjects, expect.Subjects), Equals, true)
		c.Assert(reflect.DeepEqual(exist.RoleRef, expect.RoleRef), Equals, true)
	}
}

func assertRolloutConfigMaps(expects map[SystemRolloutCRName]*corev1.ConfigMap, c *C, client *fake.Clientset) {
	objList, err := client.CoreV1().ConfigMaps(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]corev1.ConfigMap{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Data, expect.Data), Equals, true)
	}
}

func assertRolloutDaemonSets(expects map[SystemRolloutCRName]*appsv1.DaemonSet, c *C, client *fake.Clientset) {
	objList, err := client.AppsV1().DaemonSets(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]appsv1.DaemonSet{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec.Template.Spec.Containers, expect.Spec.Template.Spec.Containers), Equals, true)
	}
}

func assertRolloutDeployments(expects map[SystemRolloutCRName]*appsv1.Deployment, c *C, client *fake.Clientset) {
	objList, err := client.AppsV1().Deployments(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]appsv1.Deployment{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec.Template.Spec.Containers, expect.Spec.Template.Spec.Containers), Equals, true)
	}
}

func assertRolloutEngineImages(expects map[SystemRolloutCRName]*longhorn.EngineImage, c *C, client *lhfake.Clientset) {
	objList, err := client.LonghornV1beta2().EngineImages(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]longhorn.EngineImage{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec.Image, expect.Spec.Image), Equals, true)
	}
}

func assertRolloutPersistentVolumes(expects map[SystemRolloutCRName]*corev1.PersistentVolume, tcExists map[SystemRolloutCRName]*corev1.PersistentVolume, c *C, client *fake.Clientset) {
	objList, err := client.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]corev1.PersistentVolume{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(exist.Spec.StorageClassName, Equals, expect.Spec.StorageClassName)

		if _, prepared := tcExists[key]; !prepared {
			c.Assert(exist.Spec.ClaimRef, Equals, (*corev1.ObjectReference)(nil))
		}
	}
}

func assertRolloutPersistentVolumeClaims(expects map[SystemRolloutCRName]*corev1.PersistentVolumeClaim, tcExists map[SystemRolloutCRName]*corev1.PersistentVolumeClaim, c *C, client *fake.Clientset) {
	objList, err := client.CoreV1().PersistentVolumeClaims(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	// Longhorn does not restore the requested storage size of existing PVC to avoid data lose.
	if len(tcExists) != 0 {
		expects = tcExists
	}
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]corev1.PersistentVolumeClaim{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec.Resources.Requests, expect.Spec.Resources.Requests), Equals, true)
	}
}

func assertRolloutPodSecurityPolicies(expects map[SystemRolloutCRName]*policyv1beta1.PodSecurityPolicy, c *C, client *fake.Clientset) {
	objList, err := client.PolicyV1beta1().PodSecurityPolicies().List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]policyv1beta1.PodSecurityPolicy{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec, expect.Spec), Equals, true)
	}
}

func assertRolloutRecurringJobs(expects map[SystemRolloutCRName]*longhorn.RecurringJob, c *C, client *lhfake.Clientset) {
	objList, err := client.LonghornV1beta2().RecurringJobs(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]longhorn.RecurringJob{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Spec, expect.Spec), Equals, true)
	}
}

func assertRolloutRoles(expects map[SystemRolloutCRName]*rbacv1.Role, c *C, client *fake.Clientset) {
	objList, err := client.RbacV1().Roles(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]rbacv1.Role{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Rules, expect.Rules), Equals, true)
	}
}

func assertRolloutRoleBindings(expects map[SystemRolloutCRName]*rbacv1.RoleBinding, c *C, client *fake.Clientset) {
	objList, err := client.RbacV1().RoleBindings(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]rbacv1.RoleBinding{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(reflect.DeepEqual(exist.Subjects, expect.Subjects), Equals, true)
	}
}

func assertRolloutServiceAccounts(expects map[SystemRolloutCRName]*corev1.ServiceAccount, c *C, client *fake.Clientset) {
	objList, err := client.CoreV1().ServiceAccounts(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]corev1.ServiceAccount{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key := range expects {
		_, found := exists[key]
		c.Assert(found, Equals, true)
	}
}

func assertRolloutSettings(expects map[SystemRolloutCRName]*longhorn.Setting, c *C, client *lhfake.Clientset) {
	objList, err := client.LonghornV1beta2().Settings(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]longhorn.Setting{}
	for _, setting := range objList.Items {
		exists[SystemRolloutCRName(setting.Name)] = setting
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(exist.Value, Equals, expect.Value)
	}
}

func assertRolloutVolumes(expects map[SystemRolloutCRName]*longhorn.Volume, c *C, client *lhfake.Clientset) {
	objList, err := client.LonghornV1beta2().Volumes(TestNamespace).List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)
	c.Assert(len(objList.Items), Equals, len(expects))

	exists := map[SystemRolloutCRName]longhorn.Volume{}
	for _, obj := range objList.Items {
		exists[SystemRolloutCRName(obj.Name)] = obj
	}

	for key, expect := range expects {
		exist, found := exists[key]
		c.Assert(found, Equals, true)
		c.Assert(exist.Spec.NumberOfReplicas, Equals, expect.Spec.NumberOfReplicas)
	}
}
