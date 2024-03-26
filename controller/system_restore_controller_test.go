package controller

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	systembackupstore "github.com/longhorn/backupstore/systembackup"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apiextensionsinformers "k8s.io/apiextensions-apiserver/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

const (
	TestSystemRestoreName                        = "system-restore-0"
	TestSystemRestoreNameGetConfigFailed         = "system-restore-get-config-failed"
	TestSystemRestoreNameUploadFailed            = "system-restore-upload-failed"
	TestSystemRestoreNameUploadExceedTimeout     = "system-restore-upload-exceed-timeout"
	TestSystemRestoreNameRestoreMultipleFailures = "system-restore-multi-failures"

	TestDiffSuffix   = "-diff"
	TestIgnoreSuffix = "-ignore"
)

type SystemRolloutCRName string
type SystemRolloutCRValue string

type SystemRestoreTestCase struct {
	controllerID string

	systemBackupName string
	state            longhorn.SystemRestoreState
	notExist         bool
	isDeleting       bool

	expectError    bool
	expectJobExist bool
	expectState    longhorn.SystemRestoreState
}

func (s *TestSuite) TestReconcileSystemRestore(c *C) {
	systemRestoreName := TestSystemRestoreName
	systemRestoreOwnerID := TestNode1
	backupTargetClient := &FakeSystemBackupTargetClient{}
	testCases := map[string]SystemRestoreTestCase{
		"system restore": {
			expectJobExist: true,
		},
		"system restore not exist": {
			notExist:       true,
			expectJobExist: false,
		},
		"system restore change ownership": {
			controllerID:   TestNode2,
			expectJobExist: true,
		},
		"system restore with deletion timestamp": {
			state:          longhorn.SystemRestoreStateCompleted,
			isDeleting:     true,
			expectJobExist: true,
			expectState:    longhorn.SystemRestoreStateDeleting,
		},
		"system restore deleting": {
			state:          longhorn.SystemRestoreStateDeleting,
			isDeleting:     true,
			expectJobExist: false,
		},
		"system restore missing backup config file": {
			systemBackupName: TestSystemBackupNameGetConfigFailed,
			expectState:      longhorn.SystemRestoreStateError,
			expectJobExist:   false,
		},
	}

	for name, tc := range testCases {
		if tc.controllerID == "" {
			tc.controllerID = systemRestoreOwnerID
		}

		if tc.systemBackupName == "" {
			tc.systemBackupName = TestSystemBackupName
		}

		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		lhClient := lhfake.NewSimpleClientset()
		extensionsClient := apiextensionsfake.NewSimpleClientset()

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		fakeSystemRolloutManagerPod(c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutSettingDefaultEngineImage(c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutBackupTargetDefault(c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemBackup(tc.systemBackupName, systemRestoreOwnerID, "", false, "", longhorn.SystemBackupStateGenerating, c, informerFactories.LhInformerFactory, lhClient)

		systemRestoreController, err := newFakeSystemRestoreController(lhClient, kubeClient, extensionsClient, informerFactories, tc.controllerID)
		c.Assert(err, IsNil)

		if !tc.notExist {
			if tc.state == "" {
				tc.state = longhorn.SystemRestoreStateNone
			}

			systemRestore := fakeSystemRestore(systemRestoreName, systemRestoreOwnerID, false, tc.isDeleting, tc.state, c, informerFactories.LhInformerFactory, lhClient, systemRestoreController.ds)
			if tc.isDeleting {
				_, err := systemRestoreController.CreateSystemRestoreJob(systemRestore, backupTargetClient)
				c.Assert(err, IsNil)
			}
		}

		err = systemRestoreController.reconcile(systemRestoreName, backupTargetClient)
		if tc.expectError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}

		systemRolloutName := getSystemRolloutName(systemRestoreName)
		job, err := kubeClient.BatchV1().Jobs(TestNamespace).Get(context.TODO(), systemRolloutName, metav1.GetOptions{})
		if tc.expectJobExist {
			c.Assert(err, IsNil)
			c.Assert(job.Spec.Template.Spec.Containers[0].Image, Equals, TestManagerImage)
			c.Assert(strings.HasPrefix(job.Name, SystemRolloutNamePrefix), Equals, true)
			c.Assert(strings.HasPrefix(job.Spec.Template.Spec.Containers[0].Name, SystemRolloutNamePrefix), Equals, true)
		} else {
			c.Assert(err, NotNil)
		}

		if tc.expectState != "" {
			systemRestore, err := lhClient.LonghornV1beta2().SystemRestores(TestNamespace).Get(context.TODO(), systemRestoreName, metav1.GetOptions{})
			c.Assert(err, IsNil)
			c.Assert(systemRestore.Status.State, Equals, tc.expectState)
		}
	}
}

func newFakeSystemRestoreController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*SystemRestoreController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()
	logrus.SetLevel(logrus.DebugLevel)

	c, err := NewSystemRestoreController(logger, ds, scheme.Scheme, kubeClient, TestNamespace, controllerID)
	if err != nil {
		return nil, err
	}
	c.eventRecorder = record.NewFakeRecorder(100)
	for index := range c.cacheSyncs {
		c.cacheSyncs[index] = alwaysReady
	}

	return c, nil
}

func fakeSystemRestore(name, currentOwnerID string, isInProgress, isDeleting bool, state longhorn.SystemRestoreState,
	c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset, ds *datastore.DataStore) *longhorn.SystemRestore {
	systemRestore := newSystemRestore(name, currentOwnerID, state)
	if isDeleting {
		now := metav1.NewTime(time.Now())
		systemRestore.DeletionTimestamp = &now
	}

	systemRestore, err := client.LonghornV1beta2().SystemRestores(TestNamespace).Create(context.TODO(), systemRestore, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	if isInProgress {
		systemRestore, err = ds.UpdateSystemRestore(systemRestore)
		c.Assert(err, IsNil)
	}

	indexer := informerFactory.Longhorn().V1beta2().SystemRestores().Informer().GetIndexer()
	err = indexer.Add(systemRestore)
	c.Assert(err, IsNil)

	return systemRestore
}

func fakeSystemRolloutClusterRoles(fakeObjs map[SystemRolloutCRName]*rbacv1.ClusterRole, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Rbac().V1().ClusterRoles().Informer().GetIndexer()

	clientInterface := client.RbacV1().ClusterRoles()

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newClusterRole(name, fakeObj.Rules), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutClusterRoleBindings(fakeObjs map[SystemRolloutCRName]*rbacv1.ClusterRoleBinding, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Rbac().V1().ClusterRoleBindings().Informer().GetIndexer()

	clientInterface := client.RbacV1().ClusterRoleBindings()

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newClusterRoleBinding(name, fakeObj.Subjects, fakeObj.RoleRef), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutConfigMaps(fakeObjs map[SystemRolloutCRName]*corev1.ConfigMap, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Core().V1().ConfigMaps().Informer().GetIndexer()

	clientInterface := client.CoreV1().ConfigMaps(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newConfigMap(name, fakeObj.Data), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutCustomResourceDefinitions(fakeObjs map[SystemRolloutCRName]*apiextensionsv1.CustomResourceDefinition, c *C, informerFactory apiextensionsinformers.SharedInformerFactory, client *apiextensionsfake.Clientset) {
	indexer := informerFactory.Apiextensions().V1().CustomResourceDefinitions().Informer().GetIndexer()

	clientInterface := client.ApiextensionsV1().CustomResourceDefinitions()

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newCustomResourceDefinition(name, fakeObj.Spec), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutBackups(fakeObjs map[string]*longhorn.Backup, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().Backups().Informer().GetIndexer()

	clientInterface := client.LonghornV1beta2().Backups(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		backup := newBackup(name)
		backup.Status = fakeObj.Status
		exist, err := clientInterface.Create(context.TODO(), backup, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutBackupBackingImages(fakeObjs map[string]*longhorn.BackupBackingImage, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().BackupBackingImages().Informer().GetIndexer()

	clientInterface := client.LonghornV1beta2().BackupBackingImages(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		backupBackingImage := newBackupBackingImage(name)
		backupBackingImage.Status = fakeObj.Status
		exist, err := clientInterface.Create(context.TODO(), backupBackingImage, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutDaemonSets(fakeObjs map[SystemRolloutCRName]*appsv1.DaemonSet, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Apps().V1().DaemonSets().Informer().GetIndexer()

	clientInterface := client.AppsV1().DaemonSets(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newDaemonSet(name, fakeObj.Spec, fakeObj.Labels), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutDeployments(fakeObjs map[SystemRolloutCRName]*appsv1.Deployment, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Apps().V1().Deployments().Informer().GetIndexer()

	clientInterface := client.AppsV1().Deployments(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newDeployment(name, fakeObj.Spec), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutEngineImages(fakeObjs map[SystemRolloutCRName]*longhorn.EngineImage, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()

	clientInterface := client.LonghornV1beta2().EngineImages(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		engineImage := newEngineImage(fakeObj.Spec.Image, fakeObj.Status.State)
		engineImage.Name = name
		exist, err := clientInterface.Create(context.TODO(), engineImage, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutPersistentVolumes(fakeObjs map[SystemRolloutCRName]*corev1.PersistentVolume, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()

	clientInterface := client.CoreV1().PersistentVolumes()

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		persistentVolume := newPV()
		persistentVolume.Name = name
		persistentVolume.Spec.ClaimRef = fakeObj.Spec.ClaimRef
		persistentVolume.Spec.StorageClassName = fakeObj.Spec.StorageClassName
		if !reflect.DeepEqual(fakeObj.Spec.PersistentVolumeSource, corev1.PersistentVolumeSource{}) {
			persistentVolume.Spec.PersistentVolumeSource = fakeObj.Spec.PersistentVolumeSource
		}
		exist, err := clientInterface.Create(context.TODO(), persistentVolume, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutPersistentVolumeClaims(fakeObjs map[SystemRolloutCRName]*corev1.PersistentVolumeClaim, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()

	clientInterface := client.CoreV1().PersistentVolumeClaims(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		persistentVolumeClaim := newPVC()
		persistentVolumeClaim.Name = name
		persistentVolumeClaim.Spec.VolumeName = fakeObj.Spec.VolumeName
		persistentVolumeClaim.Spec.Resources = fakeObj.Spec.Resources
		persistentVolumeClaim.Spec.StorageClassName = fakeObj.Spec.StorageClassName
		exist, err := clientInterface.Create(context.TODO(), persistentVolumeClaim, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutPodSecurityPolicies(fakeObjs map[SystemRolloutCRName]*policyv1beta1.PodSecurityPolicy, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Policy().V1beta1().PodSecurityPolicies().Informer().GetIndexer()

	clientInterface := client.PolicyV1beta1().PodSecurityPolicies()

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newPodSecurityPolicy(fakeObj.Spec), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutRecurringJobs(fakeObjs map[SystemRolloutCRName]*longhorn.RecurringJob, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().RecurringJobs().Informer().GetIndexer()

	clientInterface := client.LonghornV1beta2().RecurringJobs(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newRecurringJob(name, fakeObj.Spec), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutRoles(fakeObjs map[SystemRolloutCRName]*rbacv1.Role, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Rbac().V1().Roles().Informer().GetIndexer()

	clientInterface := client.RbacV1().Roles(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newRole(name, fakeObj.Rules), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutRoleBindings(fakeObjs map[SystemRolloutCRName]*rbacv1.RoleBinding, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Rbac().V1().RoleBindings().Informer().GetIndexer()

	clientInterface := client.RbacV1().RoleBindings(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newRoleBinding(name, fakeObj.Subjects), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
		continue
	}
}

func fakeSystemRolloutVolumes(fakeObjs map[SystemRolloutCRName]*longhorn.Volume, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()

	clientInterface := client.LonghornV1beta2().Volumes(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		volume := newVolume(name, fakeObj.Spec.NumberOfReplicas)
		volume.Status = fakeObj.Status
		exist, err := clientInterface.Create(context.TODO(), volume, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutBackingImages(fakeObjs map[SystemRolloutCRName]*longhorn.BackingImage, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer()

	clientInterface := client.LonghornV1beta2().BackingImages(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		backingImage := newBackingIamge(name, fakeObj.Spec.SourceType)
		exist, err := clientInterface.Create(context.TODO(), backingImage, metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutManagerPod(c *C, informerFactory informers.SharedInformerFactory, kubeClient *fake.Clientset) {
	pIndexer := informerFactory.Core().V1().Pods().Informer().GetIndexer()
	pod := newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, nil)
	pod, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), pod, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	err = pIndexer.Add(pod)
	c.Assert(err, IsNil)
}

func fakeSystemRolloutBackupTargetDefault(c *C, informerFactory lhinformers.SharedInformerFactory, lhClient *lhfake.Clientset) {
	btIndexer := informerFactory.Longhorn().V1beta2().BackupTargets().Informer().GetIndexer()
	backupTarget := &longhorn.BackupTarget{
		ObjectMeta: metav1.ObjectMeta{
			Name:      types.DefaultBackupTargetName,
			Namespace: TestNamespace,
		},
		Spec: longhorn.BackupTargetSpec{
			BackupTargetURL:  "",
			CredentialSecret: "",
		},
	}
	backupTarget, err := lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Create(context.TODO(), backupTarget, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	err = btIndexer.Add(backupTarget)
	c.Assert(err, IsNil)
}

func fakeSystemRolloutServices(fakeObjs map[SystemRolloutCRName]*corev1.Service, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Core().V1().Services().Informer().GetIndexer()

	clientInterface := client.CoreV1().Services(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newService(name, fakeObj.Spec.Ports), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutServiceAccounts(fakeObjs map[SystemRolloutCRName]*corev1.ServiceAccount, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Core().V1().ServiceAccounts().Informer().GetIndexer()

	clientInterface := client.CoreV1().ServiceAccounts(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newServiceAccount(name), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutSettings(fakeObjs map[SystemRolloutCRName]*longhorn.Setting, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()

	clientInterface := client.LonghornV1beta2().Settings(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, fakeObj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		value := string(fakeObj.Value)

		exist, err := clientInterface.Create(context.TODO(), initSettingsNameValue(name, value), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutSettingDefaultEngineImage(c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	setting := map[SystemRolloutCRName]*longhorn.Setting{
		SystemRolloutCRName(types.SettingNameDefaultEngineImage): {Value: TestEngineImage},
	}
	fakeSystemRolloutSettings(setting, c, informerFactory, client)
}

func fakeSystemRolloutStorageClasses(fakeObjs map[SystemRolloutCRName]*storagev1.StorageClass, c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	indexer := informerFactory.Storage().V1().StorageClasses().Informer().GetIndexer()

	clientInterface := client.StorageV1().StorageClasses()

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	for k, obj := range fakeObjs {
		name := string(k)
		if strings.HasSuffix(name, TestIgnoreSuffix) {
			continue
		}

		exist, err := clientInterface.Create(context.TODO(), newStorageClass(name, obj.Provisioner), metav1.CreateOptions{})
		c.Assert(err, IsNil)

		err = indexer.Add(exist)
		c.Assert(err, IsNil)
	}
}

func fakeSystemRolloutStorageClassesDefault(c *C, informerFactory informers.SharedInformerFactory, client *fake.Clientset) {
	storageClasses := map[SystemRolloutCRName]*storagev1.StorageClass{
		SystemRolloutCRName(TestStorageClassName): {
			ObjectMeta: metav1.ObjectMeta{
				Name: TestStorageClassName,
			},
		},
	}
	fakeSystemRolloutStorageClasses(storageClasses, c, informerFactory, client)
}

type FakeSystemBackupTargetClient struct {
	Image      string
	URL        string
	Credential map[string]string

	name    string
	version string
}

func (c *FakeSystemBackupTargetClient) UploadSystemBackup(name, localFile, longhornVersion, longhornGitCommit, managerImage, engineImage string) (string, error) {
	switch name {
	case TestSystemBackupNameUploadFailed, TestSystemRestoreNameUploadFailed:
		return "", fmt.Errorf(name)
	case TestSystemBackupNameUploadExceedTimeout, TestSystemRestoreNameUploadExceedTimeout:
		time.Sleep(datastore.SystemBackupTimeout * 2)
	}

	return "", nil
}

func (c *FakeSystemBackupTargetClient) DownloadSystemBackup(name, version, downloadPath string) error {
	return nil
}

func (c *FakeSystemBackupTargetClient) ListSystemBackup() (systembackupstore.SystemBackups, error) {
	if c.name == TestSystemBackupNameListFailed {
		return nil, fmt.Errorf("failed to list")
	}
	return systembackupstore.SystemBackups{
		systembackupstore.Name(c.name): systembackupstore.URI(fmt.Sprintf(TestSystemBackupURIFmt, c.version, c.name)),
	}, nil
}

func (c *FakeSystemBackupTargetClient) GetSystemBackupConfig(name, version string) (*systembackupstore.Config, error) {
	switch name {
	case TestSystemBackupNameUploadFailed, TestSystemBackupNameUploadExceedTimeout, TestSystemRestoreNameUploadFailed, TestSystemRestoreNameUploadExceedTimeout:
		return nil, fmt.Errorf("cannot find %v", name)
	case TestSystemBackupNameGetConfigFailed, TestSystemRestoreNameGetConfigFailed:
		return nil, fmt.Errorf("failed to get config for %v", name)
	}

	now, err := time.Parse(time.RFC3339, TestTimeNow)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse time in test")
	}

	return &systembackupstore.Config{
		Name:              TestSystemRestoreName,
		LonghornVersion:   TestSystemBackupLonghornVersion,
		LonghornGitCommit: TestSystemBackupGitCommit,
		BackupTargetURL:   TestBackupTarget,
		ManagerImage:      TestManagerImage,
		EngineImage:       TestEngineImage,
		CreatedAt:         now,
	}, nil
}

func (c *FakeSystemBackupTargetClient) DeleteSystemBackup(systemBackup *longhorn.SystemBackup) (string, error) {
	return "", nil
}
