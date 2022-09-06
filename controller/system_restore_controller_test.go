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

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	systembackupstore "github.com/longhorn/backupstore/systembackup"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"

	. "gopkg.in/check.v1"
)

const (
	TestSystemBackupGitCommit       = "12345abcd"
	TestSystemBackupLonghornVersion = "v1.4.0"
	TestSystemBackupName            = "system-backup-0"
	TestSystemBackupURIFmt          = "backupstore/system-backups/%v/%v"

	TestSystemRestoreName                    = "system-retore-0"
	TestSystemRestoreNameGetConfigFailed     = "system-retore-get-config-failed"
	TestSystemRestoreNameUploadFailed        = "system-retore-upload-failed"
	TestSystemRestoreNameUploadExceedTimeout = "system-retore-upload-exceed-timeout"

	TestDiffSuffix   = "-diff"
	TestIgnoreSuffix = "-ignore"
)

type SystemRolloutCRName string
type SystemRolloutCRValue string

type SystemRestoreTestCase struct {
	controllerID string

	state      longhorn.SystemRestoreState
	notExist   bool
	isDeleting bool

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
	}

	for name, tc := range testCases {
		if tc.controllerID == "" {
			tc.controllerID = systemRestoreOwnerID
		}

		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformers.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())

		fakeSystemRolloutManagerPod(c, kubeInformerFactory, kubeClient)
		fakeSystemRolloutSettingDefaultEngineImage(c, lhInformerFactory, lhClient)
		fakeSystemRolloutBackupTargetDefault(c, lhInformerFactory, lhClient)
		fakeSystemBackup(TestSystemBackupName, systemRestoreOwnerID, "", false, longhorn.SystemBackupStateGenerating, c, lhInformerFactory, lhClient)

		extensionsClient := apiextensionsfake.NewSimpleClientset()

		systemRestoreController := newFakeSystemRestoreController(
			lhInformerFactory, kubeInformerFactory,
			lhClient, kubeClient, extensionsClient,
			tc.controllerID,
		)

		if !tc.notExist {
			if tc.state == "" {
				tc.state = longhorn.SystemRestoreStateNone
			}

			systemRestore := fakeSystemRestore(systemRestoreName, systemRestoreOwnerID, false, tc.isDeleting, tc.state, c, lhInformerFactory, lhClient, systemRestoreController.ds)
			if tc.isDeleting {
				err := systemRestoreController.CreateSystemRestoreJob(systemRestore, backupTargetClient)
				c.Assert(err, IsNil)
			}
		}

		err := systemRestoreController.reconcile(systemRestoreName, backupTargetClient)
		if tc.expectError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
		}

		job, err := kubeClient.BatchV1().Jobs(TestNamespace).Get(context.TODO(), systemRestoreName, metav1.GetOptions{})
		if tc.expectJobExist {
			c.Assert(err, IsNil)
			c.Assert(job.Spec.Template.Spec.Containers[0].Image, Equals, TestManagerImage)
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

func newFakeSystemRestoreController(
	lhInformerFactory lhinformers.SharedInformerFactory,
	kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset,
	kubeClient *fake.Clientset,
	extensionsClient *apiextensionsfake.Clientset,
	controllerID string) *SystemRestoreController {

	ds := datastore.NewDataStore(lhInformerFactory, lhClient, kubeInformerFactory, kubeClient, extensionsClient, TestNamespace)

	logger := logrus.StandardLogger()
	logrus.SetLevel(logrus.DebugLevel)

	c := NewSystemRestoreController(logger, ds, scheme.Scheme, kubeClient, TestNamespace, controllerID)
	c.eventRecorder = record.NewFakeRecorder(100)
	for index := range c.cacheSyncs {
		c.cacheSyncs[index] = alwaysReady
	}

	return c
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

		exist, err := clientInterface.Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil && apierrors.IsNotFound(err) {
			engineImage := newEngineImage(fakeObj.Spec.Image, fakeObj.Status.State)
			engineImage.Name = name
			exist, err := clientInterface.Create(context.TODO(), engineImage, metav1.CreateOptions{})
			c.Assert(err, IsNil)

			err = indexer.Add(exist)
			c.Assert(err, IsNil)
			continue
		}
		c.Assert(err, IsNil)

		if !reflect.DeepEqual(exist.Spec, fakeObj.Spec) {
			exist.Spec = fakeObj.Spec
			_, err := clientInterface.Update(context.TODO(), exist, metav1.UpdateOptions{})
			c.Assert(err, IsNil)
		}
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

		exist, err := clientInterface.Get(context.TODO(), name, metav1.GetOptions{})
		if err != nil && apierrors.IsNotFound(err) {
			exist, err := clientInterface.Create(context.TODO(), initSettingsNameValue(name, value), metav1.CreateOptions{})
			c.Assert(err, IsNil)

			err = indexer.Add(exist)
			c.Assert(err, IsNil)
			continue
		}
		c.Assert(err, IsNil)

		if exist.Value != value {
			exist.Value = value
			_, err := clientInterface.Update(context.TODO(), exist, metav1.UpdateOptions{})
			c.Assert(err, IsNil)
		}
	}
}

func fakeSystemRolloutSettingDefaultEngineImage(c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	setting := map[SystemRolloutCRName]*longhorn.Setting{
		SystemRolloutCRName(types.SettingNameDefaultEngineImage): {Value: TestEngineImage},
	}
	fakeSystemRolloutSettings(setting, c, informerFactory, client)
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
	case TestSystemRestoreNameUploadFailed:
		return "", fmt.Errorf(name)
	case TestSystemRestoreNameUploadExceedTimeout:
		time.Sleep(time.Duration(datastore.SystemBackupTimeout) * 2)
	}

	return "", nil
}

func (c *FakeSystemBackupTargetClient) DownloadSystemBackup(name, version, downloadPath string) error {
	return nil
}

func (c *FakeSystemBackupTargetClient) ListSystemBackup() (systembackupstore.SystemBackups, error) {
	return systembackupstore.SystemBackups{
		systembackupstore.Name(c.name): systembackupstore.URI(fmt.Sprintf(TestSystemBackupURIFmt, c.version, c.name)),
	}, nil
}

func (c *FakeSystemBackupTargetClient) GetSystemBackupConfig(name, version string) (*systembackupstore.Config, error) {
	switch name {
	case TestSystemRestoreNameUploadFailed, TestSystemRestoreNameUploadExceedTimeout:
		return nil, fmt.Errorf("cannot find %v", name)
	case TestSystemRestoreNameGetConfigFailed:
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
