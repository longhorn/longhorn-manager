package controller

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

type SystemBackupTestCase struct {
	state longhorn.SystemBackupState

	controllerID string

	notExist            bool
	isDeleting          bool
	isExistInRemote     bool
	systemBackupName    string
	systemBackupVersion string
	volumeBackupPolicy  longhorn.SystemBackupCreateVolumeBackupPolicy

	existPersistentVolumes map[SystemRolloutCRName]*corev1.PersistentVolume
	existVolumes           map[SystemRolloutCRName]*longhorn.Volume
	existBackingImages     map[SystemRolloutCRName]*longhorn.BackingImage
	existBackups           map[string]*longhorn.Backup
	existSnapshots         map[string]*longhorn.Snapshot

	expectError                 bool
	expectErrorConditionMessage string
	expectState                 longhorn.SystemBackupState
	expectRemove                bool
	expectNewVolumBackupCount   int
}

func (s *TestSuite) TestReconcileSystemBackup(c *C) {
	datastore.SkipListerCheck = true
	datastore.SystemBackupTimeout = 10 * time.Second
	datastore.VolumeBackupTimeout = 10 * time.Second

	rolloutOwnerID := TestNode1

	tempDirs := []string{}
	defer func() {
		for _, dir := range tempDirs {
			err := os.RemoveAll(dir)
			c.Assert(err, IsNil)
		}
	}()

	testCases := map[string]SystemBackupTestCase{
		"system backup create": {
			state:       longhorn.SystemBackupStateNone,
			expectState: longhorn.SystemBackupStateVolumeBackup,
		},
		"system backup create list backup failed": {
			systemBackupName: TestSystemBackupNameListFailed,
			state:            longhorn.SystemBackupStateNone,
			expectState:      longhorn.SystemBackupStateError,
		},
		"system backup create volume backup if-not-present": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "",
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
		},
		"system backup create volume backup if-not-present when backup exists": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "exists",
					},
				},
			},
			existBackups: map[string]*longhorn.Backup{
				"exists": {
					Status: longhorn.BackupStatus{
						State:        longhorn.BackupStateCompleted,
						SnapshotName: "exists",
						VolumeName:   TestVolumeName,
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 0,
		},
		"system backup create volume backup if-not-present when snapshot creationTime is not set": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "exists",
					},
				},
			},
			existBackups: map[string]*longhorn.Backup{
				"exists": {
					Status: longhorn.BackupStatus{
						State:        longhorn.BackupStateCompleted,
						SnapshotName: "exists",
						VolumeName:   TestVolumeName,
					},
				},
			},
			existSnapshots: map[string]*longhorn.Snapshot{
				"exists": {
					ObjectMeta: metav1.ObjectMeta{Name: "exists"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						ReadyToUse: true,
						// CreationTime is not set
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
		},
		"system backup create volume backup if-not-present when snapshot creationTime is null": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyIfNotPresent,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "exists",
					},
				},
			},
			existBackups: map[string]*longhorn.Backup{
				"exists": {
					Status: longhorn.BackupStatus{
						State:        longhorn.BackupStateCompleted,
						SnapshotName: "exists",
						VolumeName:   TestVolumeName,
					},
				},
			},
			existSnapshots: map[string]*longhorn.Snapshot{
				"exists": {
					ObjectMeta: metav1.ObjectMeta{Name: "exists"},
					Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
					Status: longhorn.SnapshotStatus{
						ReadyToUse:   true,
						CreationTime: "null",
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
		},
		"system backup create volume backup always": {
			state:              longhorn.SystemBackupStateVolumeBackup,
			volumeBackupPolicy: longhorn.SystemBackupCreateVolumeBackupPolicyAlways,
			existVolumes: map[SystemRolloutCRName]*longhorn.Volume{
				SystemRolloutCRName(TestVolumeName): {
					Status: longhorn.VolumeStatus{
						LastBackup: "exists",
					},
				},
			},
			existBackups: map[string]*longhorn.Backup{
				"exists": {
					Status: longhorn.BackupStatus{
						State:        longhorn.BackupStateCompleted,
						SnapshotName: "exists",
						VolumeName:   TestVolumeName,
					},
				},
			},
			expectState:               longhorn.SystemBackupStateBackingImageBackup,
			expectNewVolumBackupCount: 1,
		},
		"system backup create backingimage backup": {
			state:       longhorn.SystemBackupStateBackingImageBackup,
			expectState: longhorn.SystemBackupStateGenerating,
		},
		"system backup generate": {
			state:       longhorn.SystemBackupStateGenerating,
			expectState: longhorn.SystemBackupStateUploading,
		},
		"system backup upload": {
			state:       longhorn.SystemBackupStateUploading,
			expectState: longhorn.SystemBackupStateReady,
		},
		"system backup upload file exceed timeout": {
			systemBackupName: TestSystemBackupNameUploadExceedTimeout,
			state:            longhorn.SystemBackupStateUploading,
			expectState:      longhorn.SystemBackupStateReady,
		},
		"system backup upload file failed": {
			systemBackupName:            TestSystemBackupNameUploadFailed,
			state:                       longhorn.SystemBackupStateUploading,
			expectState:                 longhorn.SystemBackupStateError,
			expectErrorConditionMessage: fmt.Sprintf("%v:", SystemBackupErrTimeoutUpload),
		},
		"system backup upload get config failed": {
			systemBackupName:            TestSystemBackupNameGetConfigFailed,
			state:                       longhorn.SystemBackupStateUploading,
			expectState:                 longhorn.SystemBackupStateError,
			expectErrorConditionMessage: fmt.Sprintf("%v: %v:", SystemBackupErrTimeoutUpload, SystemBackupErrGetConfig),
		},
		"system backup ready": {
			state: longhorn.SystemBackupStateReady,
		},
		"system backup error": {
			state: longhorn.SystemBackupStateError,
		},
		"system backup with deletion timestamp": {
			state:       longhorn.SystemBackupStateReady,
			isDeleting:  true,
			expectState: longhorn.SystemBackupStateDeleting,
		},
		"system backup delete": {
			state:        longhorn.SystemBackupStateDeleting,
			isDeleting:   true,
			expectRemove: true,
		},
		"system backup from backup target": {
			state:               longhorn.SystemBackupStateNone,
			expectState:         longhorn.SystemBackupStateSyncing,
			systemBackupVersion: TestSystemBackupLonghornVersion,
		},
		"system backup syncing": {
			state:               longhorn.SystemBackupStateSyncing,
			expectState:         longhorn.SystemBackupStateReady,
			systemBackupVersion: TestSystemBackupLonghornVersion,
		},
		"system backup exist in remote": {
			state:           longhorn.SystemBackupStateNone,
			expectState:     longhorn.SystemBackupStateNone,
			isExistInRemote: true,
		},
		"system backup PersistentVolume source not from CSI": {
			state:       longhorn.SystemBackupStateGenerating,
			expectState: longhorn.SystemBackupStateUploading,

			existPersistentVolumes: map[SystemRolloutCRName]*corev1.PersistentVolume{
				SystemRolloutCRName(TestPVName): {
					Spec: corev1.PersistentVolumeSpec{
						ClaimRef: &corev1.ObjectReference{
							Name:      TestPVCName,
							Namespace: TestNamespace,
						},
						StorageClassName: TestStorageClassName,
						PersistentVolumeSource: corev1.PersistentVolumeSource{
							HostPath: &corev1.HostPathVolumeSource{
								Path: "/fake",
							},
						},
					},
				},
			},
		},
	}

	for name, tc := range testCases {
		if tc.systemBackupName == "" {
			tc.systemBackupName = TestSystemBackupName
		}

		if tc.expectState == "" {
			tc.expectState = tc.state
		}

		if tc.controllerID == "" {
			tc.controllerID = rolloutOwnerID
		}

		backupTargetClient := &FakeSystemBackupTargetClient{
			name: tc.systemBackupName,
		}
		if tc.isExistInRemote {
			backupTargetClient.version = TestSystemBackupLonghornVersion
		}

		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		fakeSystemRolloutNamespace(c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutSettingDefaultEngineImage(c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutBackupTargetDefault(c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutStorageClassesDefault(c, informerFactories.KubeInformerFactory, kubeClient)

		fakeSystemRolloutVolumes(tc.existVolumes, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutBackups(tc.existBackups, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutBackingImages(tc.existBackingImages, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutPersistentVolumes(tc.existPersistentVolumes, c, informerFactories.KubeInformerFactory, kubeClient)

		systemBackupController, err := newFakeSystemBackupController(lhClient, kubeClient, extensionsClient, informerFactories, tc.controllerID)
		c.Assert(err, IsNil)

		systemBackup := fakeSystemBackup(tc.systemBackupName, rolloutOwnerID, tc.systemBackupVersion, tc.isDeleting, tc.volumeBackupPolicy, tc.state, c, informerFactories.LhInformerFactory, lhClient)
		if tc.notExist {
			systemBackup = fakeSystemBackup("none", rolloutOwnerID, tc.systemBackupVersion, tc.isDeleting, tc.volumeBackupPolicy, tc.state, c, informerFactories.LhInformerFactory, lhClient)
		}

		systemBackupTempDir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("*-%v", TestSystemBackupName))
		c.Assert(err, IsNil)
		tempDirs = append(tempDirs, systemBackupTempDir)

		archievePath := filepath.Join(systemBackupTempDir, tc.systemBackupName+".zip")
		tempDir := filepath.Join(systemBackupTempDir, tc.systemBackupName)

		switch systemBackup.Status.State {
		case longhorn.SystemBackupStateVolumeBackup:
			if tc.existBackups != nil {
				existBackupSnapshots := make(map[string]*longhorn.Snapshot)
				if tc.existSnapshots == nil {
					for _, backup := range tc.existBackups {
						existBackupSnapshots[backup.Status.SnapshotName] = &longhorn.Snapshot{
							ObjectMeta: metav1.ObjectMeta{Name: backup.Status.SnapshotName},
							Spec:       longhorn.SnapshotSpec{Volume: TestVolumeName},
							Status: longhorn.SnapshotStatus{
								ReadyToUse:   true,
								CreationTime: metav1.Now().Format(time.RFC3339),
							},
						}
					}
				} else {
					existBackupSnapshots = tc.existSnapshots
				}

				for _, existBackupSnap := range existBackupSnapshots {
					fakeSystemRolloutSnapshot(existBackupSnap, c, informerFactories.LhInformerFactory, lhClient)
				}
			}
			backups, _ := systemBackupController.BackupVolumes(systemBackup)

			for _, backup := range backups {
				backup.Status.State = longhorn.BackupStateCompleted
				backup.Status.SnapshotName = backup.Spec.SnapshotName
				snapshot, err := lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(context.TODO(), backup.Status.SnapshotName, metav1.GetOptions{})
				c.Assert(err, IsNil)
				tc.existVolumes[SystemRolloutCRName(snapshot.Spec.Volume)].Status.LastBackup = backup.Name
				fakeSystemRolloutSnapshot(snapshot, c, informerFactories.LhInformerFactory, lhClient)
			}
			fakeSystemRolloutVolumes(tc.existVolumes, c, informerFactories.LhInformerFactory, lhClient)
			fakeSystemRolloutBackups(backups, c, informerFactories.LhInformerFactory, lhClient)
			err = systemBackupController.WaitForVolumeBackupToComplete(backups, systemBackup)
			c.Assert(err, IsNil)

		case longhorn.SystemBackupStateBackingImageBackup:
			backupBackingImages, _ := systemBackupController.BackupBackingImage()
			for _, backupBackingImage := range backupBackingImages {
				backupBackingImage.Status.State = longhorn.BackupStateCompleted
			}
			fakeSystemRolloutBackupBackingImages(backupBackingImages, c, informerFactories.LhInformerFactory, lhClient)
			systemBackupController.WaitForBackingImageBackupToComplete(backupBackingImages, systemBackup)

		case longhorn.SystemBackupStateGenerating:
			systemBackupController.GenerateSystemBackup(systemBackup, archievePath, tempDir)

		case longhorn.SystemBackupStateUploading:
			systemBackupController.UploadSystemBackup(systemBackup, archievePath, tempDir, backupTargetClient)

		default:
			defaultBackupTarget, err := lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
			c.Assert(err, IsNil)
			err = systemBackupController.reconcile(tc.systemBackupName, backupTargetClient, defaultBackupTarget)
			if tc.expectError {
				c.Assert(err, NotNil)
			} else {
				c.Assert(err, IsNil)
			}
		}

		systemBackup, err = lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), tc.systemBackupName, metav1.GetOptions{})
		if tc.notExist {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(systemBackup.Status.State, Equals, tc.expectState)
		}

		checkFinalizer := !util.FinalizerExists(longhornFinalizerKey, systemBackup) == (tc.expectRemove || tc.notExist)
		c.Assert(checkFinalizer, Equals, true)

		if tc.expectErrorConditionMessage != "" {
			errCondition := types.GetCondition(systemBackup.Status.Conditions, longhorn.SystemBackupConditionTypeError)
			c.Assert(errCondition.Status, Equals, longhorn.ConditionStatusTrue)
			c.Assert(strings.HasPrefix(errCondition.Message, tc.expectErrorConditionMessage), Equals, true)
		}

		if tc.isExistInRemote {
			c.Assert(systemBackup.Labels[types.GetVersionLabelKey()], NotNil)
		}

		volumeBackups, err := lhClient.LonghornV1beta2().Backups(TestNamespace).List(context.TODO(), metav1.ListOptions{})
		c.Assert(err, IsNil)
		c.Assert(len(volumeBackups.Items), Equals, tc.expectNewVolumBackupCount)
	}
}

func newFakeSystemBackupController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*SystemBackupController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	logger := logrus.StandardLogger()
	logrus.SetLevel(logrus.DebugLevel)

	c, err := NewSystemBackupController(logger, ds, scheme.Scheme, kubeClient, TestNamespace, controllerID, TestManagerImage)
	if err != nil {
		return nil, err
	}
	c.eventRecorder = record.NewFakeRecorder(100)
	for index := range c.cacheSyncs {
		c.cacheSyncs[index] = alwaysReady
	}

	return c, nil
}

func fakeSystemBackup(name, currentOwnerID, longhornVersion string, isDeleting bool,
	volumeBackupPolicy longhorn.SystemBackupCreateVolumeBackupPolicy,
	state longhorn.SystemBackupState, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) *longhorn.SystemBackup {
	if volumeBackupPolicy == "" {
		volumeBackupPolicy = longhorn.SystemBackupCreateVolumeBackupPolicyDisabled
	}
	systemBackup := newSystemBackup(name, currentOwnerID, longhornVersion, volumeBackupPolicy, state)

	err := util.AddFinalizer(longhornFinalizerKey, systemBackup)
	c.Assert(err, IsNil)

	if isDeleting {
		now := metav1.NewTime(time.Now())
		systemBackup.DeletionTimestamp = &now
	}

	systemBackup, err = client.LonghornV1beta2().SystemBackups(TestNamespace).Create(context.TODO(), systemBackup, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	indexer := informerFactory.Longhorn().V1beta2().SystemBackups().Informer().GetIndexer()
	err = indexer.Add(systemBackup)
	c.Assert(err, IsNil)

	return systemBackup
}

func fakeSystemRolloutNamespace(c *C, informerFactory informers.SharedInformerFactory, kubeClient *fake.Clientset) {
	nsIndexer := informerFactory.Core().V1().Namespaces().Informer().GetIndexer()
	namespace, err := kubeClient.CoreV1().Namespaces().Create(context.TODO(), &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: TestNamespace,
		},
	}, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	err = nsIndexer.Add(namespace)
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestSystemBackupCreatedChildrenCarrySystemBackupLabel(c *C) {
	datastore.SkipListerCheck = true
	datastore.SystemBackupTimeout = 10 * time.Second

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	fakeSystemRolloutNamespace(c, informerFactories.KubeInformerFactory, kubeClient)
	fakeSystemRolloutSettingDefaultEngineImage(c, informerFactories.LhInformerFactory, lhClient)
	fakeSystemRolloutBackupTargetDefault(c, informerFactories.LhInformerFactory, lhClient)
	fakeSystemRolloutStorageClassesDefault(c, informerFactories.KubeInformerFactory, kubeClient)

	fakeSystemRolloutVolumes(map[SystemRolloutCRName]*longhorn.Volume{
		SystemRolloutCRName(TestVolumeName): {
			Status: longhorn.VolumeStatus{},
		},
	}, c, informerFactories.LhInformerFactory, lhClient)

	systemBackupController, err := newFakeSystemBackupController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
	c.Assert(err, IsNil)

	systemBackup := fakeSystemBackup(
		TestSystemBackupName,
		TestNode1,
		"",
		false,
		longhorn.SystemBackupCreateVolumeBackupPolicyAlways,
		longhorn.SystemBackupStateVolumeBackup,
		c,
		informerFactories.LhInformerFactory,
		lhClient,
	)

	backups, err := systemBackupController.BackupVolumes(systemBackup)
	c.Assert(err, IsNil)
	c.Assert(len(backups), Equals, 1)

	for _, backup := range backups {
		c.Assert(backup.Labels[types.GetSystemBackupLabelKey()], Equals, systemBackup.Name)

		snapshot, err := lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(context.TODO(), backup.Spec.SnapshotName, metav1.GetOptions{})
		c.Assert(err, IsNil)
		c.Assert(snapshot.Labels[types.GetSystemBackupLabelKey()], Equals, systemBackup.Name)
	}
}

func (s *TestSuite) TestSystemBackupWaitForVolumeBackupToCompleteAutoCleanup(c *C) {
	datastore.SkipListerCheck = true
	datastore.SystemBackupTimeout = 10 * time.Second
	datastore.VolumeBackupTimeout = 10 * time.Second

	testCases := map[string]struct {
		autoCleanupEnabled   string
		expectSnapshotDelete bool
	}{
		"auto cleanup enabled": {
			autoCleanupEnabled:   "true",
			expectSnapshotDelete: true,
		},
		"auto cleanup disabled": {
			autoCleanupEnabled:   "false",
			expectSnapshotDelete: false,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		fakeSystemRolloutNamespace(c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutSettingDefaultEngineImage(c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutBackupTargetDefault(c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutStorageClassesDefault(c, informerFactories.KubeInformerFactory, kubeClient)

		fakeSystemRolloutSettings(map[SystemRolloutCRName]*longhorn.Setting{
			SystemRolloutCRName(types.SettingNameAutoCleanupRecurringJobBackupSnapshot): {
				Value: tc.autoCleanupEnabled,
			},
		}, c, informerFactories.LhInformerFactory, lhClient)

		existVolumes := map[SystemRolloutCRName]*longhorn.Volume{
			SystemRolloutCRName(TestVolumeName): {
				Status: longhorn.VolumeStatus{},
			},
		}
		fakeSystemRolloutVolumes(existVolumes, c, informerFactories.LhInformerFactory, lhClient)

		systemBackupController, err := newFakeSystemBackupController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
		c.Assert(err, IsNil)

		systemBackup := fakeSystemBackup(
			TestSystemBackupName,
			TestNode1,
			"",
			false,
			longhorn.SystemBackupCreateVolumeBackupPolicyAlways,
			longhorn.SystemBackupStateVolumeBackup,
			c,
			informerFactories.LhInformerFactory,
			lhClient,
		)

		backups, err := systemBackupController.BackupVolumes(systemBackup)
		c.Assert(err, IsNil)
		c.Assert(len(backups), Equals, 1)

		for _, backup := range backups {
			backup.Status.State = longhorn.BackupStateCompleted
			backup.Status.SnapshotName = backup.Spec.SnapshotName

			snapshot, err := lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(context.TODO(), backup.Status.SnapshotName, metav1.GetOptions{})
			c.Assert(err, IsNil)

			snapshot.Spec.Volume = TestVolumeName
			snapshot.Status.ReadyToUse = true
			snapshot.Status.CreationTime = metav1.Now().Format(time.RFC3339)
			fakeSystemRolloutSnapshotWithMetadata(snapshot, c, informerFactories.LhInformerFactory, lhClient)

			existVolumes[SystemRolloutCRName(TestVolumeName)].Status.LastBackup = backup.Name
		}

		fakeSystemRolloutVolumes(existVolumes, c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutBackups(backups, c, informerFactories.LhInformerFactory, lhClient)

		err = systemBackupController.WaitForVolumeBackupToComplete(backups, systemBackup)
		c.Assert(err, IsNil)

		for _, backup := range backups {
			_, err := lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(context.TODO(), backup.Spec.SnapshotName, metav1.GetOptions{})
			if tc.expectSnapshotDelete {
				c.Assert(apierrors.IsNotFound(err), Equals, true)
			} else {
				c.Assert(err, IsNil)
			}
		}
	}
}

func (s *TestSuite) TestSystemBackupDeletingStateCleansAssociatedBackupAndSnapshotCRs(c *C) {
	datastore.SkipListerCheck = true

	testCases := map[string]struct {
		reconcileCount int
		expectRemove   bool
	}{
		"first reconcile deletes children and removes finalizer": {
			reconcileCount: 1,
			expectRemove:   true,
		},
		"second reconcile keeps finalizer removed": {
			reconcileCount: 2,
			expectRemove:   true,
		},
	}

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		fakeSystemRolloutNamespace(c, informerFactories.KubeInformerFactory, kubeClient)
		fakeSystemRolloutSettingDefaultEngineImage(c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutBackupTargetDefault(c, informerFactories.LhInformerFactory, lhClient)
		fakeSystemRolloutStorageClassesDefault(c, informerFactories.KubeInformerFactory, kubeClient)

		systemBackupController, err := newFakeSystemBackupController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
		c.Assert(err, IsNil)

		systemBackup := fakeSystemBackup(
			TestSystemBackupName,
			TestNode1,
			"",
			true,
			longhorn.SystemBackupCreateVolumeBackupPolicyAlways,
			longhorn.SystemBackupStateDeleting,
			c,
			informerFactories.LhInformerFactory,
			lhClient,
		)

		snapshot := newSnapshot("system-backup-snapshot-1")
		snapshot.Labels = map[string]string{
			types.GetSystemBackupLabelKey(): systemBackup.Name,
		}
		snapshot.Spec.Volume = TestVolumeName
		snapshot.Status.ReadyToUse = true
		snapshot.Status.CreationTime = metav1.Now().Format(time.RFC3339)
		fakeSystemRolloutSnapshotWithMetadata(snapshot, c, informerFactories.LhInformerFactory, lhClient)

		backup := newBackup("system-backup-backup-1")
		backup.Labels = map[string]string{
			types.GetSystemBackupLabelKey(): systemBackup.Name,
		}
		backup.Spec.SnapshotName = snapshot.Name
		backup.Status.SnapshotName = snapshot.Name
		backup.Status.VolumeName = TestVolumeName
		backup.Status.State = longhorn.BackupStateCompleted
		fakeSystemRolloutBackupWithMetadata(backup, c, informerFactories.LhInformerFactory, lhClient)

		defaultBackupTarget, err := lhClient.LonghornV1beta2().BackupTargets(TestNamespace).Get(context.TODO(), types.DefaultBackupTargetName, metav1.GetOptions{})
		c.Assert(err, IsNil)

		backupTargetClient := &FakeSystemBackupTargetClient{name: systemBackup.Name}

		for i := 0; i < tc.reconcileCount; i++ {
			err = systemBackupController.reconcile(systemBackup.Name, backupTargetClient, defaultBackupTarget)
			c.Assert(err, IsNil)

			// In this unit test we don't run Backup/Snapshot controllers that remove child
			// finalizers, so simulate child resource cleanup before the second reconcile.
			if i == 0 && tc.reconcileCount > 1 {
				err = lhClient.LonghornV1beta2().Backups(TestNamespace).Delete(context.TODO(), backup.Name, metav1.DeleteOptions{})
				if err != nil {
					c.Assert(apierrors.IsNotFound(err), Equals, true)
				}

				backupIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Backups().Informer().GetIndexer()
				_ = backupIndexer.Delete(backup)

				err = lhClient.LonghornV1beta2().Snapshots(TestNamespace).Delete(context.TODO(), snapshot.Name, metav1.DeleteOptions{})
				if err != nil {
					c.Assert(apierrors.IsNotFound(err), Equals, true)
				}

				snapshotIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer()
				_ = snapshotIndexer.Delete(snapshot)
			}
		}

		systemBackup, err = lhClient.LonghornV1beta2().SystemBackups(TestNamespace).Get(context.TODO(), systemBackup.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		finalizerExists := util.FinalizerExists(longhornFinalizerKey, systemBackup)
		c.Assert(finalizerExists, Equals, !tc.expectRemove)

		_, backupErr := lhClient.LonghornV1beta2().Backups(TestNamespace).Get(context.TODO(), backup.Name, metav1.GetOptions{})
		_, snapshotErr := lhClient.LonghornV1beta2().Snapshots(TestNamespace).Get(context.TODO(), snapshot.Name, metav1.GetOptions{})

		c.Assert(apierrors.IsNotFound(backupErr), Equals, true)
		c.Assert(apierrors.IsNotFound(snapshotErr), Equals, true)
	}
}

func fakeSystemRolloutBackupWithMetadata(fakeObj *longhorn.Backup, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().Backups().Informer().GetIndexer()
	clientInterface := client.LonghornV1beta2().Backups(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		if exist.Name != fakeObj.Name {
			continue
		}

		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	backup := newBackup(fakeObj.Name)
	backup.Labels = fakeObj.Labels
	backup.Spec = fakeObj.Spec
	backup.Status = fakeObj.Status

	exist, err := clientInterface.Create(context.TODO(), backup, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	err = indexer.Add(exist)
	c.Assert(err, IsNil)
}

func fakeSystemRolloutSnapshotWithMetadata(fakeObj *longhorn.Snapshot, c *C, informerFactory lhinformers.SharedInformerFactory, client *lhfake.Clientset) {
	indexer := informerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer()
	clientInterface := client.LonghornV1beta2().Snapshots(TestNamespace)

	exists, err := clientInterface.List(context.TODO(), metav1.ListOptions{})
	c.Assert(err, IsNil)

	for _, exist := range exists.Items {
		if exist.Name != fakeObj.Name {
			continue
		}

		exist, err := clientInterface.Get(context.TODO(), exist.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)

		err = clientInterface.Delete(context.TODO(), exist.Name, metav1.DeleteOptions{})
		c.Assert(err, IsNil)

		err = indexer.Delete(exist)
		c.Assert(err, IsNil)
	}

	snapshot := newSnapshot(fakeObj.Name)
	snapshot.Labels = fakeObj.Labels
	snapshot.Spec = fakeObj.Spec
	snapshot.Status = fakeObj.Status

	exist, err := clientInterface.Create(context.TODO(), snapshot, metav1.CreateOptions{})
	c.Assert(err, IsNil)

	err = indexer.Add(exist)
	c.Assert(err, IsNil)
}
