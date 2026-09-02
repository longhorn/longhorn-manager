package controller

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestPreferredDataEngineIPFamilySettingDefinition(t *testing.T) {
	definition, ok := types.GetSettingDefinition(types.SettingNamePreferredDataEngineIPFamily)
	require.True(t, ok)
	require.Equal(t, types.SettingCategoryDangerZone, definition.Category)
	require.Equal(t, types.SettingTypeString, definition.Type)
	require.Equal(t, types.DataEngineIPFamilyDefault, definition.Default)
	require.Equal(t, []any{types.DataEngineIPFamilyDefault, types.DataEngineIPFamilyIPv4, types.DataEngineIPFamilyIPv6}, definition.Choices)

	for _, value := range []string{types.DataEngineIPFamilyDefault, types.DataEngineIPFamilyIPv4, types.DataEngineIPFamilyIPv6} {
		require.NoError(t, types.ValidateSetting(string(types.SettingNamePreferredDataEngineIPFamily), value))
	}
	require.Error(t, types.ValidateSetting(string(types.SettingNamePreferredDataEngineIPFamily), "ipv3"))
}

func TestDataEngineIPFamilySettingAppliedFalseBeforeAttachedMutation(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()                   // nolint:staticcheck
	lhClient := lhfake.NewSimpleClientset()                   // nolint:staticcheck
	extensionClient := apiextensionsfake.NewSimpleClientset() // nolint:staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	instanceManagerIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()

	setting := newSetting(string(types.SettingNamePreferredDataEngineIPFamily), types.DataEngineIPFamilyIPv6)
	setting.Status.Applied = true
	createdSetting, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, settingIndexer.Add(createdSetting))

	volume := newVolume(TestVolumeName, 1)
	volume.Namespace = TestNamespace
	volume.Status.State = longhorn.VolumeStateAttached
	createdVolume, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), volume, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, volumeIndexer.Add(createdVolume))
	oldFamily := types.DataEngineIPFamilyIPv4
	im := newIPFamilyTestInstanceManager("im-a", longhorn.DataEngineTypeV1, &oldFamily, true)
	im.Namespace = TestNamespace
	createdIM, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, instanceManagerIndexer.Add(createdIM))

	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionClient, informerFactories)
	sc := &SettingController{
		baseController: newBaseController("longhorn-setting", logrus.StandardLogger()),
		ds:             ds,
	}

	err = sc.syncSetting(TestNamespace + "/" + string(types.SettingNamePreferredDataEngineIPFamily))
	require.Error(t, err)

	updated, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Get(context.TODO(), string(types.SettingNamePreferredDataEngineIPFamily), metav1.GetOptions{})
	require.NoError(t, err)
	require.False(t, updated.Status.Applied)
	require.Equal(t, types.DataEngineIPFamilyIPv6, updated.Value)
}

func TestPreferredDataEngineIPFamilySettingUsesStableCacheKey(t *testing.T) {
	setting := newSetting(string(types.SettingNamePreferredDataEngineIPFamily), types.DataEngineIPFamilyIPv4)
	setting.Namespace = TestNamespace
	key, err := cache.MetaNamespaceKeyFunc(setting)
	require.NoError(t, err)
	require.Equal(t, TestNamespace+"/"+string(types.SettingNamePreferredDataEngineIPFamily), key)
}

func newIPFamilySettingControllerFixture(t *testing.T, desired string, applied bool, managers ...*longhorn.InstanceManager) (*SettingController, *lhfake.Clientset) {
	t.Helper()

	kubeClient := fake.NewSimpleClientset()                   // nolint:staticcheck
	lhClient := lhfake.NewSimpleClientset()                   // nolint:staticcheck
	extensionClient := apiextensionsfake.NewSimpleClientset() // nolint:staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	instanceManagerIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()

	settings := []*longhorn.Setting{
		newSetting(string(types.SettingNamePreferredDataEngineIPFamily), desired),
		newSetting(string(types.SettingNameV1DataEngine), "true"),
		newSetting(string(types.SettingNameV2DataEngine), "false"),
	}
	settings[0].Status.Applied = applied
	for _, setting := range settings {
		created, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
		require.NoError(t, err)
		require.NoError(t, settingIndexer.Add(created))
	}
	for _, im := range managers {
		im.Namespace = TestNamespace
		created, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(context.TODO(), im, metav1.CreateOptions{})
		require.NoError(t, err)
		require.NoError(t, instanceManagerIndexer.Add(created))
	}

	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionClient, informerFactories)
	return &SettingController{
		baseController: newBaseController("longhorn-setting", logrus.StandardLogger()),
		ds:             ds,
	}, lhClient
}

func newIPFamilyTestInstanceManager(name string, dataEngine longhorn.DataEngineType, family *string, synced bool) *longhorn.InstanceManager {
	status := longhorn.InstanceManagerStatus{IPFamily: family}
	statusValue := longhorn.ConditionStatusFalse
	if synced {
		statusValue = longhorn.ConditionStatusTrue
	}
	status.Conditions = []longhorn.Condition{{
		Type:   longhorn.InstanceManagerConditionTypeSettingSynced,
		Status: statusValue,
	}}
	return &longhorn.InstanceManager{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: longhorn.InstanceManagerSpec{
			DataEngine: dataEngine,
		},
		Status: status,
	}
}

func TestPreferredDataEngineIPFamilyConvergenceRequiresInitializedSyncedManagers(t *testing.T) {
	defaultFamily := types.DataEngineIPFamilyDefault
	legacyEmpty := ""
	tests := []struct {
		name   string
		family *string
		synced bool
		want   bool
	}{
		{name: "nil family is not converged", family: nil, synced: true, want: false},
		{name: "default family is converged", family: &defaultFamily, synced: true, want: true},
		{name: "legacy empty family is converged as default", family: &legacyEmpty, synced: true, want: true},
		{name: "unsynced manager is not converged", family: &defaultFamily, synced: false, want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			im := newIPFamilyTestInstanceManager("im-a", longhorn.DataEngineTypeV1, tc.family, tc.synced)
			sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyDefault, false, im)
			converged, err := sc.isDataEngineIPFamilyConverged(types.DataEngineIPFamilyDefault)
			require.NoError(t, err)
			require.Equal(t, tc.want, converged)
		})
	}
}

func TestDataEngineIPFamilyConvergenceIgnoresDisabledDataEngine(t *testing.T) {
	family := types.DataEngineIPFamilyIPv6
	im := newIPFamilyTestInstanceManager("im-v2", longhorn.DataEngineTypeV2, &family, true)
	sc, _ := newIPFamilySettingControllerFixture(t, family, false, im)
	converged, err := sc.isDataEngineIPFamilyConverged(family)
	require.NoError(t, err)
	require.True(t, converged)
}

func TestDataEngineIPFamilyPersistsUnappliedBeforeWaitingForConvergence(t *testing.T) {
	oldFamily := types.DataEngineIPFamilyIPv4
	im := newIPFamilyTestInstanceManager("im-a", longhorn.DataEngineTypeV1, &oldFamily, true)
	sc, lhClient := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, true, im)

	err := sc.syncDataEngineIPFamily()
	require.Error(t, err)
	require.Contains(t, err.Error(), "waiting for all Longhorn components to converge")

	updated, getErr := lhClient.LonghornV1beta2().Settings(TestNamespace).Get(context.TODO(), string(types.SettingNamePreferredDataEngineIPFamily), metav1.GetOptions{})
	require.NoError(t, getErr)
	require.False(t, updated.Status.Applied)
}

func TestBackingImageFamilyConvergenceRemainsIndependent(t *testing.T) {
	sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bim-a",
			Namespace: TestNamespace,
			Labels:    types.GetBackingImageManagerLabels("", ""),
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: BackingImageManagerPodContainerName,
			Args: []string{"daemon", "--ip-family", types.DataEngineIPFamilyIPv4},
		}}},
	}
	bim := &longhorn.BackingImageManager{ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: TestNamespace}}
	require.NoError(t, sc.ds.BackingImageManagerInformer.GetStore().Add(bim))
	require.NoError(t, sc.ds.PodInformer.GetStore().Add(pod))

	converged, err := sc.isBackingImageIPFamilyConverged(types.DataEngineIPFamilyIPv6)
	require.NoError(t, err)
	require.False(t, converged)

	pod.Spec.Containers[0].Args = []string{"daemon", "--ip-family", types.DataEngineIPFamilyIPv6}
	require.NoError(t, sc.ds.PodInformer.GetStore().Update(pod))
	converged, err = sc.isBackingImageIPFamilyConverged(types.DataEngineIPFamilyIPv6)
	require.NoError(t, err)
	require.True(t, converged)
}

func TestDataEngineIPFamilySettingAppliesAfterFullConvergence(t *testing.T) {
	family := types.DataEngineIPFamilyIPv6
	im := newIPFamilyTestInstanceManager("im-a", longhorn.DataEngineTypeV1, &family, true)
	sc, lhClient := newIPFamilySettingControllerFixture(t, family, false, im)

	err := sc.syncSetting(TestNamespace + "/" + string(types.SettingNamePreferredDataEngineIPFamily))
	require.NoError(t, err)

	updated, getErr := lhClient.LonghornV1beta2().Settings(TestNamespace).Get(context.TODO(), string(types.SettingNamePreferredDataEngineIPFamily), metav1.GetOptions{})
	require.NoError(t, getErr)
	require.True(t, updated.Status.Applied)
}

func TestBackingImageFamilyConvergenceRequiresReplacementPods(t *testing.T) {
	t.Run("missing BIM pod", func(t *testing.T) {
		sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false)
		bim := &longhorn.BackingImageManager{ObjectMeta: metav1.ObjectMeta{Name: "bim-a", Namespace: TestNamespace}}
		require.NoError(t, sc.ds.BackingImageManagerInformer.GetStore().Add(bim))

		converged, err := sc.isBackingImageIPFamilyConverged(types.DataEngineIPFamilyIPv6)
		require.NoError(t, err)
		require.False(t, converged)
	})

	t.Run("missing BIDS pod", func(t *testing.T) {
		sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false)
		bids := &longhorn.BackingImageDataSource{ObjectMeta: metav1.ObjectMeta{Name: "bids-a", Namespace: TestNamespace}}
		require.NoError(t, sc.ds.BackingImageDataSourceInformer.GetStore().Add(bids))

		converged, err := sc.isBackingImageIPFamilyConverged(types.DataEngineIPFamilyIPv6)
		require.NoError(t, err)
		require.False(t, converged)
	})

	t.Run("deleting BIM pod", func(t *testing.T) {
		sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false)
		bim := &longhorn.BackingImageManager{ObjectMeta: metav1.ObjectMeta{Name: "bim-a", Namespace: TestNamespace}}
		require.NoError(t, sc.ds.BackingImageManagerInformer.GetStore().Add(bim))
		deletionTime := metav1.Now()
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:              "bim-a",
				Namespace:         TestNamespace,
				Labels:            types.GetBackingImageManagerLabels("", ""),
				DeletionTimestamp: &deletionTime,
			},
		}
		require.NoError(t, sc.ds.PodInformer.GetStore().Add(pod))

		converged, err := sc.isBackingImageIPFamilyConverged(types.DataEngineIPFamilyIPv6)
		require.NoError(t, err)
		require.False(t, converged)
	})
}

func TestDataEngineIPFamilyReplacementEventsEnqueueSetting(t *testing.T) {
	sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false)
	events := []interface{}{
		&longhorn.InstanceManager{},
		&longhorn.BackingImageManager{},
		&longhorn.BackingImageDataSource{},
	}
	for _, event := range events {
		sc.enqueueDataEngineIPFamilySetting(event)
	}

	require.Equal(t, 1, sc.queue.Len())
	key, shutdown := sc.queue.Get()
	require.False(t, shutdown)
	require.Equal(t, sc.namespace+"/"+string(types.SettingNamePreferredDataEngineIPFamily), key)
	sc.queue.Done(key)
}

func TestDataEngineIPFamilyAlreadyConvergedKeepsAppliedWithAttachedVolume(t *testing.T) {
	family := types.DataEngineIPFamilyIPv6
	im := newIPFamilyTestInstanceManager("im-a", longhorn.DataEngineTypeV1, &family, true)
	sc, lhClient := newIPFamilySettingControllerFixture(t, family, true, im)
	volume := newVolume(TestVolumeName, 1)
	volume.Namespace = TestNamespace
	volume.Status.State = longhorn.VolumeStateAttached
	require.NoError(t, sc.ds.VolumeInformer.GetStore().Add(volume))

	require.NoError(t, sc.syncDataEngineIPFamily())
	updated, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Get(
		context.TODO(), string(types.SettingNamePreferredDataEngineIPFamily), metav1.GetOptions{})
	require.NoError(t, err)
	require.True(t, updated.Status.Applied)
}

func TestFileTransferredBIDSWithoutPodIsAlreadyConverged(t *testing.T) {
	sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false)
	bids := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{Name: "bids-file-transferred", Namespace: TestNamespace},
		Spec:       longhorn.BackingImageDataSourceSpec{FileTransferred: true},
	}
	require.NoError(t, sc.ds.BackingImageDataSourceInformer.GetStore().Add(bids))

	converged, err := sc.isBackingImageIPFamilyConverged(types.DataEngineIPFamilyIPv6)
	require.NoError(t, err)
	require.True(t, converged)
}
