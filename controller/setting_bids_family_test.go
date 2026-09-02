package controller

import (
	"context"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sfake "k8s.io/client-go/kubernetes/fake"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func newBackingImageFamilySettingFixture(t *testing.T, desired string) (*SettingController, *k8sfake.Clientset) {
	t.Helper()

	kubeClient := k8sfake.NewSimpleClientset() // nolint:staticcheck
	lhClient := lhfake.NewSimpleClientset()    // nolint:staticcheck
	extensionClient := apiextensionsfake.NewSimpleClientset()
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
	for _, setting := range []*longhorn.Setting{
		newSetting(string(types.SettingNamePreferredDataEngineIPFamily), desired),
		newSetting(string(types.SettingNameV1DataEngine), "true"),
		newSetting(string(types.SettingNameV2DataEngine), "false"),
	} {
		created, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
		require.NoError(t, err)
		require.NoError(t, settingIndexer.Add(created))
	}

	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionClient, informerFactories)
	appliedFamily := desired
	im := newIPFamilyTestInstanceManager("im-test", longhorn.DataEngineTypeV1, &appliedFamily, true)
	im.Namespace = TestNamespace
	im.Status.Conditions = types.SetCondition(im.Status.Conditions,
		longhorn.InstanceManagerConditionTypeSettingSynced,
		longhorn.ConditionStatusTrue, "", "")
	require.NoError(t, ds.InstanceManagerInformer.GetStore().Add(im))
	return &SettingController{
		baseController: newBaseController("longhorn-setting", logrus.StandardLogger()),
		ds:             ds,
	}, kubeClient
}

func addMismatchedBackingImageFamilyPods(t *testing.T, sc *SettingController, bids *longhorn.BackingImageDataSource, bidsFamily, bimFamily string) {
	t.Helper()

	require.NoError(t, sc.ds.BackingImageDataSourceInformer.GetStore().Add(bids))
	require.NoError(t, sc.ds.PodInformer.GetStore().Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      types.GetBackingImageDataSourcePodName(bids.Name),
			Namespace: TestNamespace,
			Labels:    types.GetBackingImageDataSourceLabels(bids.Name, "node-test", "disk-test"),
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: BackingImageDataSourcePodContainerName,
			Args: []string{"--ip-family", bidsFamily},
		}}},
	}))
	require.NoError(t, sc.ds.PodInformer.GetStore().Add(&corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bim-test",
			Namespace: TestNamespace,
			Labels:    types.GetBackingImageManagerLabels("node-test", "disk-test"),
		},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: BackingImageManagerPodContainerName,
			Args: []string{"daemon", "--ip-family", bimFamily},
		}}},
	}))
}

func countDeleteActions(client *k8sfake.Clientset) int {
	count := 0
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			count++
		}
	}
	return count
}

func TestPreferredDataEngineIPFamilyBlocksActiveMismatchedBIDSWithoutDeletes(t *testing.T) {
	for _, sourceType := range []longhorn.BackingImageDataSourceType{
		longhorn.BackingImageDataSourceTypeDownload,
		longhorn.BackingImageDataSourceTypeUpload,
		longhorn.BackingImageDataSourceTypeRestore,
		longhorn.BackingImageDataSourceTypeClone,
		longhorn.BackingImageDataSourceTypeExportFromVolume,
	} {
		t.Run(string(sourceType), func(t *testing.T) {
			sc, kubeClient := newBackingImageFamilySettingFixture(t, types.DataEngineIPFamilyIPv6)
			bids := &longhorn.BackingImageDataSource{
				ObjectMeta: metav1.ObjectMeta{Name: "bids-test", Namespace: TestNamespace},
				Spec: longhorn.BackingImageDataSourceSpec{
					SourceType: sourceType,
				},
				Status: longhorn.BackingImageDataSourceStatus{
					CurrentState: longhorn.BackingImageStateInProgress,
				},
			}
			addMismatchedBackingImageFamilyPods(t, sc, bids, types.DataEngineIPFamilyIPv4, types.DataEngineIPFamilyIPv4)

			err := sc.updateBackingImageIPFamily()
			require.Error(t, err)
			var invalidState *types.ErrorInvalidState
			require.ErrorAs(t, err, &invalidState)
			require.Zero(t, countDeleteActions(kubeClient))
		})
	}
}

func TestPreferredDataEngineIPFamilyLeavesMatchingActiveBIDSRunning(t *testing.T) {
	sc, kubeClient := newBackingImageFamilySettingFixture(t, types.DataEngineIPFamilyIPv6)
	bids := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{Name: "bids-test", Namespace: TestNamespace},
		Status: longhorn.BackingImageDataSourceStatus{
			CurrentState: longhorn.BackingImageStateInProgress,
		},
	}
	addMismatchedBackingImageFamilyPods(t, sc, bids, types.DataEngineIPFamilyIPv6, types.DataEngineIPFamilyIPv4)

	require.NoError(t, sc.updateBackingImageIPFamily())
	require.Equal(t, 1, countDeleteActions(kubeClient))
}

func TestPreferredDataEngineIPFamilyBlocksActiveBIDSWithoutLivePod(t *testing.T) {
	for _, tc := range []struct {
		name        string
		terminating bool
	}{
		{name: "missing"},
		{name: "terminating", terminating: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, kubeClient := newBackingImageFamilySettingFixture(t, types.DataEngineIPFamilyIPv6)
			bids := &longhorn.BackingImageDataSource{
				ObjectMeta: metav1.ObjectMeta{Name: "bids-test", Namespace: TestNamespace},
				Status: longhorn.BackingImageDataSourceStatus{
					CurrentState: longhorn.BackingImageStateInProgress,
				},
			}
			require.NoError(t, sc.ds.BackingImageDataSourceInformer.GetStore().Add(bids))
			if tc.terminating {
				now := metav1.Now()
				require.NoError(t, sc.ds.PodInformer.GetStore().Add(&corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name:              types.GetBackingImageDataSourcePodName(bids.Name),
						Namespace:         TestNamespace,
						DeletionTimestamp: &now,
					},
				}))
			}
			require.NoError(t, sc.ds.PodInformer.GetStore().Add(&corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "bim-test",
					Namespace: TestNamespace,
					Labels:    types.GetBackingImageManagerLabels("node-test", "disk-test"),
				},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{
					Name: BackingImageManagerPodContainerName,
					Args: []string{"daemon", "--ip-family", types.DataEngineIPFamilyIPv4},
				}}},
			}))

			err := sc.updateBackingImageIPFamily()
			require.Error(t, err)
			var invalidState *types.ErrorInvalidState
			require.ErrorAs(t, err, &invalidState)
			require.Zero(t, countDeleteActions(kubeClient))
		})
	}
}

func TestPreferredDataEngineIPFamilyAllowsSafeBIDSReplacement(t *testing.T) {
	for _, tc := range []struct {
		name            string
		fileTransferred bool
		state           longhorn.BackingImageState
	}{
		{name: "file transferred", fileTransferred: true, state: longhorn.BackingImageStateInProgress},
		{name: "terminal cleanup", state: longhorn.BackingImageStateFailedAndCleanUp},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, kubeClient := newBackingImageFamilySettingFixture(t, types.DataEngineIPFamilyIPv6)
			bids := &longhorn.BackingImageDataSource{
				ObjectMeta: metav1.ObjectMeta{Name: "bids-test", Namespace: TestNamespace},
				Spec:       longhorn.BackingImageDataSourceSpec{FileTransferred: tc.fileTransferred},
				Status:     longhorn.BackingImageDataSourceStatus{CurrentState: tc.state},
			}
			addMismatchedBackingImageFamilyPods(t, sc, bids, types.DataEngineIPFamilyIPv4, types.DataEngineIPFamilyIPv4)

			require.NoError(t, sc.updateBackingImageIPFamily())
			require.Equal(t, 2, countDeleteActions(kubeClient))
		})
	}
}
