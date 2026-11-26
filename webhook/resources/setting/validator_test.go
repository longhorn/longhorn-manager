package setting

import (
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/cache"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func newSettingValidatorDataStore(t *testing.T, volumeState longhorn.VolumeState) *datastore.DataStore {
	t.Helper()
	namespace := "longhorn-system"
	lhClient := lhfake.NewClientset(
		&longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(types.SettingNamePreferredDataEngineIPFamily), Namespace: namespace}, Value: types.DataEngineIPFamilyDefault},
		&longhorn.Volume{ObjectMeta: metav1.ObjectMeta{Name: "volume-1", Namespace: namespace}, Status: longhorn.VolumeStatus{State: volumeState}},
	)
	kubeClient := fake.NewSimpleClientset()
	extensionsClient := apiextensionsfake.NewSimpleClientset()
	informerFactories := util.NewInformerFactories(namespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStoreForGlobal(namespace, lhClient, kubeClient, extensionsClient, informerFactories)
	stopCh := make(chan struct{})
	t.Cleanup(func() { close(stopCh) })
	informerFactories.Start(stopCh)
	require.True(t, cache.WaitForCacheSync(stopCh, ds.SettingInformer.HasSynced, ds.VolumeInformer.HasSynced))
	return ds
}

func settingUpdate(value string) (*longhorn.Setting, *longhorn.Setting) {
	oldSetting := &longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(types.SettingNamePreferredDataEngineIPFamily)}, Value: types.DataEngineIPFamilyDefault}
	newSetting := oldSetting.DeepCopy()
	newSetting.Value = value
	return oldSetting, newSetting
}

func TestPreferredDataEngineIPFamilyUpdateRequiresDetachedVolumes(t *testing.T) {
	for _, state := range []longhorn.VolumeState{
		longhorn.VolumeStateAttached,
		longhorn.VolumeStateDetaching,
	} {
		t.Run(string(state), func(t *testing.T) {
			oldSetting, newSetting := settingUpdate(types.DataEngineIPFamilyIPv4)
			validator := NewValidator(newSettingValidatorDataStore(t, state))
			require.Error(t, validator.Update(nil, oldSetting, newSetting))
		})
	}
}

func TestPreferredDataEngineIPFamilyUpdateAllowsDetachedVolumes(t *testing.T) {
	oldSetting, newSetting := settingUpdate(types.DataEngineIPFamilyIPv4)
	validator := NewValidator(newSettingValidatorDataStore(t, longhorn.VolumeStateDetached))
	require.NoError(t, validator.Update(nil, oldSetting, newSetting))
}

func TestPreferredDataEngineIPFamilyUnchangedAllowsStatusUpdateWithAttachedVolume(t *testing.T) {
	oldSetting, newSetting := settingUpdate(types.DataEngineIPFamilyDefault)
	newSetting.Status = longhorn.SettingStatus{Applied: true}
	validator := NewValidator(newSettingValidatorDataStore(t, longhorn.VolumeStateAttached))
	require.NoError(t, validator.Update(nil, oldSetting, newSetting))
}
