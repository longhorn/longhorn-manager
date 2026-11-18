package datastore

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

func newSettingTestDataStore(t *testing.T, setting *longhorn.Setting) (*DataStore, *lhfake.Clientset, cache.Indexer) {
	t.Helper()

	var objects []runtime.Object
	if setting != nil {
		objects = append(objects, setting)
	}
	lhClient := lhfake.NewSimpleClientset(objects...) // nolint:staticcheck
	settingInformer := lhinformerfactory.NewSharedInformerFactory(lhClient, 0).Longhorn().V1beta2().Settings()
	indexer := settingInformer.Informer().GetIndexer()
	if setting != nil {
		require.NoError(t, indexer.Add(setting.DeepCopy()))
	}

	return &DataStore{
		namespace:     "longhorn-system",
		lhClient:      lhClient,
		settingLister: settingInformer.Lister(),
	}, lhClient, indexer
}

func TestCreateOrUpdateSettingRetriesConflict(t *testing.T) {
	const desiredValue, configMapResourceVersion = "true", "10"
	name := types.SettingNameCreateDefaultDiskLabeledNodes
	annotationKey := types.GetLonghornLabelKey(types.ConfigMapResourceVersionKey)
	resource := longhorn.SchemeGroupVersion.WithResource("settings")

	for _, concurrentWriterSetsDesiredValue := range []bool{false, true} {
		t.Run(fmt.Sprintf("concurrent writer sets desired value=%t", concurrentWriterSetsDesiredValue), func(t *testing.T) {
			initial := &longhorn.Setting{
				ObjectMeta: metav1.ObjectMeta{
					Name:            string(name),
					Namespace:       "longhorn-system",
					ResourceVersion: "1",
				},
				Value: "false",
			}
			ds, lhClient, indexer := newSettingTestDataStore(t, initial)
			updates := 0
			lhClient.PrependReactor("update", "settings", func(action k8stesting.Action) (bool, runtime.Object, error) {
				updates++
				if updates == 1 {
					// A competing writer updates the API object, but the informer cache remains stale.
					latest := initial.DeepCopy()
					latest.ResourceVersion = "2"
					latest.Labels = map[string]string{"concurrent-writer": "preserved"}
					latest.Annotations = map[string]string{"concurrent-writer": "preserved"}
					latest.Status.Applied = true
					if concurrentWriterSetsDesiredValue {
						latest.Value = desiredValue
						latest.Annotations[annotationKey] = configMapResourceVersion
					}
					require.NoError(t, lhClient.Tracker().Update(resource, latest, ds.namespace))
					return true, nil, apierrors.NewConflict(resource.GroupResource(), string(name), fmt.Errorf("concurrent update"))
				}

				// The fake client does not enforce resource versions, so emulate API conflict detection.
				obj, err := lhClient.Tracker().Get(resource, ds.namespace, string(name))
				require.NoError(t, err)
				latest := obj.(*longhorn.Setting)
				updated := action.(k8stesting.UpdateAction).GetObject().(*longhorn.Setting).DeepCopy()
				if updated.ResourceVersion != latest.ResourceVersion {
					return true, nil, apierrors.NewConflict(resource.GroupResource(), string(name), fmt.Errorf("stale resource version"))
				}
				version, err := strconv.Atoi(latest.ResourceVersion)
				require.NoError(t, err)
				updated.ResourceVersion = strconv.Itoa(version + 1)
				require.NoError(t, lhClient.Tracker().Update(resource, updated, ds.namespace))
				require.NoError(t, indexer.Update(updated.DeepCopy()))
				return true, updated, nil
			})

			require.NoError(t, ds.createOrUpdateSetting(name, desiredValue, configMapResourceVersion))
			setting, err := lhClient.LonghornV1beta2().Settings(ds.namespace).Get(context.TODO(), string(name), metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, desiredValue, setting.Value)
			assert.Equal(t, configMapResourceVersion, setting.Annotations[annotationKey])
			assert.Equal(t, "preserved", setting.Annotations["concurrent-writer"])
			assert.Equal(t, "preserved", setting.Labels["concurrent-writer"])
			assert.True(t, setting.Status.Applied)
			assert.NotContains(t, setting.Annotations, types.GetLonghornLabelKey(types.UpdateSettingFromLonghorn))
			if concurrentWriterSetsDesiredValue {
				assert.Equal(t, 1, updates, "a retry should skip an already synchronized setting")
			} else {
				assert.Equal(t, 3, updates, "one conflict followed by the setting update and annotation cleanup")
			}
		})
	}
}

func TestCreateOrUpdateSettingPropagatesErrors(t *testing.T) {
	name := types.SettingNameCreateDefaultDiskLabeledNodes
	resource := longhorn.SchemeGroupVersion.WithResource("settings").GroupResource()
	for _, tc := range []struct {
		name             string
		verb             string
		err              error
		settingExists    bool
		cleanupConflict  bool
		expectedAttempts int
	}{
		{"read failure", "get", apierrors.NewForbidden(resource, string(name), fmt.Errorf("denied")), true, false, 1},
		{"update failure", "update", apierrors.NewForbidden(resource, string(name), fmt.Errorf("denied")), true, false, 1},
		{"create failure", "create", apierrors.NewForbidden(resource, string(name), fmt.Errorf("denied")), false, false, 1},
		{"conflict retries exhausted", "update", apierrors.NewConflict(resource, string(name), fmt.Errorf("concurrent update")), true, false, retry.DefaultRetry.Steps},
		{"cleanup conflicts exhausted", "update", apierrors.NewConflict(resource, string(name), fmt.Errorf("concurrent update")), true, true, retry.DefaultRetry.Steps * retry.DefaultRetry.Steps},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var initial *longhorn.Setting
			if tc.settingExists {
				initial = &longhorn.Setting{
					ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: "longhorn-system"},
					Value:      "false",
				}
			}
			ds, lhClient, _ := newSettingTestDataStore(t, initial)
			attempts := 0
			lhClient.PrependReactor(tc.verb, "settings", func(action k8stesting.Action) (bool, runtime.Object, error) {
				if tc.cleanupConflict {
					setting := action.(k8stesting.UpdateAction).GetObject().(*longhorn.Setting)
					if _, exists := setting.Annotations[types.GetLonghornLabelKey(types.UpdateSettingFromLonghorn)]; exists {
						return false, nil, nil
					}
				}
				attempts++
				return true, nil, tc.err
			})

			err := ds.createOrUpdateSetting(name, "true", "10")
			require.ErrorIs(t, err, tc.err)
			assert.Equal(t, tc.expectedAttempts, attempts)
		})
	}
}

func TestCreateOrUpdateSettingCreatesOrSkips(t *testing.T) {
	name := types.SettingNameCreateDefaultDiskLabeledNodes
	annotationKey := types.GetLonghornLabelKey(types.ConfigMapResourceVersionKey)
	for _, tc := range []struct {
		name        string
		exists      bool
		value       string
		annotations map[string]string
		updates     int
	}{
		{name: "create missing setting"},
		{name: "skip synchronized setting", exists: true, value: "true", annotations: map[string]string{annotationKey: "10"}},
		{name: "update value", exists: true, value: "false", annotations: map[string]string{annotationKey: "10"}, updates: 2},
		{name: "update ConfigMap version", exists: true, value: "true", annotations: map[string]string{annotationKey: "9"}, updates: 2},
		{name: "initialize annotations", exists: true, value: "false", updates: 2},
		{name: "finish annotation cleanup", exists: true, value: "true", annotations: map[string]string{annotationKey: "10", types.GetLonghornLabelKey(types.UpdateSettingFromLonghorn): ""}, updates: 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var initial *longhorn.Setting
			if tc.exists {
				initial = &longhorn.Setting{
					ObjectMeta: metav1.ObjectMeta{
						Name:        string(name),
						Namespace:   "longhorn-system",
						Annotations: tc.annotations,
					},
					Value: tc.value,
				}
			}
			ds, lhClient, _ := newSettingTestDataStore(t, initial)
			require.NoError(t, ds.createOrUpdateSetting(name, "true", "10"))
			updates := 0
			for _, action := range lhClient.Actions() {
				if action.GetVerb() == "update" {
					updates++
				}
			}
			assert.Equal(t, tc.updates, updates)
			setting, err := lhClient.LonghornV1beta2().Settings(ds.namespace).Get(context.TODO(), string(name), metav1.GetOptions{})
			require.NoError(t, err)
			assert.Equal(t, "true", setting.Value)
			assert.Equal(t, "10", setting.Annotations[annotationKey])
			assert.NotContains(t, setting.Annotations, types.GetLonghornLabelKey(types.UpdateSettingFromLonghorn))
		})
	}
}
