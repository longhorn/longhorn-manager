package v113xto1140

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

const bimUpgradeNamespace = "longhorn-system"
const bimDiskReadyAt = "2026-10-15T08:00:00Z"

func newBIMPodUpgradeFixture() (*longhorn.BackingImageManager, *longhorn.Node, *corev1.Pod) {
	bim := &longhorn.BackingImageManager{
		Name: "bim-upgrade", Namespace: bimUpgradeNamespace,
		Spec: longhorn.BackingImageManagerSpec{NodeID: "node-1", DiskUUID: "disk-uuid", DiskPath: "/mnt/disk"},
	}
	node := &longhorn.Node{
		Name: bim.Spec.NodeID, Namespace: bimUpgradeNamespace,
		Spec: longhorn.NodeSpec{
			Disks: map[string]longhorn.DiskSpec{"disk-1": {Path: bim.Spec.DiskPath}},
		},
		Status: longhorn.NodeStatus{
			DiskStatus: map[string]*longhorn.DiskStatus{
				"disk-1": {
					DiskUUID: bim.Spec.DiskUUID,
					Conditions: []longhorn.Condition{
						{Type: longhorn.DiskConditionTypeReady, Status: longhorn.ConditionStatusTrue, LastTransitionTime: bimDiskReadyAt},
					},
				},
				"nil-disk": nil,
			},
		},
	}
	pod := &corev1.Pod{
		Name: bim.Name, Namespace: bimUpgradeNamespace, UID: "legacy-bim-pod",
		Annotations: map[string]string{"unrelated": "keep"},
		Labels:      types.GetBackingImageManagerLabels(bim.Spec.NodeID, bim.Spec.DiskUUID),
		Spec:        corev1.PodSpec{NodeName: bim.Spec.NodeID},
		Status:      corev1.PodStatus{Phase: corev1.PodRunning},
	}
	return bim, node, pod
}

func TestUpgradeResourcesAnnotatesBackingImageManagerPods(t *testing.T) {
	annotationKey := types.GetLonghornLabelKey(types.BackingImageManagerStartedAtAnnotationKeySuffix)
	for _, tc := range []struct {
		name           string
		configure      func(*longhorn.Node, *corev1.Pod)
		missingPod     bool
		wantAnnotation *string
		wantUpdates    int
	}{
		{name: "legacy running pod", wantAnnotation: new(bimDiskReadyAt), wantUpdates: 1},
		{name: "nil annotations", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Annotations = nil
		}, wantAnnotation: new(bimDiskReadyAt), wantUpdates: 1},
		{name: "already annotated", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Annotations[annotationKey] = bimDiskReadyAt
		}, wantAnnotation: new(bimDiskReadyAt)},
		{name: "stale annotation preserved", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Annotations[annotationKey] = "2026-10-14T08:00:00Z"
		}, wantAnnotation: new("2026-10-14T08:00:00Z")},
		{name: "explicitly empty annotation preserved", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Annotations[annotationKey] = ""
		}, wantAnnotation: new("")},
		{name: "pending pod", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Status.Phase = corev1.PodPending
		}},
		{name: "failed pod", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Status.Phase = corev1.PodFailed
		}},
		{name: "deleting pod", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.DeletionTimestamp = new(metav1.Now())
		}},
		{name: "wrong pod node", configure: func(_ *longhorn.Node, pod *corev1.Pod) {
			pod.Spec.NodeName = "another-node"
		}},
		{name: "missing node", configure: func(node *longhorn.Node, _ *corev1.Pod) {
			node.Name = "another-node"
		}},
		{name: "missing disk", configure: func(node *longhorn.Node, _ *corev1.Pod) {
			delete(node.Status.DiskStatus, "disk-1")
		}},
		{name: "disk path migrated", configure: func(node *longhorn.Node, _ *corev1.Pod) {
			node.Spec.Disks["disk-1"] = longhorn.DiskSpec{Path: "/mnt/another-path"}
		}},
		{name: "missing pod", missingPod: true},
		{name: "unavailable disk", configure: func(node *longhorn.Node, _ *corev1.Pod) {
			node.Status.DiskStatus["disk-1"].Conditions[0].Status = longhorn.ConditionStatusFalse
		}, wantAnnotation: new(bimDiskReadyAt), wantUpdates: 1},
		{name: "missing Ready condition", configure: func(node *longhorn.Node, _ *corev1.Pod) {
			node.Status.DiskStatus["disk-1"].Conditions[0].Type = longhorn.DiskConditionTypeSchedulable
		}, wantAnnotation: new(""), wantUpdates: 1},
		{name: "missing transition timestamp", configure: func(node *longhorn.Node, _ *corev1.Pod) {
			node.Status.DiskStatus["disk-1"].Conditions[0].LastTransitionTime = ""
		}, wantAnnotation: new(""), wantUpdates: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bim, node, pod := newBIMPodUpgradeFixture()
			if tc.configure != nil {
				tc.configure(node, pod)
			}
			unrelated := &corev1.Pod{
				Name: "unrelated-pod", Namespace: bimUpgradeNamespace,
				Status: corev1.PodStatus{Phase: corev1.PodRunning},
			}
			kube := kubefake.NewClientset(unrelated)
			if !tc.missingPod {
				require.NoError(t, kube.Tracker().Add(pod.DeepCopy()))
			}
			resourceMaps := map[string]any{
				types.LonghornKindNode:                map[string]*longhorn.Node{node.Name: node},
				types.LonghornKindBackingImageManager: map[string]*longhorn.BackingImageManager{bim.Name: bim},
				// Cached so the default data path migration does not use the nil Longhorn client.
				types.LonghornKindSetting: map[string]*longhorn.Setting{},
			}
			require.NoError(t, UpgradeResources(bimUpgradeNamespace, nil, kube, resourceMaps))
			checkPod := func() {
				t.Helper()
				actual, err := kube.CoreV1().Pods(bimUpgradeNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
				if tc.missingPod {
					require.True(t, apierrors.IsNotFound(err))
					return
				}
				require.NoError(t, err)
				value, exists := actual.Annotations[annotationKey]
				if tc.wantAnnotation == nil {
					require.False(t, exists)
				} else {
					require.True(t, exists)
					require.Equal(t, *tc.wantAnnotation, value)
				}
				require.Equal(t, pod.Spec, actual.Spec)
				require.Equal(t, pod.Status, actual.Status)
				require.Equal(t, pod.UID, actual.UID)
				require.Equal(t, pod.Labels, actual.Labels)
				require.Equal(t, pod.Annotations["unrelated"], actual.Annotations["unrelated"])
			}
			checkPod()

			// Upgrade retries must not overwrite an annotation after another disk transition.
			if disk := node.Status.DiskStatus["disk-1"]; disk != nil {
				disk.Conditions[0].Status = longhorn.ConditionStatusTrue
				disk.Conditions[0].LastTransitionTime = "2026-10-16T08:00:00Z"
			}
			require.NoError(t, UpgradeResources(bimUpgradeNamespace, nil, kube, resourceMaps))
			checkPod()
			updates := 0
			for _, action := range kube.Actions() {
				if action.GetVerb() == "update" {
					updates++
				}
			}
			require.Equal(t, tc.wantUpdates, updates)
			actual, err := kube.CoreV1().Pods(bimUpgradeNamespace).Get(t.Context(), unrelated.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Empty(t, actual.Annotations)
		})
	}
}

func TestUpgradeResourcesRetriesBackingImageManagerPodErrors(t *testing.T) {
	for _, verb := range []string{"get", "update"} {
		t.Run(verb, func(t *testing.T) {
			bim, node, pod := newBIMPodUpgradeFixture()
			kube := kubefake.NewClientset(pod)
			fail := true
			kube.PrependReactor(verb, "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
				if fail {
					fail = false
					return true, nil, apierrors.NewConflict(corev1.Resource("pods"), pod.Name, nil)
				}
				return false, nil, nil
			})
			resourceMaps := map[string]any{
				types.LonghornKindNode:                map[string]*longhorn.Node{node.Name: node},
				types.LonghornKindBackingImageManager: map[string]*longhorn.BackingImageManager{bim.Name: bim},
				// Cached so the default data path migration does not use the nil Longhorn client.
				types.LonghornKindSetting: map[string]*longhorn.Setting{},
			}
			require.Error(t, UpgradeResources(bimUpgradeNamespace, nil, kube, resourceMaps))
			actual, err := kube.CoreV1().Pods(bimUpgradeNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.NotContains(t, actual.Annotations, types.GetLonghornLabelKey(types.BackingImageManagerStartedAtAnnotationKeySuffix))
			require.NoError(t, UpgradeResources(bimUpgradeNamespace, nil, kube, resourceMaps))
			actual, err = kube.CoreV1().Pods(bimUpgradeNamespace).Get(t.Context(), pod.Name, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, bimDiskReadyAt, actual.Annotations[types.GetLonghornLabelKey(types.BackingImageManagerStartedAtAnnotationKeySuffix)])
		})
	}
}

func TestUpgradeResourcesRejectsNilCache(t *testing.T) {
	require.ErrorContains(t, UpgradeResources(bimUpgradeNamespace, nil, nil, nil), "resourceMaps cannot be nil")
}

// fakeSettingAPI serves the settings Create endpoint. The migration takes a
// concrete *lhclientset.Clientset, so the generated fake clientset cannot be used.
type fakeSettingAPI struct {
	t         *testing.T
	mu        sync.Mutex
	createErr *apierrors.StatusError
	created   []string
}

func newFakeSettingAPI(t *testing.T, createErr *apierrors.StatusError) (*fakeSettingAPI, *lhclientset.Clientset) {
	t.Helper()
	api := &fakeSettingAPI{t: t, createErr: createErr}
	server := httptest.NewServer(api)
	t.Cleanup(server.Close)

	client, err := lhclientset.NewForConfig(&rest.Config{Host: server.URL})
	require.NoError(t, err)
	return api, client
}

func (f *fakeSettingAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	settingsPath := fmt.Sprintf("/apis/%s/namespaces/%s/settings", longhorn.SchemeGroupVersion, bimUpgradeNamespace)
	if r.Method != http.MethodPost || r.URL.Path != settingsPath {
		f.t.Errorf("unexpected request %v %v", r.Method, r.URL.Path)
		http.Error(w, "unexpected request", http.StatusNotFound)
		return
	}

	var setting longhorn.Setting
	if err := json.NewDecoder(r.Body).Decode(&setting); err != nil {
		f.t.Errorf("failed to decode setting: %v", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if setting.Name != string(types.SettingNameV2DefaultDataPath) {
		f.t.Errorf("unexpected setting %v created", setting.Name)
	}

	f.mu.Lock()
	f.created = append(f.created, setting.Value)
	createErr := f.createErr
	f.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	var body any = &setting
	code := http.StatusCreated
	if createErr != nil {
		status := createErr.ErrStatus
		status.Kind, status.APIVersion = "Status", "v1"
		body, code = &status, int(status.Code)
	}
	w.WriteHeader(code)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		f.t.Errorf("failed to encode response: %v", err)
	}
}

func (f *fakeSettingAPI) setCreateErr(err *apierrors.StatusError) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createErr = err
}

func (f *fakeSettingAPI) createdValues() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.created)
}

func newUpgradeSetting(name types.SettingName, value string) *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: bimUpgradeNamespace},
		Value:      value,
	}
}

func requireSettingValue(t *testing.T, settings map[string]*longhorn.Setting, name types.SettingName, want *string) {
	t.Helper()
	setting, exists := settings[string(name)]
	if want == nil {
		require.False(t, exists, "setting %v should not be cached", name)
		return
	}
	require.True(t, exists, "setting %v should be cached", name)
	require.Equal(t, *want, setting.Value, "setting %v", name)
}

func TestUpgradeDefaultDataPathSetting(t *testing.T) {
	const blockPath = "/dev/nvme1n1"
	const bdfAddress = "0000:00:1e.0"
	defaultPath := types.SettingDefinitionDefaultDataPath.Default
	alreadyExists := apierrors.NewAlreadyExists(longhorn.Resource("settings"), string(types.SettingNameV2DefaultDataPath))
	internalErr := apierrors.NewInternalError(fmt.Errorf("injected create failure"))

	for _, tc := range []struct {
		name      string
		v1Value   *string // nil: default-data-path is not in the cache
		v2Value   *string // nil: v2-default-data-path is not in the cache
		createErr *apierrors.StatusError
		wantErr   bool
		// Expected values in the settings cache, which UpdateResources persists later.
		wantV1 *string
		wantV2 *string
		// Values sent to the API server in Create requests for v2-default-data-path.
		wantCreated []string
	}{
		{name: "block device path moved to new setting", v1Value: new(blockPath),
			wantV1: new(defaultPath), wantCreated: []string{blockPath}},
		{name: "BDF address moved to new setting", v1Value: new(bdfAddress),
			wantV1: new(defaultPath), wantCreated: []string{bdfAddress}},
		{name: "default directory untouched", v1Value: new(defaultPath),
			wantV1: new(defaultPath)},
		{name: "custom directory untouched", v1Value: new("/mnt/longhorn"),
			wantV1: new("/mnt/longhorn")},
		{name: "directory untouched when V2 setting exists", v1Value: new("/mnt/longhorn"), v2Value: new("/dev/sdc"),
			wantV1: new("/mnt/longhorn"), wantV2: new("/dev/sdc")},
		{name: "empty V2 setting filled in cache", v1Value: new(blockPath), v2Value: new(""),
			wantV1: new(defaultPath), wantV2: new(blockPath)},
		{name: "existing V2 value not overwritten", v1Value: new(blockPath), v2Value: new("/dev/sdc"),
			wantV1: new(defaultPath), wantV2: new("/dev/sdc")},
		{name: "V1 setting missing"},
		{name: "V2 setting created concurrently", v1Value: new(blockPath), createErr: alreadyExists,
			wantV1: new(defaultPath), wantCreated: []string{blockPath}},
		{name: "create failure keeps V1 value for retry", v1Value: new(blockPath), createErr: internalErr,
			wantErr: true, wantV1: new(blockPath), wantCreated: []string{blockPath}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			settings := map[string]*longhorn.Setting{}
			if tc.v1Value != nil {
				settings[string(types.SettingNameDefaultDataPath)] = newUpgradeSetting(types.SettingNameDefaultDataPath, *tc.v1Value)
			}
			if tc.v2Value != nil {
				settings[string(types.SettingNameV2DefaultDataPath)] = newUpgradeSetting(types.SettingNameV2DefaultDataPath, *tc.v2Value)
			}
			resourceMaps := map[string]any{types.LonghornKindSetting: settings}
			api, lhClient := newFakeSettingAPI(t, tc.createErr)

			err := upgradeDefaultDataPathSetting(bimUpgradeNamespace, lhClient, resourceMaps)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			requireSettingValue(t, settings, types.SettingNameDefaultDataPath, tc.wantV1)
			requireSettingValue(t, settings, types.SettingNameV2DefaultDataPath, tc.wantV2)
			require.Equal(t, tc.wantCreated, api.createdValues())

			if tc.wantErr {
				// The upgrade is retried after a failure; the next run must complete the migration.
				api.setCreateErr(nil)
				require.NoError(t, upgradeDefaultDataPathSetting(bimUpgradeNamespace, lhClient, resourceMaps))
				requireSettingValue(t, settings, types.SettingNameDefaultDataPath, new(defaultPath))
				require.Equal(t, append(slices.Clone(tc.wantCreated), *tc.v1Value), api.createdValues())
				return
			}

			// Re-running the migration must be a no-op.
			require.NoError(t, upgradeDefaultDataPathSetting(bimUpgradeNamespace, lhClient, resourceMaps))
			requireSettingValue(t, settings, types.SettingNameDefaultDataPath, tc.wantV1)
			requireSettingValue(t, settings, types.SettingNameV2DefaultDataPath, tc.wantV2)
			require.Equal(t, tc.wantCreated, api.createdValues())
		})
	}
}
