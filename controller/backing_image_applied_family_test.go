package controller

import (
	"testing"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func newAppliedBackingImageFamilyControllers(t *testing.T, desired string, managers ...*longhorn.InstanceManager) (*BackingImageManagerController, *BackingImageDataSourceController) {
	t.Helper()
	sc, _ := newIPFamilySettingControllerFixture(t, desired, false, managers...)

	bimController := &BackingImageManagerController{
		namespace:    TestNamespace,
		bimImageName: "backing-image-manager-image",
		ds:           sc.ds,
	}
	bidsController := &BackingImageDataSourceController{
		namespace:    TestNamespace,
		bimImageName: "backing-image-manager-image",
		ds:           sc.ds,
	}
	return bimController, bidsController
}

func backingImageFamilyTestBIM() *longhorn.BackingImageManager {
	return &longhorn.BackingImageManager{
		ObjectMeta: metav1.ObjectMeta{Name: "bim-test", Namespace: TestNamespace},
		Spec: longhorn.BackingImageManagerSpec{
			Image:    "backing-image-manager-image",
			NodeID:   "node-test",
			DiskUUID: "disk-test",
			DiskPath: "/var/lib/longhorn",
			BackingImages: map[string]string{
				"bi-test": "uuid-test",
			},
		},
	}
}

func backingImageFamilyTestBIDS() *longhorn.BackingImageDataSource {
	return &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{Name: "bids-test", Namespace: TestNamespace},
		Spec: longhorn.BackingImageDataSourceSpec{
			UUID:       "uuid-test",
			SourceType: longhorn.BackingImageDataSourceTypeDownload,
		},
	}
}

func addBackingImageFamilyTestBI(t *testing.T, c *BackingImageDataSourceController) {
	t.Helper()
	bi := &longhorn.BackingImage{
		ObjectMeta: metav1.ObjectMeta{Name: "bids-test", Namespace: TestNamespace},
		Status: longhorn.BackingImageStatus{
			UUID: "uuid-test",
		},
	}
	require.NoError(t, c.ds.BackingImageInformer.GetStore().Add(bi))
}

func backingImageFamilyTestCommand(t *testing.T, pod *corev1.Pod) []string {
	t.Helper()
	require.Len(t, pod.Spec.Containers, 1)
	return pod.Spec.Containers[0].Command
}

func TestAppliedBackingImageIPFamilyDrivesBothManifests(t *testing.T) {
	applied := types.DataEngineIPFamilyIPv4
	bimController, bidsController := newAppliedBackingImageFamilyControllers(t, types.DataEngineIPFamilyIPv6,
		newIPFamilyTestInstanceManager("im-v1", longhorn.DataEngineTypeV1, &applied, true))
	addBackingImageFamilyTestBI(t, bidsController)

	bimManifest, err := bimController.generateBackingImageManagerPodManifest(
		backingImageFamilyTestBIM(), nil, "", nil)
	require.NoError(t, err)
	bidsManifest, err := bidsController.generateBackingImageDataSourcePodManifest(backingImageFamilyTestBIDS())
	require.NoError(t, err)

	for _, command := range [][]string{
		backingImageFamilyTestCommand(t, bimManifest),
		backingImageFamilyTestCommand(t, bidsManifest),
	} {
		require.Contains(t, command, "0.0.0.0:8000")
		require.Contains(t, command, "--ip-family")
		require.Contains(t, command, types.DataEngineIPFamilyIPv4)
		require.NotContains(t, command, types.DataEngineIPFamilyIPv6)
	}
}

func TestAppliedBackingImageIPFamilyDefaultsWithoutInitializedPeers(t *testing.T) {
	bimController, bidsController := newAppliedBackingImageFamilyControllers(t, types.DataEngineIPFamilyIPv6)
	addBackingImageFamilyTestBI(t, bidsController)

	bimManifest, err := bimController.generateBackingImageManagerPodManifest(
		backingImageFamilyTestBIM(), nil, "", nil)
	require.NoError(t, err)
	bidsManifest, err := bidsController.generateBackingImageDataSourcePodManifest(backingImageFamilyTestBIDS())
	require.NoError(t, err)

	for _, command := range [][]string{
		backingImageFamilyTestCommand(t, bimManifest),
		backingImageFamilyTestCommand(t, bidsManifest),
	} {
		require.Contains(t, command, ":8000")
		require.NotContains(t, command, "--ip-family")
	}
}

func TestAppliedBackingImageIPFamilyDisagreementBlocksLaunch(t *testing.T) {
	ipv4 := types.DataEngineIPFamilyIPv4
	ipv6 := types.DataEngineIPFamilyIPv6
	bimController, _ := newAppliedBackingImageFamilyControllers(t, types.DataEngineIPFamilyIPv6,
		newIPFamilyTestInstanceManager("im-v1-a", longhorn.DataEngineTypeV1, &ipv4, true),
		newIPFamilyTestInstanceManager("im-v1-b", longhorn.DataEngineTypeV1, &ipv6, true))

	family, err := getAppliedBackingImageIPFamily(bimController.ds)
	require.Empty(t, family)
	require.Error(t, err)
	var invalidState *types.ErrorInvalidState
	require.ErrorAs(t, err, &invalidState)
}
