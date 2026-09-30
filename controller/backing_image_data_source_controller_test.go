package controller

import (
	"context"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fake "k8s.io/client-go/kubernetes/fake"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestBackingImageDataSourceBackoffAdvancesOnlyWhenPodCreated(t *testing.T) {
	testCases := []struct {
		name            string
		podExists       bool
		expectedBackoff time.Duration
	}{
		{
			name:            "pod created",
			expectedBackoff: time.Minute,
		},
		{
			name:      "pod already exists but is not in the informer cache yet",
			podExists: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
			lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
			extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
			informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
			ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

			bc, err := NewBackingImageDataSourceController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient,
				TestNamespace, TestNode1, "", "longhorn-backing-image-manager:latest", nil)
			require.NoError(t, err)

			bi := &longhorn.BackingImage{
				ObjectMeta: metav1.ObjectMeta{Name: "bi", Namespace: TestNamespace},
				Status:     longhorn.BackingImageStatus{UUID: "bi-uuid"},
			}
			require.NoError(t, informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer().Add(bi))

			bids := &longhorn.BackingImageDataSource{
				ObjectMeta: metav1.ObjectMeta{Name: bi.Name, Namespace: TestNamespace},
				Spec: longhorn.BackingImageDataSourceSpec{
					NodeID:     TestNode1,
					UUID:       bi.Status.UUID,
					DiskUUID:   TestDiskID1,
					DiskPath:   TestDefaultDataPath,
					SourceType: longhorn.BackingImageDataSourceTypeDownload,
					Parameters: map[string]string{longhorn.DataSourceTypeDownloadParameterURL: "http://example.com/image"},
				},
				Status: longhorn.BackingImageDataSourceStatus{
					OwnerID:      TestNode1,
					CurrentState: longhorn.BackingImageStateFailed,
					Message:      "download failed",
				},
			}

			podName := types.GetBackingImageDataSourcePodName(bids.Name)
			if tc.podExists {
				_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(),
					&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: TestNamespace}}, metav1.CreateOptions{})
				require.NoError(t, err)
			}

			require.NoError(t, bc.syncBackingImageDataSourcePod(bids))
			_, err = kubeClient.CoreV1().Pods(TestNamespace).Get(context.TODO(), podName, metav1.GetOptions{})
			require.NoError(t, err)
			require.Equal(t, tc.expectedBackoff, bc.backoff.Get(bids.Name))
		})
	}
}
