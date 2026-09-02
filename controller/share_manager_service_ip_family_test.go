package controller

import (
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"k8s.io/client-go/kubernetes/fake"
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

func TestShareManagerControllerCreateServiceManifestIPFamilyPolicy(t *testing.T) {
	tests := []struct {
		name            string
		endpointNetwork bool
		wantClusterIP   string
		wantSelector    map[string]string
	}{
		{
			name:          "selector service",
			wantClusterIP: "",
			wantSelector:  types.GetShareManagerInstanceLabel("share-manager"),
		},
		{
			name:            "endpoint network headless service",
			endpointNetwork: true,
			wantClusterIP:   corev1.ClusterIPNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			controller := newShareManagerServiceControllerFixture(t, tt.endpointNetwork)
			sm := &longhorn.ShareManager{}
			sm.Name = "share-manager"
			sm.Namespace = TestNamespace

			service := controller.createServiceManifest(sm)
			require.Equal(t, corev1.ServiceTypeClusterIP, service.Spec.Type)
			require.Equal(t, tt.wantClusterIP, service.Spec.ClusterIP)
			require.Equal(t, tt.wantSelector, service.Spec.Selector)
			require.NotNil(t, service.Spec.IPFamilyPolicy)
			require.Equal(t, corev1.IPFamilyPolicyPreferDualStack, *service.Spec.IPFamilyPolicy)

			otherService := controller.createServiceManifest(sm)
			require.NotSame(t, service.Spec.IPFamilyPolicy, otherService.Spec.IPFamilyPolicy)
		})
	}
}

func TestShareManagerControllerReconcileServiceIPFamilyPolicy(t *testing.T) {
	controller := newShareManagerServiceControllerFixture(t, false)
	service, err := controller.ds.CreateService(TestNamespace, &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "share-manager",
			Namespace: TestNamespace,
		},
	})
	require.NoError(t, err)
	require.Nil(t, service.Spec.IPFamilyPolicy)

	updated, err := controller.reconcileShareManagerServiceIPFamilyPolicy(service)
	require.NoError(t, err)
	require.NotNil(t, updated.Spec.IPFamilyPolicy)
	require.Equal(t, corev1.IPFamilyPolicyPreferDualStack, *updated.Spec.IPFamilyPolicy)
	require.Nil(t, service.Spec.IPFamilyPolicy)

	unchanged, err := controller.reconcileShareManagerServiceIPFamilyPolicy(updated)
	require.NoError(t, err)
	require.Same(t, updated, unchanged)
}

func newShareManagerServiceControllerFixture(t *testing.T, endpointNetwork bool) *ShareManagerController {
	t.Helper()

	kubeClient := fake.NewSimpleClientset()                    // nolint:staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint:staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint:staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

	if endpointNetwork {
		setting := newSetting(string(types.SettingNameEndpointNetworkForRWXVolume), "longhorn-system/ipv4-only")
		settingIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()
		require.NoError(t, settingIndexer.Add(setting))
	}

	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	return &ShareManagerController{
		baseController: newBaseController("test-share-manager", logrus.StandardLogger()),
		namespace:      TestNamespace,
		ds:             ds,
	}
}
