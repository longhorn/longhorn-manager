package controller

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestSystemRestoreJobPreservesDataEngineCapability(t *testing.T) {
	for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable, types.DataEngineModeV2Only} {
		t.Run(string(mode), func(t *testing.T) {
			lh, kube := lhfake.NewClientset(), kubefake.NewClientset()
			factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
			store := datastore.NewDataStoreForGlobal("longhorn-system", lh, kube, nil, factories, mode)
			controller := &SystemRestoreController{ds: store}
			restore := &longhorn.SystemRestore{ObjectMeta: metav1.ObjectMeta{Name: "restore-check"}}
			job := controller.newSystemRestoreJob(restore, "longhorn-system", "backup-manager-image", "longhorn", "", nil)
			want := []string{"longhorn-manager", "system-rollout", "restore-check"}
			if mode == types.DataEngineModeV2Only {
				want = []string{"longhorn-manager", "system-rollout", "--data-engine-mode", "v2-only", "restore-check"}
			}
			if got := job.Spec.Template.Spec.Containers[0].Command; !reflect.DeepEqual(got, want) {
				t.Fatalf("restore worker lost the installation capability or changed the legacy command: got=%v want=%v", got, want)
			}
		})
	}
}
