package app

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestV2OnlyPreUpgradeRejectsLegacyBeforeUpgradeAndHostChecks(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		objects := []runtime.Object{}
		if legacy {
			objects = append(objects, &longhorn.Volume{ObjectMeta: metav1.ObjectMeta{Name: "retained-with-empty-engine", Namespace: "longhorn-system"}})
		}
		lh, kube := lhfake.NewClientset(objects...), kubefake.NewClientset()
		upgrader := newPreUpgrader("longhorn-system", lh, kube, record.NewFakeRecorder(10), types.DataEngineModeV2Only)
		err := upgrader.Run(t.Context())
		if legacy {
			if err == nil || !strings.Contains(err.Error(), "cannot use v2-only") {
				t.Fatalf("legacy pre-upgrade did not fail at the storage gate: %v", err)
			}
			for _, action := range lh.Actions() {
				if action.GetVerb() != "get" && action.GetVerb() != "list" {
					t.Fatal("legacy pre-upgrade mutated storage")
				}
			}
		} else if err != nil {
			t.Fatalf("empty V2-only bootstrap required iSCSI: %v", err)
		}
	}
}
