package datastore

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	extensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/types"
)

func TestV2OnlyRestoreCRDsRequireCompleteCurrentCompatibleSchemas(t *testing.T) {
	crd := apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "volumes.longhorn.io", UID: "live-crd", ResourceVersion: "7"},
		Spec:       apiextensionsv1.CustomResourceDefinitionSpec{Group: "longhorn.io", Names: apiextensionsv1.CustomResourceDefinitionNames{Plural: "volumes"}, Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{Name: "v1beta2", Served: true, Storage: true}}},
	}
	archive := &apiextensionsv1.CustomResourceDefinitionList{Items: []apiextensionsv1.CustomResourceDefinition{*crd.DeepCopy()}}
	for _, test := range []string{"compatible", "missing", "changed-schema", "deleting", "missing-identity", "incomplete", "duplicate-archive", "foreign-archive", "extra-live", "empty-archive"} {
		t.Run(test, func(t *testing.T) {
			candidate, live := archive.DeepCopy(), crd.DeepCopy()
			switch test {
			case "missing":
				live.Name = "other.longhorn.io"
			case "changed-schema":
				live.Spec.Versions[0].Served = false
			case "deleting":
				now := metav1.Now()
				live.DeletionTimestamp = &now
			case "missing-identity":
				live.UID = ""
			case "duplicate-archive":
				candidate.Items = append(candidate.Items, crd)
			case "foreign-archive":
				candidate.Items[0].Spec.Group = "other.io"
			case "empty-archive":
				candidate.Items = nil
			}
			client := extensionsfake.NewClientset(live)
			if test == "incomplete" {
				client.PrependReactor("list", "customresourcedefinitions", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, &apiextensionsv1.CustomResourceDefinitionList{ListMeta: metav1.ListMeta{Continue: "next"}, Items: []apiextensionsv1.CustomResourceDefinition{*live}}, nil
				})
			}
			if test == "extra-live" {
				extra := crd.DeepCopy()
				extra.Name = "settings.longhorn.io"
				if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(t.Context(), extra, metav1.CreateOptions{}); err != nil {
					t.Fatal(err)
				}
				client.ClearActions()
			}
			ds := &DataStore{dataEngineMode: types.DataEngineModeV2Only, extensionsClient: client}
			if err := ds.ValidateDataEngineModeRestoreCRDs(t.Context(), candidate); (err == nil) != (test == "compatible") {
				t.Fatalf("unexpected CRD compatibility result: %v", err)
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "list" && action.GetVerb() != "get" {
					t.Fatal("CRD validation wrote to the cluster")
				}
			}
		})
	}
}
