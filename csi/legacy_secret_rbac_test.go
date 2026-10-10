package csi

import (
	"errors"
	"testing"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"

	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/types"
)

const testRBACNamespace = "longhorn-system"

func TestReconcileLegacySecretRBACListFailureDoesNotMutateRBAC(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "storageclasses", func(action ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("storage class list failed")
	})

	if err := ReconcileLegacySecretRBAC(client, testRBACNamespace, logrus.New()); err == nil {
		t.Fatal("expected StorageClass list error")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() != "list" || action.GetResource().Resource != "storageclasses" {
			t.Fatalf("unexpected action after StorageClass list failure: %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func TestReconcileLegacySecretRBACEnableMutatesRoleBeforeBinding(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testRBACNamespace, UID: "namespace-uid"}},
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: "legacy-sc"},
			Provisioner: types.LonghornDriverName,
			Parameters:  map[string]string{"csi.storage.k8s.io/provisioner-secret-name": ""},
		},
	)

	if err := ReconcileLegacySecretRBAC(client, testRBACNamespace, logrus.New()); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	var creates []string
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" {
			creates = append(creates, action.GetResource().Resource)
		}
	}
	if len(creates) != 2 || creates[0] != "clusterroles" || creates[1] != "clusterrolebindings" {
		t.Fatalf("expected ClusterRole create before ClusterRoleBinding create, got %v", creates)
	}

	role, err := client.RbacV1().ClusterRoles().Get(t.Context(), types.CSISecretRoleName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get created ClusterRole: %v", err)
	}
	if len(role.Rules) != 1 || len(role.Rules[0].APIGroups) != 1 || role.Rules[0].APIGroups[0] != "" || len(role.Rules[0].Resources) != 1 || role.Rules[0].Resources[0] != "secrets" || len(role.Rules[0].Verbs) != 1 || role.Rules[0].Verbs[0] != "get" {
		t.Fatalf("unexpected ClusterRole rules: %#v", role.Rules)
	}
	binding, err := client.RbacV1().ClusterRoleBindings().Get(t.Context(), types.CSISecretRoleBindingName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get created ClusterRoleBinding: %v", err)
	}
	if len(binding.Subjects) != 1 || binding.Subjects[0].Kind != "ServiceAccount" || binding.Subjects[0].Name != types.CSIControllerServiceAccountName || binding.Subjects[0].Namespace != testRBACNamespace {
		t.Fatalf("unexpected ClusterRoleBinding subjects: %#v", binding.Subjects)
	}
}

func TestReconcileLegacySecretRBACDoesNotAdoptUnownedBinding(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testRBACNamespace, UID: "namespace-uid"}},
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: "legacy-sc"},
			Provisioner: types.LonghornDriverName,
			Parameters:  map[string]string{"csi.storage.k8s.io/provisioner-secret-name": ""},
		},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: types.CSISecretRoleBindingName}},
	)

	if err := ReconcileLegacySecretRBAC(client, testRBACNamespace, logrus.New()); err == nil {
		t.Fatal("expected ownership collision")
	}
	for _, action := range client.Actions() {
		if action.GetVerb() == "create" || action.GetVerb() == "update" || action.GetVerb() == "delete" {
			t.Fatalf("ownership collision mutated RBAC with action %s %s", action.GetVerb(), action.GetResource().Resource)
		}
	}
}

func TestReconcileLegacySecretRBACCleanupDeletesBindingBeforeRole(t *testing.T) {
	ownedLabels := map[string]string{legacySecretRBACOwnerLabel: "namespace-uid"}
	client := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testRBACNamespace, UID: "namespace-uid"}},
		&rbacv1.ClusterRole{ObjectMeta: metav1.ObjectMeta{Name: types.CSISecretRoleName, Labels: ownedLabels}},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: types.CSISecretRoleBindingName, Labels: ownedLabels}},
	)

	if err := ReconcileLegacySecretRBAC(client, testRBACNamespace, logrus.New()); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}

	var deletes []string
	for _, action := range client.Actions() {
		if action.GetVerb() == "delete" {
			deletes = append(deletes, action.GetResource().Resource)
		}
	}
	if len(deletes) != 2 || deletes[0] != "clusterrolebindings" || deletes[1] != "clusterroles" {
		t.Fatalf("expected ClusterRoleBinding delete before ClusterRole delete, got %v", deletes)
	}
	if _, err := client.RbacV1().ClusterRoles().Get(t.Context(), types.CSISecretRoleName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected ClusterRole to be deleted, got %v", err)
	}
}

func TestReconcileLegacySecretRBACRemovesAggregation(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testRBACNamespace, UID: "namespace-uid"}},
		&storagev1.StorageClass{
			ObjectMeta:  metav1.ObjectMeta{Name: "legacy"},
			Provisioner: types.LonghornDriverName,
			Parameters:  map[string]string{legacyProvisionerSecretParameterPrefix + "name": "legacy-secret"},
		},
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{
				Name:   types.CSISecretRoleName,
				Labels: map[string]string{legacySecretRBACOwnerLabel: "namespace-uid"},
			},
			AggregationRule: &rbacv1.AggregationRule{
				ClusterRoleSelectors: []metav1.LabelSelector{{MatchLabels: map[string]string{"aggregate": "true"}}},
			},
			Rules: []rbacv1.PolicyRule{{APIGroups: []string{"*"}, Resources: []string{"*"}, Verbs: []string{"*"}}},
		},
	)
	if err := ReconcileLegacySecretRBAC(client, testRBACNamespace, logrus.New()); err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	role, err := client.RbacV1().ClusterRoles().Get(t.Context(), types.CSISecretRoleName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("failed to get reconciled ClusterRole: %v", err)
	}
	if role.AggregationRule != nil {
		t.Fatal("compatibility role can still aggregate permissions beyond Secret GET")
	}
}

func TestCleanupLegacySecretRBACPreservesForeignBindingButRevokesOwnedRole(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: testRBACNamespace, UID: "namespace-uid"}},
		&rbacv1.ClusterRole{
			ObjectMeta: metav1.ObjectMeta{
				Name:   types.CSISecretRoleName,
				Labels: map[string]string{legacySecretRBACOwnerLabel: "namespace-uid"},
			},
		},
		&rbacv1.ClusterRoleBinding{ObjectMeta: metav1.ObjectMeta{Name: types.CSISecretRoleBindingName}},
	)
	if err := CleanupLegacySecretRBAC(client, testRBACNamespace); err != nil {
		t.Fatalf("cleanup failed: %v", err)
	}
	if _, err := client.RbacV1().ClusterRoleBindings().Get(t.Context(), types.CSISecretRoleBindingName, metav1.GetOptions{}); err != nil {
		t.Fatalf("foreign ClusterRoleBinding was not preserved: %v", err)
	}
	if _, err := client.RbacV1().ClusterRoles().Get(t.Context(), types.CSISecretRoleName, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("owned ClusterRole was not revoked: %v", err)
	}
}
