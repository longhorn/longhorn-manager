package csi

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/sirupsen/logrus"

	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	kubernetes "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/types"
)

const (
	legacyProvisionerSecretParameterPrefix = "csi.storage.k8s.io/provisioner-secret-"
	legacySecretRBACOwnerLabel             = "longhorn.io/csi-secret-rbac-namespace-uid"
)

// ReconcileLegacySecretRBAC grants the CSI controller sidecars access to
// Secrets only while an existing Longhorn StorageClass uses the historical
// provisioner Secret parameters. The StorageClass list is intentionally read
// before any RBAC mutation so an API failure cannot leave partial state.
func ReconcileLegacySecretRBAC(kubeClient kubernetes.Interface, namespace string, logger logrus.FieldLogger) error {
	storageClasses, err := kubeClient.StorageV1().StorageClasses().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("failed to list StorageClasses for CSI Secret compatibility: %w", err)
	}

	legacyStorageClasses := legacySecretStorageClasses(storageClasses)
	if len(legacyStorageClasses) > 0 {
		if err := ensureLegacySecretRBAC(kubeClient, namespace); err != nil {
			return err
		}
		logger.Warnf("Longhorn StorageClasses %v use legacy CSI provisioner Secret parameters; clean up those StorageClasses and restart the Longhorn driver deployer after cleanup to revoke this compatibility access", legacyStorageClasses)
		return nil
	}

	return CleanupLegacySecretRBAC(kubeClient, namespace)
}

func legacySecretStorageClasses(storageClasses *storagev1.StorageClassList) []string {
	var names []string
	for _, storageClass := range storageClasses.Items {
		if storageClass.Provisioner != types.LonghornDriverName {
			continue
		}
		for parameter := range storageClass.Parameters {
			if strings.HasPrefix(parameter, legacyProvisionerSecretParameterPrefix) {
				names = append(names, storageClass.Name)
				break
			}
		}
	}
	slices.Sort(names)
	return names
}

func ensureLegacySecretRBAC(kubeClient kubernetes.Interface, namespace string) error {
	namespaceObject, err := kubeClient.CoreV1().Namespaces().Get(context.TODO(), namespace, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get Longhorn namespace for CSI Secret RBAC ownership: %w", err)
	}

	bindings := kubeClient.RbacV1().ClusterRoleBindings()
	binding, bindingErr := bindings.Get(context.TODO(), types.CSISecretRoleBindingName, metav1.GetOptions{})
	if bindingErr != nil && !apierrors.IsNotFound(bindingErr) {
		return fmt.Errorf("failed to get CSI Secret compatibility ClusterRoleBinding: %w", bindingErr)
	}
	if bindingErr == nil && !ownedByNamespace(binding, namespaceObject.UID) {
		return fmt.Errorf("CSI Secret compatibility ClusterRoleBinding %q is not owned by this Longhorn installation", types.CSISecretRoleBindingName)
	}

	roles := kubeClient.RbacV1().ClusterRoles()
	role, roleErr := roles.Get(context.TODO(), types.CSISecretRoleName, metav1.GetOptions{})
	if roleErr != nil && !apierrors.IsNotFound(roleErr) {
		return fmt.Errorf("failed to get CSI Secret compatibility ClusterRole: %w", roleErr)
	}
	if roleErr == nil {
		if !ownedByNamespace(role, namespaceObject.UID) {
			return fmt.Errorf("CSI Secret compatibility ClusterRole %q is not owned by this Longhorn installation", types.CSISecretRoleName)
		}
		role.Rules = secretRoleRules()
		role.AggregationRule = nil
		setNamespaceOwner(role, namespace, namespaceObject.UID)
		if _, err := roles.Update(context.TODO(), role, metav1.UpdateOptions{}); err != nil {
			return fmt.Errorf("failed to update CSI Secret compatibility ClusterRole: %w", err)
		}
	} else if _, err := roles.Create(context.TODO(), desiredClusterRole(namespace, namespaceObject.UID), metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("failed to create CSI Secret compatibility ClusterRole: %w", err)
	}

	if apierrors.IsNotFound(bindingErr) {
		if _, err := bindings.Create(context.TODO(), desiredClusterRoleBinding(namespace, namespaceObject.UID), metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to create CSI Secret compatibility ClusterRoleBinding: %w", err)
		}
		return nil
	}
	desiredBinding := desiredClusterRoleBinding(namespace, namespaceObject.UID)
	if binding.RoleRef != desiredBinding.RoleRef {
		if err := bindings.Delete(context.TODO(), types.CSISecretRoleBindingName, metav1.DeleteOptions{
			Preconditions: &metav1.Preconditions{UID: &binding.UID, ResourceVersion: &binding.ResourceVersion},
		}); err != nil {
			return fmt.Errorf("failed to replace CSI Secret compatibility ClusterRoleBinding: %w", err)
		}
		if _, err := bindings.Create(context.TODO(), desiredBinding, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("failed to recreate CSI Secret compatibility ClusterRoleBinding: %w", err)
		}
		return nil
	}
	binding.Subjects = desiredBinding.Subjects
	setNamespaceOwner(binding, namespace, namespaceObject.UID)
	if _, err := bindings.Update(context.TODO(), binding, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("failed to update CSI Secret compatibility ClusterRoleBinding: %w", err)
	}
	return nil
}

// CleanupLegacySecretRBAC revokes only this installation's compatibility resources.
func CleanupLegacySecretRBAC(kubeClient kubernetes.Interface, namespace string) error {
	namespaceObject, err := kubeClient.CoreV1().Namespaces().Get(context.TODO(), namespace, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get Longhorn namespace for CSI Secret RBAC cleanup: %w", err)
	}

	bindings := kubeClient.RbacV1().ClusterRoleBindings()
	binding, err := bindings.Get(context.TODO(), types.CSISecretRoleBindingName, metav1.GetOptions{})
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to get CSI Secret compatibility ClusterRoleBinding for cleanup: %w", err)
	}
	if err == nil {
		if ownedByNamespace(binding, namespaceObject.UID) {
			if err := bindings.Delete(context.TODO(), types.CSISecretRoleBindingName, metav1.DeleteOptions{
				Preconditions: &metav1.Preconditions{UID: &binding.UID, ResourceVersion: &binding.ResourceVersion},
			}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete CSI Secret compatibility ClusterRoleBinding: %w", err)
			}
		} else {
			logrus.Warnf("Leaving CSI Secret compatibility ClusterRoleBinding %q unchanged because it is not owned by this Longhorn installation", binding.Name)
		}
	}

	roles := kubeClient.RbacV1().ClusterRoles()
	role, err := roles.Get(context.TODO(), types.CSISecretRoleName, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("failed to get CSI Secret compatibility ClusterRole for cleanup: %w", err)
	}
	if !ownedByNamespace(role, namespaceObject.UID) {
		logrus.Warnf("Leaving CSI Secret compatibility ClusterRole %q unchanged because it is not owned by this Longhorn installation", role.Name)
		return nil
	}
	if err := roles.Delete(context.TODO(), types.CSISecretRoleName, metav1.DeleteOptions{
		Preconditions: &metav1.Preconditions{UID: &role.UID, ResourceVersion: &role.ResourceVersion},
	}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete CSI Secret compatibility ClusterRole: %w", err)
	}
	return nil
}

func desiredClusterRole(namespace string, namespaceUID k8stypes.UID) *rbacv1.ClusterRole {
	role := &rbacv1.ClusterRole{
		ObjectMeta: metav1.ObjectMeta{
			Name: types.CSISecretRoleName,
		},
		Rules: secretRoleRules(),
	}
	setNamespaceOwner(role, namespace, namespaceUID)
	return role
}

func desiredClusterRoleBinding(namespace string, namespaceUID k8stypes.UID) *rbacv1.ClusterRoleBinding {
	binding := &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: types.CSISecretRoleBindingName,
		},
		RoleRef: rbacv1.RoleRef{
			APIGroup: "rbac.authorization.k8s.io",
			Kind:     "ClusterRole",
			Name:     types.CSISecretRoleName,
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      types.CSIControllerServiceAccountName,
			Namespace: namespace,
		}},
	}
	setNamespaceOwner(binding, namespace, namespaceUID)
	return binding
}

func secretRoleRules() []rbacv1.PolicyRule {
	return []rbacv1.PolicyRule{{
		APIGroups: []string{""},
		Resources: []string{"secrets"},
		Verbs:     []string{"get"},
	}}
}

func ownedByNamespace(object metav1.Object, namespaceUID k8stypes.UID) bool {
	return object.GetLabels()[legacySecretRBACOwnerLabel] == string(namespaceUID)
}

func setNamespaceOwner(object metav1.Object, namespace string, namespaceUID k8stypes.UID) {
	labels := object.GetLabels()
	if labels == nil {
		labels = map[string]string{}
	}
	labels[legacySecretRBACOwnerLabel] = string(namespaceUID)
	object.SetLabels(labels)
	object.SetOwnerReferences([]metav1.OwnerReference{{
		APIVersion: "v1",
		Kind:       "Namespace",
		Name:       namespace,
		UID:        namespaceUID,
	}})
}
