// Package hookexecutor runs hook commands inside application pods. It may
// only act in a namespace where an admin has granted access by applying a
// RoleBinding; this package checks that grant.
package hookexecutor

import (
	"fmt"

	authorizationv1 "k8s.io/api/authorization/v1"

	"github.com/longhorn/longhorn-manager/types"
)

// AccessChecker asks the API server whether the hook executor is allowed
// to perform the given action.
type AccessChecker func(attributes authorizationv1.ResourceAttributes) (allowed bool, err error)

// AccessDenial reports one permission the hook executor is missing.
type AccessDenial struct {
	// Reason states the missing permission and where to apply the
	// RoleBinding that grants it.
	Reason string
	// Manifest is the RoleBinding YAML that grants the permission.
	Manifest string
}

// Message returns the reason followed by the manifest. Admission uses it
// as the rejection message.
func (d *AccessDenial) Message() string {
	return d.Reason + ":\n" + d.Manifest
}

// VerifyAccess reports whether the hook executor has every permission it
// needs in namespace. It returns nil when nothing is missing. Otherwise it
// returns a denial that describes the first missing permission and tells
// the admin to apply the RoleBinding at placement. An error means a check
// did not complete; treat it as a denial.
func VerifyAccess(namespace, longhornNamespace string, grants []authorizationv1.ResourceAttributes, placement string, allowed AccessChecker) (denial *AccessDenial, err error) {
	for _, grant := range grants {
		action := grant.Verb + " " + grant.Resource
		if grant.Subresource != "" {
			action += "/" + grant.Subresource
		}
		ok, err := allowed(grant)
		if err != nil {
			return nil, fmt.Errorf("could not verify that the hook executor may %v in namespace %v: %w", action, namespace, err)
		}
		if !ok {
			return &AccessDenial{
				Reason:   fmt.Sprintf("the hook executor may not %v in namespace %v; grant it by applying this RoleBinding %v", action, namespace, placement),
				Manifest: RoleBindingManifest(namespace, longhornNamespace),
			}, nil
		}
	}
	return nil, nil
}

// RoleBindingManifest renders the RoleBinding that grants the hook executor
// access in namespace. It appears wherever a missing grant is reported, so
// the admin always gets the same fix.
func RoleBindingManifest(namespace, longhornNamespace string) string {
	return fmt.Sprintf(`apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: %s
  namespace: %s
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: %s}
subjects:
- {kind: ServiceAccount, name: %s, namespace: %s}
`, types.HookExecutorRoleBindingName, namespace, types.HookExecutorClusterRoleName, types.HookExecutorServiceAccountName, longhornNamespace)
}
