package snapshothook

import (
	authorizationv1 "k8s.io/api/authorization/v1"

	"github.com/longhorn/longhorn-manager/hookexecutor"
	"github.com/longhorn/longhorn-manager/hookexecutor/podexec"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// ExecutorGrants lists the permissions the hook executor needs in a
// namespace it pauses pods in. The list must match the executor ClusterRole.
// Admission and the preview both use it, so they always agree.
func ExecutorGrants(namespace string) []authorizationv1.ResourceAttributes {
	return append(podexec.Grants(namespace), authorizationv1.ResourceAttributes{
		Namespace: namespace, Group: longhorn.SchemeGroupVersion.Group, Verb: "get", Resource: "volumehookpolicies",
	})
}

// VerifyExecutorAccess checks every executor grant in namespace.
func VerifyExecutorAccess(namespace, longhornNamespace string, allowed hookexecutor.AccessChecker) (*hookexecutor.AccessDenial, error) {
	return hookexecutor.VerifyAccess(namespace, longhornNamespace, ExecutorGrants(namespace), "next to the VolumeHookPolicy", allowed)
}
