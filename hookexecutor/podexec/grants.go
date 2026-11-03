package podexec

import (
	authorizationv1 "k8s.io/api/authorization/v1"
)

// Grants lists the permissions the pods/exec transport needs in every
// namespace it runs commands in. Each hook kind adds the permissions it
// needs to look up its own resources, and admission and the preview check
// the combined list.
//
// pods/exec needs both create and get: the SPDY transport sends a POST,
// which the API server authorizes as create, while the WebSocket transport
// sends a GET, which the API server authorizes as either create or get
// depending on its version and configuration.
//
// The permissions listed here, the calls the executor makes at runtime,
// and the rules of the shipped ClusterRole types.HookExecutorClusterRoleName
// must all match. Longhorn only ships the ClusterRole; a namespace admin
// grants it by applying a RoleBinding in the application namespace. If the
// ClusterRole lacks a permission listed here, admission rejects even a
// correct RoleBinding. If a runtime call is missing from this list,
// admission passes but the executor later fails with a Forbidden error it
// retries forever.
func Grants(namespace string) []authorizationv1.ResourceAttributes {
	return []authorizationv1.ResourceAttributes{
		{Namespace: namespace, Verb: "create", Resource: "pods", Subresource: "exec"},
		{Namespace: namespace, Verb: "get", Resource: "pods", Subresource: "exec"},
		{Namespace: namespace, Verb: "list", Resource: "pods"},
	}
}
