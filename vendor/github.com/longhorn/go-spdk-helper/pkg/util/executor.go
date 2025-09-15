package util

import (
	commonns "github.com/longhorn/go-common-libs/ns"
	commontypes "github.com/longhorn/go-common-libs/types"
)

// NewExecutor creates a new namespaced executor
func NewExecutor(hostProc string) (*commonns.Executor, error) {
	namespaces := []commontypes.Namespace{commontypes.NamespaceMnt, commontypes.NamespaceIpc, commontypes.NamespaceNet}

	return commonns.NewNamespaceExecutor(commontypes.ProcessNone, hostProc, namespaces)
}
