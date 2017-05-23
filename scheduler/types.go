package scheduler

type ResourceManager interface {
	ListSchedulingNodes() (map[string]*Node, error)
}

type Spec struct {
	Size int64
}

type PolicyBindingType string

const (
	PolicyBindingTypeSoftAntiAffinity = PolicyBindingType("soft.anti-affinity")
)

type Policy struct {
	Binding PolicyBindingType
	NodeIDs map[string]struct{}
}

type Node struct {
	ID string
}
