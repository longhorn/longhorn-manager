package util

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

func AddFinalizer(name string, obj runtime.Object) error {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	exists := false
	finalizers := metadata.GetFinalizers()
	for _, f := range finalizers {
		if f == name {
			exists = true
			break
		}
	}
	if !exists {
		metadata.SetFinalizers(append(finalizers, name))
	}

	return nil
}

func RemoveFinalizer(name string, obj runtime.Object) error {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		return err
	}

	var finalizers []string
	for _, finalizer := range metadata.GetFinalizers() {
		if finalizer == name {
			continue
		}
		finalizers = append(finalizers, finalizer)
	}
	metadata.SetFinalizers(finalizers)

	return nil
}

func FinalizerExists(name string, obj runtime.Object) bool {
	metadata, err := meta.Accessor(obj)
	if err != nil {
		utilruntime.HandleError(err)
		return false
	}
	for _, finalizer := range metadata.GetFinalizers() {
		if finalizer == name {
			return true
		}
	}
	return false
}

func GetNodeSelectorTermMatchExpressionNodeName(nodeName string) corev1.NodeSelectorTerm {
	return corev1.NodeSelectorTerm{
		MatchExpressions: []corev1.NodeSelectorRequirement{
			{
				Key:      corev1.LabelHostname,
				Operator: corev1.NodeSelectorOpIn,
				Values:   []string{nodeName},
			},
		},
	}
}

// GetImageOfDeploymentContainerWithName returns the image of the container with
// the given name from the specification of the deployment dpl. It assumes that
// a container with the given name exists and returns an empty string if there
// is no such container
func GetImageOfDeploymentContainerWithName(dpl *appsv1.Deployment, name string) string {
	for _, container := range dpl.Spec.Template.Spec.Containers {
		if container.Name == name {
			return container.Image
		}
	}
	return ""
}

func GetArgsOfDeploymentContainerWithName(dpl *appsv1.Deployment, name string) []string {
	for _, container := range dpl.Spec.Template.Spec.Containers {
		if container.Name == name {
			return container.Args
		}
	}
	return []string{}
}

func SetImageOfDeploymentContainerWithName(dpl *appsv1.Deployment, name, image string) error {
	idx, err := deploymentFindIndexOfContainerWithName(dpl, name)
	if err != nil {
		return err
	}
	dpl.Spec.Template.Spec.Containers[idx].Image = image
	return nil
}

func SetArgsOfDeploymentContainerWithName(dpl *appsv1.Deployment, name string, args []string) error {
	idx, err := deploymentFindIndexOfContainerWithName(dpl, name)
	if err != nil {
		return err
	}
	dpl.Spec.Template.Spec.Containers[idx].Args = args
	return nil
}

func deploymentFindIndexOfContainerWithName(dpl *appsv1.Deployment, name string) (int, error) {
	idx := int(-1)

	for i, container := range dpl.Spec.Template.Spec.Containers {
		if container.Name == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return idx, fmt.Errorf("could not find container with name %v", name)
	}
	return idx, nil
}
