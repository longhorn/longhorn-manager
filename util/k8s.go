package util

import (
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
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
