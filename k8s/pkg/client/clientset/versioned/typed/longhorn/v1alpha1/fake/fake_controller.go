/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package fake

import (
	v1alpha1 "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	labels "k8s.io/apimachinery/pkg/labels"
	schema "k8s.io/apimachinery/pkg/runtime/schema"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	testing "k8s.io/client-go/testing"
)

// FakeControllers implements ControllerInterface
type FakeControllers struct {
	Fake *FakeLonghornV1alpha1
	ns   string
}

var controllersResource = schema.GroupVersionResource{Group: "longhorn.rancher.io", Version: "v1alpha1", Resource: "controllers"}

var controllersKind = schema.GroupVersionKind{Group: "longhorn.rancher.io", Version: "v1alpha1", Kind: "Controller"}

// Get takes name of the controller, and returns the corresponding controller object, and an error if there is any.
func (c *FakeControllers) Get(name string, options v1.GetOptions) (result *v1alpha1.Controller, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(controllersResource, c.ns, name), &v1alpha1.Controller{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Controller), err
}

// List takes label and field selectors, and returns the list of Controllers that match those selectors.
func (c *FakeControllers) List(opts v1.ListOptions) (result *v1alpha1.ControllerList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(controllersResource, controllersKind, c.ns, opts), &v1alpha1.ControllerList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha1.ControllerList{}
	for _, item := range obj.(*v1alpha1.ControllerList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested controllers.
func (c *FakeControllers) Watch(opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(controllersResource, c.ns, opts))

}

// Create takes the representation of a controller and creates it.  Returns the server's representation of the controller, and an error, if there is any.
func (c *FakeControllers) Create(controller *v1alpha1.Controller) (result *v1alpha1.Controller, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(controllersResource, c.ns, controller), &v1alpha1.Controller{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Controller), err
}

// Update takes the representation of a controller and updates it. Returns the server's representation of the controller, and an error, if there is any.
func (c *FakeControllers) Update(controller *v1alpha1.Controller) (result *v1alpha1.Controller, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(controllersResource, c.ns, controller), &v1alpha1.Controller{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Controller), err
}

// Delete takes name of the controller and deletes it. Returns an error if one occurs.
func (c *FakeControllers) Delete(name string, options *v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteAction(controllersResource, c.ns, name), &v1alpha1.Controller{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeControllers) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(controllersResource, c.ns, listOptions)

	_, err := c.Fake.Invokes(action, &v1alpha1.ControllerList{})
	return err
}

// Patch applies the patch and returns the patched controller.
func (c *FakeControllers) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.Controller, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(controllersResource, c.ns, name, data, subresources...), &v1alpha1.Controller{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Controller), err
}
