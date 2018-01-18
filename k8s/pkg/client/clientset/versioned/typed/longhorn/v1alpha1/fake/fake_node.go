/*
Copyright 2018 The Kubernetes Authors.

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

// FakeNodes implements NodeInterface
type FakeNodes struct {
	Fake *FakeLonghornV1alpha1
	ns   string
}

var nodesResource = schema.GroupVersionResource{Group: "longhorn.rancher.io", Version: "v1alpha1", Resource: "nodes"}

var nodesKind = schema.GroupVersionKind{Group: "longhorn.rancher.io", Version: "v1alpha1", Kind: "Node"}

// Get takes name of the node, and returns the corresponding node object, and an error if there is any.
func (c *FakeNodes) Get(name string, options v1.GetOptions) (result *v1alpha1.Node, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(nodesResource, c.ns, name), &v1alpha1.Node{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Node), err
}

// List takes label and field selectors, and returns the list of Nodes that match those selectors.
func (c *FakeNodes) List(opts v1.ListOptions) (result *v1alpha1.NodeList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(nodesResource, nodesKind, c.ns, opts), &v1alpha1.NodeList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha1.NodeList{}
	for _, item := range obj.(*v1alpha1.NodeList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested nodes.
func (c *FakeNodes) Watch(opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(nodesResource, c.ns, opts))

}

// Create takes the representation of a node and creates it.  Returns the server's representation of the node, and an error, if there is any.
func (c *FakeNodes) Create(node *v1alpha1.Node) (result *v1alpha1.Node, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(nodesResource, c.ns, node), &v1alpha1.Node{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Node), err
}

// Update takes the representation of a node and updates it. Returns the server's representation of the node, and an error, if there is any.
func (c *FakeNodes) Update(node *v1alpha1.Node) (result *v1alpha1.Node, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(nodesResource, c.ns, node), &v1alpha1.Node{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Node), err
}

// Delete takes name of the node and deletes it. Returns an error if one occurs.
func (c *FakeNodes) Delete(name string, options *v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteAction(nodesResource, c.ns, name), &v1alpha1.Node{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeNodes) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(nodesResource, c.ns, listOptions)

	_, err := c.Fake.Invokes(action, &v1alpha1.NodeList{})
	return err
}

// Patch applies the patch and returns the patched node.
func (c *FakeNodes) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.Node, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(nodesResource, c.ns, name, data, subresources...), &v1alpha1.Node{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Node), err
}
