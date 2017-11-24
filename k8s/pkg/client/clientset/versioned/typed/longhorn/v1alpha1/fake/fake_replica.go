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

// FakeReplicas implements ReplicaInterface
type FakeReplicas struct {
	Fake *FakeLonghornV1alpha1
	ns   string
}

var replicasResource = schema.GroupVersionResource{Group: "longhorn.rancher.io", Version: "v1alpha1", Resource: "replicas"}

var replicasKind = schema.GroupVersionKind{Group: "longhorn.rancher.io", Version: "v1alpha1", Kind: "Replica"}

// Get takes name of the replica, and returns the corresponding replica object, and an error if there is any.
func (c *FakeReplicas) Get(name string, options v1.GetOptions) (result *v1alpha1.Replica, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(replicasResource, c.ns, name), &v1alpha1.Replica{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Replica), err
}

// List takes label and field selectors, and returns the list of Replicas that match those selectors.
func (c *FakeReplicas) List(opts v1.ListOptions) (result *v1alpha1.ReplicaList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(replicasResource, replicasKind, c.ns, opts), &v1alpha1.ReplicaList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha1.ReplicaList{}
	for _, item := range obj.(*v1alpha1.ReplicaList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested replicas.
func (c *FakeReplicas) Watch(opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(replicasResource, c.ns, opts))

}

// Create takes the representation of a replica and creates it.  Returns the server's representation of the replica, and an error, if there is any.
func (c *FakeReplicas) Create(replica *v1alpha1.Replica) (result *v1alpha1.Replica, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(replicasResource, c.ns, replica), &v1alpha1.Replica{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Replica), err
}

// Update takes the representation of a replica and updates it. Returns the server's representation of the replica, and an error, if there is any.
func (c *FakeReplicas) Update(replica *v1alpha1.Replica) (result *v1alpha1.Replica, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(replicasResource, c.ns, replica), &v1alpha1.Replica{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Replica), err
}

// Delete takes name of the replica and deletes it. Returns an error if one occurs.
func (c *FakeReplicas) Delete(name string, options *v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteAction(replicasResource, c.ns, name), &v1alpha1.Replica{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeReplicas) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(replicasResource, c.ns, listOptions)

	_, err := c.Fake.Invokes(action, &v1alpha1.ReplicaList{})
	return err
}

// Patch applies the patch and returns the patched replica.
func (c *FakeReplicas) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.Replica, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(replicasResource, c.ns, name, data, subresources...), &v1alpha1.Replica{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Replica), err
}
