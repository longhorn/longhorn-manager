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

// FakeVolumes implements VolumeInterface
type FakeVolumes struct {
	Fake *FakeLonghornV1alpha1
	ns   string
}

var volumesResource = schema.GroupVersionResource{Group: "longhorn.rancher.io", Version: "v1alpha1", Resource: "volumes"}

var volumesKind = schema.GroupVersionKind{Group: "longhorn.rancher.io", Version: "v1alpha1", Kind: "Volume"}

// Get takes name of the volume, and returns the corresponding volume object, and an error if there is any.
func (c *FakeVolumes) Get(name string, options v1.GetOptions) (result *v1alpha1.Volume, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewGetAction(volumesResource, c.ns, name), &v1alpha1.Volume{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Volume), err
}

// List takes label and field selectors, and returns the list of Volumes that match those selectors.
func (c *FakeVolumes) List(opts v1.ListOptions) (result *v1alpha1.VolumeList, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewListAction(volumesResource, volumesKind, c.ns, opts), &v1alpha1.VolumeList{})

	if obj == nil {
		return nil, err
	}

	label, _, _ := testing.ExtractFromListOptions(opts)
	if label == nil {
		label = labels.Everything()
	}
	list := &v1alpha1.VolumeList{}
	for _, item := range obj.(*v1alpha1.VolumeList).Items {
		if label.Matches(labels.Set(item.Labels)) {
			list.Items = append(list.Items, item)
		}
	}
	return list, err
}

// Watch returns a watch.Interface that watches the requested volumes.
func (c *FakeVolumes) Watch(opts v1.ListOptions) (watch.Interface, error) {
	return c.Fake.
		InvokesWatch(testing.NewWatchAction(volumesResource, c.ns, opts))

}

// Create takes the representation of a volume and creates it.  Returns the server's representation of the volume, and an error, if there is any.
func (c *FakeVolumes) Create(volume *v1alpha1.Volume) (result *v1alpha1.Volume, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewCreateAction(volumesResource, c.ns, volume), &v1alpha1.Volume{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Volume), err
}

// Update takes the representation of a volume and updates it. Returns the server's representation of the volume, and an error, if there is any.
func (c *FakeVolumes) Update(volume *v1alpha1.Volume) (result *v1alpha1.Volume, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateAction(volumesResource, c.ns, volume), &v1alpha1.Volume{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Volume), err
}

// UpdateStatus was generated because the type contains a Status member.
// Add a +genclient:noStatus comment above the type to avoid generating UpdateStatus().
func (c *FakeVolumes) UpdateStatus(volume *v1alpha1.Volume) (*v1alpha1.Volume, error) {
	obj, err := c.Fake.
		Invokes(testing.NewUpdateSubresourceAction(volumesResource, "status", c.ns, volume), &v1alpha1.Volume{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Volume), err
}

// Delete takes name of the volume and deletes it. Returns an error if one occurs.
func (c *FakeVolumes) Delete(name string, options *v1.DeleteOptions) error {
	_, err := c.Fake.
		Invokes(testing.NewDeleteAction(volumesResource, c.ns, name), &v1alpha1.Volume{})

	return err
}

// DeleteCollection deletes a collection of objects.
func (c *FakeVolumes) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	action := testing.NewDeleteCollectionAction(volumesResource, c.ns, listOptions)

	_, err := c.Fake.Invokes(action, &v1alpha1.VolumeList{})
	return err
}

// Patch applies the patch and returns the patched volume.
func (c *FakeVolumes) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.Volume, err error) {
	obj, err := c.Fake.
		Invokes(testing.NewPatchSubresourceAction(volumesResource, c.ns, name, data, subresources...), &v1alpha1.Volume{})

	if obj == nil {
		return nil, err
	}
	return obj.(*v1alpha1.Volume), err
}
