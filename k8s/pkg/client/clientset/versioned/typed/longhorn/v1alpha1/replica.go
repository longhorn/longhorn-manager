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

package v1alpha1

import (
	v1alpha1 "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	scheme "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned/scheme"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	types "k8s.io/apimachinery/pkg/types"
	watch "k8s.io/apimachinery/pkg/watch"
	rest "k8s.io/client-go/rest"
)

// ReplicasGetter has a method to return a ReplicaInterface.
// A group's client should implement this interface.
type ReplicasGetter interface {
	Replicas(namespace string) ReplicaInterface
}

// ReplicaInterface has methods to work with Replica resources.
type ReplicaInterface interface {
	Create(*v1alpha1.Replica) (*v1alpha1.Replica, error)
	Update(*v1alpha1.Replica) (*v1alpha1.Replica, error)
	Delete(name string, options *v1.DeleteOptions) error
	DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error
	Get(name string, options v1.GetOptions) (*v1alpha1.Replica, error)
	List(opts v1.ListOptions) (*v1alpha1.ReplicaList, error)
	Watch(opts v1.ListOptions) (watch.Interface, error)
	Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.Replica, err error)
	ReplicaExpansion
}

// replicas implements ReplicaInterface
type replicas struct {
	client rest.Interface
	ns     string
}

// newReplicas returns a Replicas
func newReplicas(c *LonghornV1alpha1Client, namespace string) *replicas {
	return &replicas{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the replica, and returns the corresponding replica object, and an error if there is any.
func (c *replicas) Get(name string, options v1.GetOptions) (result *v1alpha1.Replica, err error) {
	result = &v1alpha1.Replica{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("replicas").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do().
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of Replicas that match those selectors.
func (c *replicas) List(opts v1.ListOptions) (result *v1alpha1.ReplicaList, err error) {
	result = &v1alpha1.ReplicaList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("replicas").
		VersionedParams(&opts, scheme.ParameterCodec).
		Do().
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested replicas.
func (c *replicas) Watch(opts v1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("replicas").
		VersionedParams(&opts, scheme.ParameterCodec).
		Watch()
}

// Create takes the representation of a replica and creates it.  Returns the server's representation of the replica, and an error, if there is any.
func (c *replicas) Create(replica *v1alpha1.Replica) (result *v1alpha1.Replica, err error) {
	result = &v1alpha1.Replica{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("replicas").
		Body(replica).
		Do().
		Into(result)
	return
}

// Update takes the representation of a replica and updates it. Returns the server's representation of the replica, and an error, if there is any.
func (c *replicas) Update(replica *v1alpha1.Replica) (result *v1alpha1.Replica, err error) {
	result = &v1alpha1.Replica{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("replicas").
		Name(replica.Name).
		Body(replica).
		Do().
		Into(result)
	return
}

// Delete takes name of the replica and deletes it. Returns an error if one occurs.
func (c *replicas) Delete(name string, options *v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("replicas").
		Name(name).
		Body(options).
		Do().
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *replicas) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("replicas").
		VersionedParams(&listOptions, scheme.ParameterCodec).
		Body(options).
		Do().
		Error()
}

// Patch applies the patch and returns the patched replica.
func (c *replicas) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.Replica, err error) {
	result = &v1alpha1.Replica{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("replicas").
		SubResource(subresources...).
		Name(name).
		Body(data).
		Do().
		Into(result)
	return
}
