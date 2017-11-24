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

// ControllersGetter has a method to return a ControllerInterface.
// A group's client should implement this interface.
type ControllersGetter interface {
	Controllers(namespace string) ControllerInterface
}

// ControllerInterface has methods to work with Controller resources.
type ControllerInterface interface {
	Create(*v1alpha1.Controller) (*v1alpha1.Controller, error)
	Update(*v1alpha1.Controller) (*v1alpha1.Controller, error)
	Delete(name string, options *v1.DeleteOptions) error
	DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error
	Get(name string, options v1.GetOptions) (*v1alpha1.Controller, error)
	List(opts v1.ListOptions) (*v1alpha1.ControllerList, error)
	Watch(opts v1.ListOptions) (watch.Interface, error)
	Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.Controller, err error)
	ControllerExpansion
}

// controllers implements ControllerInterface
type controllers struct {
	client rest.Interface
	ns     string
}

// newControllers returns a Controllers
func newControllers(c *LonghornV1alpha1Client, namespace string) *controllers {
	return &controllers{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the controller, and returns the corresponding controller object, and an error if there is any.
func (c *controllers) Get(name string, options v1.GetOptions) (result *v1alpha1.Controller, err error) {
	result = &v1alpha1.Controller{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("controllers").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do().
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of Controllers that match those selectors.
func (c *controllers) List(opts v1.ListOptions) (result *v1alpha1.ControllerList, err error) {
	result = &v1alpha1.ControllerList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("controllers").
		VersionedParams(&opts, scheme.ParameterCodec).
		Do().
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested controllers.
func (c *controllers) Watch(opts v1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("controllers").
		VersionedParams(&opts, scheme.ParameterCodec).
		Watch()
}

// Create takes the representation of a controller and creates it.  Returns the server's representation of the controller, and an error, if there is any.
func (c *controllers) Create(controller *v1alpha1.Controller) (result *v1alpha1.Controller, err error) {
	result = &v1alpha1.Controller{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("controllers").
		Body(controller).
		Do().
		Into(result)
	return
}

// Update takes the representation of a controller and updates it. Returns the server's representation of the controller, and an error, if there is any.
func (c *controllers) Update(controller *v1alpha1.Controller) (result *v1alpha1.Controller, err error) {
	result = &v1alpha1.Controller{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("controllers").
		Name(controller.Name).
		Body(controller).
		Do().
		Into(result)
	return
}

// Delete takes name of the controller and deletes it. Returns an error if one occurs.
func (c *controllers) Delete(name string, options *v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("controllers").
		Name(name).
		Body(options).
		Do().
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *controllers) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("controllers").
		VersionedParams(&listOptions, scheme.ParameterCodec).
		Body(options).
		Do().
		Error()
}

// Patch applies the patch and returns the patched controller.
func (c *controllers) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.Controller, err error) {
	result = &v1alpha1.Controller{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("controllers").
		SubResource(subresources...).
		Name(name).
		Body(data).
		Do().
		Into(result)
	return
}
