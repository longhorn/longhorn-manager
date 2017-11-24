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

// SettingsGetter has a method to return a SettingInterface.
// A group's client should implement this interface.
type SettingsGetter interface {
	Settings(namespace string) SettingInterface
}

// SettingInterface has methods to work with Setting resources.
type SettingInterface interface {
	Create(*v1alpha1.Setting) (*v1alpha1.Setting, error)
	Update(*v1alpha1.Setting) (*v1alpha1.Setting, error)
	Delete(name string, options *v1.DeleteOptions) error
	DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error
	Get(name string, options v1.GetOptions) (*v1alpha1.Setting, error)
	List(opts v1.ListOptions) (*v1alpha1.SettingList, error)
	Watch(opts v1.ListOptions) (watch.Interface, error)
	Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.Setting, err error)
	SettingExpansion
}

// settings implements SettingInterface
type settings struct {
	client rest.Interface
	ns     string
}

// newSettings returns a Settings
func newSettings(c *LonghornV1alpha1Client, namespace string) *settings {
	return &settings{
		client: c.RESTClient(),
		ns:     namespace,
	}
}

// Get takes name of the setting, and returns the corresponding setting object, and an error if there is any.
func (c *settings) Get(name string, options v1.GetOptions) (result *v1alpha1.Setting, err error) {
	result = &v1alpha1.Setting{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("settings").
		Name(name).
		VersionedParams(&options, scheme.ParameterCodec).
		Do().
		Into(result)
	return
}

// List takes label and field selectors, and returns the list of Settings that match those selectors.
func (c *settings) List(opts v1.ListOptions) (result *v1alpha1.SettingList, err error) {
	result = &v1alpha1.SettingList{}
	err = c.client.Get().
		Namespace(c.ns).
		Resource("settings").
		VersionedParams(&opts, scheme.ParameterCodec).
		Do().
		Into(result)
	return
}

// Watch returns a watch.Interface that watches the requested settings.
func (c *settings) Watch(opts v1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return c.client.Get().
		Namespace(c.ns).
		Resource("settings").
		VersionedParams(&opts, scheme.ParameterCodec).
		Watch()
}

// Create takes the representation of a setting and creates it.  Returns the server's representation of the setting, and an error, if there is any.
func (c *settings) Create(setting *v1alpha1.Setting) (result *v1alpha1.Setting, err error) {
	result = &v1alpha1.Setting{}
	err = c.client.Post().
		Namespace(c.ns).
		Resource("settings").
		Body(setting).
		Do().
		Into(result)
	return
}

// Update takes the representation of a setting and updates it. Returns the server's representation of the setting, and an error, if there is any.
func (c *settings) Update(setting *v1alpha1.Setting) (result *v1alpha1.Setting, err error) {
	result = &v1alpha1.Setting{}
	err = c.client.Put().
		Namespace(c.ns).
		Resource("settings").
		Name(setting.Name).
		Body(setting).
		Do().
		Into(result)
	return
}

// Delete takes name of the setting and deletes it. Returns an error if one occurs.
func (c *settings) Delete(name string, options *v1.DeleteOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("settings").
		Name(name).
		Body(options).
		Do().
		Error()
}

// DeleteCollection deletes a collection of objects.
func (c *settings) DeleteCollection(options *v1.DeleteOptions, listOptions v1.ListOptions) error {
	return c.client.Delete().
		Namespace(c.ns).
		Resource("settings").
		VersionedParams(&listOptions, scheme.ParameterCodec).
		Body(options).
		Do().
		Error()
}

// Patch applies the patch and returns the patched setting.
func (c *settings) Patch(name string, pt types.PatchType, data []byte, subresources ...string) (result *v1alpha1.Setting, err error) {
	result = &v1alpha1.Setting{}
	err = c.client.Patch(pt).
		Namespace(c.ns).
		Resource("settings").
		SubResource(subresources...).
		Name(name).
		Body(data).
		Do().
		Into(result)
	return
}
