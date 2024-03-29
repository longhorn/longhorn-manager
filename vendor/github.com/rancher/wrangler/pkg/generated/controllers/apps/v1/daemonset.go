/*
Copyright The Kubernetes Authors.

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

// Code generated by main. DO NOT EDIT.

package v1

import (
	"context"
	"sync"
	"time"

	"github.com/rancher/lasso/pkg/client"
	"github.com/rancher/lasso/pkg/controller"
	"github.com/rancher/wrangler/pkg/apply"
	"github.com/rancher/wrangler/pkg/condition"
	"github.com/rancher/wrangler/pkg/generic"
	"github.com/rancher/wrangler/pkg/kv"
	v1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

type DaemonSetHandler func(string, *v1.DaemonSet) (*v1.DaemonSet, error)

type DaemonSetController interface {
	generic.ControllerMeta
	DaemonSetClient

	OnChange(ctx context.Context, name string, sync DaemonSetHandler)
	OnRemove(ctx context.Context, name string, sync DaemonSetHandler)
	Enqueue(namespace, name string)
	EnqueueAfter(namespace, name string, duration time.Duration)

	Cache() DaemonSetCache
}

type DaemonSetClient interface {
	Create(*v1.DaemonSet) (*v1.DaemonSet, error)
	Update(*v1.DaemonSet) (*v1.DaemonSet, error)
	UpdateStatus(*v1.DaemonSet) (*v1.DaemonSet, error)
	Delete(namespace, name string, options *metav1.DeleteOptions) error
	Get(namespace, name string, options metav1.GetOptions) (*v1.DaemonSet, error)
	List(namespace string, opts metav1.ListOptions) (*v1.DaemonSetList, error)
	Watch(namespace string, opts metav1.ListOptions) (watch.Interface, error)
	Patch(namespace, name string, pt types.PatchType, data []byte, subresources ...string) (result *v1.DaemonSet, err error)
}

type DaemonSetCache interface {
	Get(namespace, name string) (*v1.DaemonSet, error)
	List(namespace string, selector labels.Selector) ([]*v1.DaemonSet, error)

	AddIndexer(indexName string, indexer DaemonSetIndexer)
	GetByIndex(indexName, key string) ([]*v1.DaemonSet, error)
}

type DaemonSetIndexer func(obj *v1.DaemonSet) ([]string, error)

type daemonSetController struct {
	controller    controller.SharedController
	client        *client.Client
	gvk           schema.GroupVersionKind
	groupResource schema.GroupResource
}

func NewDaemonSetController(gvk schema.GroupVersionKind, resource string, namespaced bool, controller controller.SharedControllerFactory) DaemonSetController {
	c := controller.ForResourceKind(gvk.GroupVersion().WithResource(resource), gvk.Kind, namespaced)
	return &daemonSetController{
		controller: c,
		client:     c.Client(),
		gvk:        gvk,
		groupResource: schema.GroupResource{
			Group:    gvk.Group,
			Resource: resource,
		},
	}
}

func FromDaemonSetHandlerToHandler(sync DaemonSetHandler) generic.Handler {
	return func(key string, obj runtime.Object) (ret runtime.Object, err error) {
		var v *v1.DaemonSet
		if obj == nil {
			v, err = sync(key, nil)
		} else {
			v, err = sync(key, obj.(*v1.DaemonSet))
		}
		if v == nil {
			return nil, err
		}
		return v, err
	}
}

func (c *daemonSetController) Updater() generic.Updater {
	return func(obj runtime.Object) (runtime.Object, error) {
		newObj, err := c.Update(obj.(*v1.DaemonSet))
		if newObj == nil {
			return nil, err
		}
		return newObj, err
	}
}

func UpdateDaemonSetDeepCopyOnChange(client DaemonSetClient, obj *v1.DaemonSet, handler func(obj *v1.DaemonSet) (*v1.DaemonSet, error)) (*v1.DaemonSet, error) {
	if obj == nil {
		return obj, nil
	}

	copyObj := obj.DeepCopy()
	newObj, err := handler(copyObj)
	if newObj != nil {
		copyObj = newObj
	}
	if obj.ResourceVersion == copyObj.ResourceVersion && !equality.Semantic.DeepEqual(obj, copyObj) {
		return client.Update(copyObj)
	}

	return copyObj, err
}

func (c *daemonSetController) AddGenericHandler(ctx context.Context, name string, handler generic.Handler) {
	c.controller.RegisterHandler(ctx, name, controller.SharedControllerHandlerFunc(handler))
}

func (c *daemonSetController) AddGenericRemoveHandler(ctx context.Context, name string, handler generic.Handler) {
	c.AddGenericHandler(ctx, name, generic.NewRemoveHandler(name, c.Updater(), handler))
}

func (c *daemonSetController) OnChange(ctx context.Context, name string, sync DaemonSetHandler) {
	c.AddGenericHandler(ctx, name, FromDaemonSetHandlerToHandler(sync))
}

func (c *daemonSetController) OnRemove(ctx context.Context, name string, sync DaemonSetHandler) {
	c.AddGenericHandler(ctx, name, generic.NewRemoveHandler(name, c.Updater(), FromDaemonSetHandlerToHandler(sync)))
}

func (c *daemonSetController) Enqueue(namespace, name string) {
	c.controller.Enqueue(namespace, name)
}

func (c *daemonSetController) EnqueueAfter(namespace, name string, duration time.Duration) {
	c.controller.EnqueueAfter(namespace, name, duration)
}

func (c *daemonSetController) Informer() cache.SharedIndexInformer {
	return c.controller.Informer()
}

func (c *daemonSetController) GroupVersionKind() schema.GroupVersionKind {
	return c.gvk
}

func (c *daemonSetController) Cache() DaemonSetCache {
	return &daemonSetCache{
		indexer:  c.Informer().GetIndexer(),
		resource: c.groupResource,
	}
}

func (c *daemonSetController) Create(obj *v1.DaemonSet) (*v1.DaemonSet, error) {
	result := &v1.DaemonSet{}
	return result, c.client.Create(context.TODO(), obj.Namespace, obj, result, metav1.CreateOptions{})
}

func (c *daemonSetController) Update(obj *v1.DaemonSet) (*v1.DaemonSet, error) {
	result := &v1.DaemonSet{}
	return result, c.client.Update(context.TODO(), obj.Namespace, obj, result, metav1.UpdateOptions{})
}

func (c *daemonSetController) UpdateStatus(obj *v1.DaemonSet) (*v1.DaemonSet, error) {
	result := &v1.DaemonSet{}
	return result, c.client.UpdateStatus(context.TODO(), obj.Namespace, obj, result, metav1.UpdateOptions{})
}

func (c *daemonSetController) Delete(namespace, name string, options *metav1.DeleteOptions) error {
	if options == nil {
		options = &metav1.DeleteOptions{}
	}
	return c.client.Delete(context.TODO(), namespace, name, *options)
}

func (c *daemonSetController) Get(namespace, name string, options metav1.GetOptions) (*v1.DaemonSet, error) {
	result := &v1.DaemonSet{}
	return result, c.client.Get(context.TODO(), namespace, name, result, options)
}

func (c *daemonSetController) List(namespace string, opts metav1.ListOptions) (*v1.DaemonSetList, error) {
	result := &v1.DaemonSetList{}
	return result, c.client.List(context.TODO(), namespace, result, opts)
}

func (c *daemonSetController) Watch(namespace string, opts metav1.ListOptions) (watch.Interface, error) {
	return c.client.Watch(context.TODO(), namespace, opts)
}

func (c *daemonSetController) Patch(namespace, name string, pt types.PatchType, data []byte, subresources ...string) (*v1.DaemonSet, error) {
	result := &v1.DaemonSet{}
	return result, c.client.Patch(context.TODO(), namespace, name, pt, data, result, metav1.PatchOptions{}, subresources...)
}

type daemonSetCache struct {
	indexer  cache.Indexer
	resource schema.GroupResource
}

func (c *daemonSetCache) Get(namespace, name string) (*v1.DaemonSet, error) {
	obj, exists, err := c.indexer.GetByKey(namespace + "/" + name)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, errors.NewNotFound(c.resource, name)
	}
	return obj.(*v1.DaemonSet), nil
}

func (c *daemonSetCache) List(namespace string, selector labels.Selector) (ret []*v1.DaemonSet, err error) {

	err = cache.ListAllByNamespace(c.indexer, namespace, selector, func(m interface{}) {
		ret = append(ret, m.(*v1.DaemonSet))
	})

	return ret, err
}

func (c *daemonSetCache) AddIndexer(indexName string, indexer DaemonSetIndexer) {
	utilruntime.Must(c.indexer.AddIndexers(map[string]cache.IndexFunc{
		indexName: func(obj interface{}) (strings []string, e error) {
			return indexer(obj.(*v1.DaemonSet))
		},
	}))
}

func (c *daemonSetCache) GetByIndex(indexName, key string) (result []*v1.DaemonSet, err error) {
	objs, err := c.indexer.ByIndex(indexName, key)
	if err != nil {
		return nil, err
	}
	result = make([]*v1.DaemonSet, 0, len(objs))
	for _, obj := range objs {
		result = append(result, obj.(*v1.DaemonSet))
	}
	return result, nil
}

// DaemonSetStatusHandler is executed for every added or modified DaemonSet. Should return the new status to be updated
type DaemonSetStatusHandler func(obj *v1.DaemonSet, status v1.DaemonSetStatus) (v1.DaemonSetStatus, error)

// DaemonSetGeneratingHandler is the top-level handler that is executed for every DaemonSet event. It extends DaemonSetStatusHandler by a returning a slice of child objects to be passed to apply.Apply
type DaemonSetGeneratingHandler func(obj *v1.DaemonSet, status v1.DaemonSetStatus) ([]runtime.Object, v1.DaemonSetStatus, error)

// RegisterDaemonSetStatusHandler configures a DaemonSetController to execute a DaemonSetStatusHandler for every events observed.
// If a non-empty condition is provided, it will be updated in the status conditions for every handler execution
func RegisterDaemonSetStatusHandler(ctx context.Context, controller DaemonSetController, condition condition.Cond, name string, handler DaemonSetStatusHandler) {
	statusHandler := &daemonSetStatusHandler{
		client:    controller,
		condition: condition,
		handler:   handler,
	}
	controller.AddGenericHandler(ctx, name, FromDaemonSetHandlerToHandler(statusHandler.sync))
}

// RegisterDaemonSetGeneratingHandler configures a DaemonSetController to execute a DaemonSetGeneratingHandler for every events observed, passing the returned objects to the provided apply.Apply.
// If a non-empty condition is provided, it will be updated in the status conditions for every handler execution
func RegisterDaemonSetGeneratingHandler(ctx context.Context, controller DaemonSetController, apply apply.Apply,
	condition condition.Cond, name string, handler DaemonSetGeneratingHandler, opts *generic.GeneratingHandlerOptions) {
	statusHandler := &daemonSetGeneratingHandler{
		DaemonSetGeneratingHandler: handler,
		apply:                      apply,
		name:                       name,
		gvk:                        controller.GroupVersionKind(),
	}
	if opts != nil {
		statusHandler.opts = *opts
	}
	controller.OnChange(ctx, name, statusHandler.Remove)
	RegisterDaemonSetStatusHandler(ctx, controller, condition, name, statusHandler.Handle)
}

type daemonSetStatusHandler struct {
	client    DaemonSetClient
	condition condition.Cond
	handler   DaemonSetStatusHandler
}

// sync is executed on every resource addition or modification. Executes the configured handlers and sends the updated status to the Kubernetes API
func (a *daemonSetStatusHandler) sync(key string, obj *v1.DaemonSet) (*v1.DaemonSet, error) {
	if obj == nil {
		return obj, nil
	}

	origStatus := obj.Status.DeepCopy()
	obj = obj.DeepCopy()
	newStatus, err := a.handler(obj, obj.Status)
	if err != nil {
		// Revert to old status on error
		newStatus = *origStatus.DeepCopy()
	}

	if a.condition != "" {
		if errors.IsConflict(err) {
			a.condition.SetError(&newStatus, "", nil)
		} else {
			a.condition.SetError(&newStatus, "", err)
		}
	}
	if !equality.Semantic.DeepEqual(origStatus, &newStatus) {
		if a.condition != "" {
			// Since status has changed, update the lastUpdatedTime
			a.condition.LastUpdated(&newStatus, time.Now().UTC().Format(time.RFC3339))
		}

		var newErr error
		obj.Status = newStatus
		newObj, newErr := a.client.UpdateStatus(obj)
		if err == nil {
			err = newErr
		}
		if newErr == nil {
			obj = newObj
		}
	}
	return obj, err
}

type daemonSetGeneratingHandler struct {
	DaemonSetGeneratingHandler
	apply apply.Apply
	opts  generic.GeneratingHandlerOptions
	gvk   schema.GroupVersionKind
	name  string
	seen  sync.Map
}

// Remove handles the observed deletion of a resource, cascade deleting every associated resource previously applied
func (a *daemonSetGeneratingHandler) Remove(key string, obj *v1.DaemonSet) (*v1.DaemonSet, error) {
	if obj != nil {
		return obj, nil
	}

	obj = &v1.DaemonSet{}
	obj.Namespace, obj.Name = kv.RSplit(key, "/")
	obj.SetGroupVersionKind(a.gvk)

	if a.opts.UniqueApplyForResourceVersion {
		a.seen.Delete(key)
	}

	return nil, generic.ConfigureApplyForObject(a.apply, obj, &a.opts).
		WithOwner(obj).
		WithSetID(a.name).
		ApplyObjects()
}

// Handle executes the configured DaemonSetGeneratingHandler and pass the resulting objects to apply.Apply, finally returning the new status of the resource
func (a *daemonSetGeneratingHandler) Handle(obj *v1.DaemonSet, status v1.DaemonSetStatus) (v1.DaemonSetStatus, error) {
	if !obj.DeletionTimestamp.IsZero() {
		return status, nil
	}

	objs, newStatus, err := a.DaemonSetGeneratingHandler(obj, status)
	if err != nil {
		return newStatus, err
	}
	if !a.isNewResourceVersion(obj) {
		return newStatus, nil
	}

	err = generic.ConfigureApplyForObject(a.apply, obj, &a.opts).
		WithOwner(obj).
		WithSetID(a.name).
		ApplyObjects(objs...)
	if err != nil {
		return newStatus, err
	}
	a.storeResourceVersion(obj)
	return newStatus, nil
}

// isNewResourceVersion detects if a specific resource version was already successfully processed.
// Only used if UniqueApplyForResourceVersion is set in generic.GeneratingHandlerOptions
func (a *daemonSetGeneratingHandler) isNewResourceVersion(obj *v1.DaemonSet) bool {
	if !a.opts.UniqueApplyForResourceVersion {
		return true
	}

	// Apply once per resource version
	key := obj.Namespace + "/" + obj.Name
	previous, ok := a.seen.Load(key)
	return !ok || previous != obj.ResourceVersion
}

// storeResourceVersion keeps track of the latest resource version of an object for which Apply was executed
// Only used if UniqueApplyForResourceVersion is set in generic.GeneratingHandlerOptions
func (a *daemonSetGeneratingHandler) storeResourceVersion(obj *v1.DaemonSet) {
	if !a.opts.UniqueApplyForResourceVersion {
		return
	}

	key := obj.Namespace + "/" + obj.Name
	a.seen.Store(key, obj.ResourceVersion)
}
