package crdops

import (
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/apimachinery/pkg/runtime"
)

type CrdOp struct {
	Cl     *rest.RESTClient
	Ns     string
	Plural string
	Codec  runtime.ParameterCodec
}

func (f *CrdOp) Create(obj interface{}, result runtime.Object) error {
	err := f.Cl.Post().
		Namespace(f.Ns).Resource(f.Plural).
		Body(obj).Do().Into(result)
	return  err
}

func (f *CrdOp) Update(obj interface{}, result runtime.Object, name string) error {
	err := f.Cl.Put().Name(name).
		Namespace(f.Ns).Resource(f.Plural).
		Body(obj).Do().Into(result)
	return err
}

func (f *CrdOp) Delete(name string, options *meta_v1.DeleteOptions) error {
	return f.Cl.Delete().
		Namespace(f.Ns).Resource(f.Plural).
		Name(name).Body(options).Do().
		Error()
}

func (f *CrdOp) Get(name string, result runtime.Object) error {
	err := f.Cl.Get().
		Namespace(f.Ns).Resource(f.Plural).
		Name(name).Do().Into(result)
	return err
}

func (f *CrdOp) List(opts meta_v1.ListOptions, result runtime.Object) error {
	err := f.Cl.Get().
		Namespace(f.Ns).Resource(f.Plural).
		VersionedParams(&opts, f.Codec).
		Do().Into(result)
	return err
}

func (f *CrdOp) NewListWatch() *cache.ListWatch {
	return cache.NewListWatchFromClient(f.Cl, f.Plural, f.Ns, fields.Everything())
}
