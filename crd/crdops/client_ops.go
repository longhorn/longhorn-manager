package crdops

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type CrdOp struct {
	Cl    *rest.RESTClient
	Ns    string
	Codec runtime.ParameterCodec
}

func (f *CrdOp) Create(obj interface{}, plural string, result runtime.Object) error {
	err := f.Cl.Post().
		Namespace(f.Ns).Resource(plural).
		Body(obj).Do().Into(result)
	return err
}

func (f *CrdOp) Update(obj interface{}, plural string, result runtime.Object, name string) error {
	err := f.Cl.Put().Name(name).
		Namespace(f.Ns).Resource(plural).
		Body(obj).Do().Into(result)
	return err
}

func (f *CrdOp) Delete(name string, plural string, options *metav1.DeleteOptions) error {
	return f.Cl.Delete().
		Namespace(f.Ns).Resource(plural).
		Name(name).Body(options).Do().
		Error()
}

func (f *CrdOp) Get(name string, plural string, result runtime.Object) error {
	err := f.Cl.Get().
		Namespace(f.Ns).Resource(plural).
		Name(name).Do().Into(result)
	return err
}

func (f *CrdOp) List(opts metav1.ListOptions, plural string, result runtime.Object) error {
	err := f.Cl.Get().
		Namespace(f.Ns).Resource(plural).
		VersionedParams(&opts, f.Codec).
		Do().Into(result)
	return err
}

func (f *CrdOp) NewListWatch(plural string) *cache.ListWatch {
	return cache.NewListWatchFromClient(f.Cl, plural, f.Ns, fields.Everything())
}
