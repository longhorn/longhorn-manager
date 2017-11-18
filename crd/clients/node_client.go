package crdclient

import (
	"reflect"
	"github.com/rancher/longhorn-manager/crd/crdtype"
	apiextv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	//"k8s.io/client-go/rest"
	"github.com/rancher/longhorn-manager/crd/crdops"
	apiv1 "k8s.io/api/core/v1"
	"k8s.io/client-go/rest"
)



// Create the CRD resource, ignore error if it already exists
func CreateNodeCRD(clientset apiextcs.Interface) error {
	crd := &apiextv1beta1.CustomResourceDefinition{
		ObjectMeta: meta_v1.ObjectMeta{Name: crdtype.NodeFullName},
		Spec: apiextv1beta1.CustomResourceDefinitionSpec{
			Group:   crdtype.CRDGroup,
			Version: crdtype.CRDVersion,
			Scope:   apiextv1beta1.NamespaceScoped,
			Names:   apiextv1beta1.CustomResourceDefinitionNames{
				Plural: crdtype.NodePlural,
				Kind:   reflect.TypeOf(crdtype.Crdnode{}).Name(),
				ShortNames: []string{crdtype.NodeShortname},
			},
		},
	}

	_, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil && apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err

	// Note the original apiextensions example adds logic to wait for creation and exception handling
}


func NodeWaitCRDCreateDone(clientset apiextcs.Interface) error {
	return WaitCRDCreateDone(clientset, crdtype.NodeFullName)
}


func nodeAddKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(schema.GroupVersion{
		Group: crdtype.CRDGroup,
		Version: crdtype.CRDVersion,
	},
		&crdtype.Crdnode{},
		&crdtype.CrdnodeList{},
	)
	meta_v1.AddToGroupVersion(scheme, schema.GroupVersion{
		Group: crdtype.CRDGroup,
		Version: crdtype.CRDVersion,
	})
	return nil
}

func CreateNodeClient(clientset apiextcs.Interface, cfg *rest.Config) *crdops.CrdOp {
	// note: if the CRD exist our CreateCRD function is set to exit without an error
	err := CreateNodeCRD(clientset)
	if err != nil {
		panic(err)
	}

	// Wait for the CRD to be created before we use it (only needed if its a new one)
	err = NodeWaitCRDCreateDone(clientset)
	if err != nil {
		panic(err)
	}

	// Create a new clientset which include our CRD schema
	crdcs, scheme, err := NewClient(cfg, nodeAddKnownTypes)
	if err != nil {
		panic(err)
	}
	return &crdops.CrdOp{
		crdcs,
		apiv1.NamespaceDefault,
		crdtype.NodePlural,
		runtime.NewParameterCodec(scheme)}
}