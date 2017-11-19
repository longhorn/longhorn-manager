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
	"k8s.io/client-go/rest"
	apiv1 "k8s.io/api/core/v1"
	"github.com/rancher/longhorn-manager/crd/crdops"
)

func ControllerWaitCRDCreateDone(clientset apiextcs.Interface) error {
	return WaitCRDCreateDone(clientset, crdtype.ControllerFullName)
}

// Create the CRD resource, ignore error if it already exists
func CreateControllerCRD(clientset apiextcs.Interface) error {
	crd := &apiextv1beta1.CustomResourceDefinition{
		ObjectMeta: meta_v1.ObjectMeta{Name: crdtype.ControllerFullName},
		Spec: apiextv1beta1.CustomResourceDefinitionSpec{
			Group:   crdtype.CRDGroup,
			Version: crdtype.CRDVersion,
			Scope:   apiextv1beta1.NamespaceScoped,
			Names:   apiextv1beta1.CustomResourceDefinitionNames{
				Plural: crdtype.ControllerPlural,
				Kind:   reflect.TypeOf(crdtype.Crdcontroller{}).Name(),
				ShortNames: []string{crdtype.ControllerShortname},
			},
		},
	}

	_, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
	if err != nil && apierrors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func controllerAddKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(schema.GroupVersion{
		Group: crdtype.CRDGroup,
		Version: crdtype.CRDVersion,
	},
		&crdtype.Crdcontroller{},
		&crdtype.CrdcontrollerList{},
	)
	meta_v1.AddToGroupVersion(scheme, schema.GroupVersion{
		Group: crdtype.CRDGroup,
		Version: crdtype.CRDVersion,
	})
	return nil
}

func CreateControllerClient(clientset apiextcs.Interface, cfg *rest.Config) *crdops.CrdOp {
	// note: if the CRD exist our CreateCRD function is set to exit without an error
	err := CreateControllerCRD(clientset)
	if err != nil {
		panic(err)
	}

	// Wait for the CRD to be created before we use it (only needed if its a new one)
	err = ControllerWaitCRDCreateDone(clientset)
	if err != nil {
		panic(err)
	}

	// Create a new clientset which include our CRD schema
	crdcs, scheme, err := NewClient(cfg, controllerAddKnownTypes)
	if err != nil {
		panic(err)
	}

	return &crdops.CrdOp{
		crdcs,
		apiv1.NamespaceDefault,
		crdtype.ControllerPlural,
		runtime.NewParameterCodec(scheme)}
}

