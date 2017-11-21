package crdclient

import (
	"fmt"
	"reflect"
	"time"

	"github.com/rancher/longhorn-manager/crd/crdops"
	"github.com/rancher/longhorn-manager/crd/crdtype"

	apiv1 "k8s.io/api/core/v1"
	apiextv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"

)

func WaitCRDCreateDone(clientset apiextcs.Interface, FullCrdName string) error {
	// wait for CRD being established

	err := wait.Poll(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		crd, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Get(FullCrdName, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, cond := range crd.Status.Conditions {
			switch cond.Type {
			case apiextv1beta1.Established:
				if cond.Status == apiextv1beta1.ConditionTrue {
					return true, err
				}
			case apiextv1beta1.NamesAccepted:
				if cond.Status == apiextv1beta1.ConditionFalse {
					fmt.Printf("Name conflict: %v\n", cond.Reason)
				}
			}
		}
		return false, err
	})

	if err != nil {
		deleteErr := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(FullCrdName, nil)
		if deleteErr != nil {
			return errors.NewAggregate([]error{err, deleteErr})
		}
		return err
	}
	return err
}

func NewClient(cfg *rest.Config) (*rest.RESTClient, *runtime.Scheme, error) {

	scheme := runtime.NewScheme()
	if err := crdtype.SchemeBuilder.AddToScheme(scheme); err != nil {
		return nil, nil, err
	}

	config := *cfg
	config.GroupVersion = &schema.GroupVersion{
		Group:   crdtype.CRDGroup,
		Version: crdtype.CRDVersion,
	}
	config.APIPath = "/apis"
	config.ContentType = runtime.ContentTypeJSON
	config.NegotiatedSerializer = serializer.DirectCodecFactory{
		CodecFactory: serializer.NewCodecFactory(scheme)}

	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, nil, err
	}

	return client, scheme, nil
}

// Create the CRD resource, ignore error if it already exists
func CreateCRD(clientset apiextcs.Interface) error {

	for _, crdType := range crdtype.CrdMap {
		crd := &apiextv1beta1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{Name: crdType.CrdFullName},
			Spec: apiextv1beta1.CustomResourceDefinitionSpec{
				Group:   crdtype.CRDGroup,
				Version: crdtype.CRDVersion,
				Scope:   apiextv1beta1.NamespaceScoped,
				Names: apiextv1beta1.CustomResourceDefinitionNames{
					Plural:     crdType.CrdPlural,
					Kind:       reflect.TypeOf(crdType.CrdObj).Name(),
					ShortNames: []string{crdType.CrdShortName},
				},
			},
		}
		_, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}

		if err := WaitCRDCreateDone(clientset, crdType.CrdFullName); err != nil {
			return err
		}
	}
	return nil
}

func CreateClient(clientset apiextcs.Interface, cfg *rest.Config) (*crdops.CrdOp, error) {

	if err := CreateCRD(clientset); err != nil {
		return nil, err
	}

	// Create a new clientset which include our CRD schema
	restClient, scheme, err := NewClient(cfg)
	if err != nil {
		return nil, err
	}

	return &crdops.CrdOp{
		restClient,
		apiv1.NamespaceDefault,
		runtime.NewParameterCodec(scheme)}, nil
}
