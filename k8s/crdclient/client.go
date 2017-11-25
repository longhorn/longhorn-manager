package crdclient

import (
	"fmt"
	"reflect"
	"time"

	apiextv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/rest"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
)

func WaitForCRDCreation(clientset apiextcs.Interface, FullCRDName string) error {
	// wait for CRD being established
	err := wait.Poll(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		crd, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Get(FullCRDName, metav1.GetOptions{})
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
		deleteErr := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Delete(FullCRDName, nil)
		if deleteErr != nil {
			return errors.NewAggregate([]error{err, deleteErr})
		}
		return err
	}
	return err
}

func CreateCRD(clientset apiextcs.Interface, cfg *rest.Config) error {
	for _, c := range CRDInfoMap {
		kind := reflect.TypeOf(c.Obj).Name()
		crd := &apiextv1beta1.CustomResourceDefinition{
			ObjectMeta: metav1.ObjectMeta{
				Name: c.FullName,
				Labels: map[string]string{
					"longhorn-manager": kind,
				},
			},
			Spec: apiextv1beta1.CustomResourceDefinitionSpec{
				Group:   longhorn.SchemeGroupVersion.Group,
				Version: longhorn.SchemeGroupVersion.Version,
				Scope:   apiextv1beta1.NamespaceScoped,
				Names: apiextv1beta1.CustomResourceDefinitionNames{
					Plural:     c.Plural,
					Kind:       kind,
					ShortNames: []string{c.ShortName},
				},
			},
		}
		_, err := clientset.ApiextensionsV1beta1().CustomResourceDefinitions().Create(crd)
		if err != nil && !apierrors.IsAlreadyExists(err) {
			return err
		}

		if err := WaitForCRDCreation(clientset, c.FullName); err != nil {
			return err
		}
	}
	return nil
}
