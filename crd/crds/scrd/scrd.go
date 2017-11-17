
package scrd

import (
	"reflect"
	"github.com/rancher/longhorn-manager/crd/crds/commoncrd"
	apiextv1beta1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1beta1"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"github.com/rancher/longhorn-manager/crd/types/stype"
)

const (
	CRDPlural      string = "crdsettings"
	CRDGroup       string = "rancher.io"
	CRDVersion     string = "v1"
	FullCRDName    string = CRDPlural + "." + CRDGroup
	Shortname	   string = "crs"
)



// Create the CRD resource, ignore error if it already exists
func CreateSettingCRD(clientset apiextcs.Interface) error {
	crd := &apiextv1beta1.CustomResourceDefinition{
		ObjectMeta: meta_v1.ObjectMeta{Name: FullCRDName},
		Spec: apiextv1beta1.CustomResourceDefinitionSpec{
			Group:   CRDGroup,
			Version: CRDVersion,
			Scope:   apiextv1beta1.NamespaceScoped,
			Names:   apiextv1beta1.CustomResourceDefinitionNames{
				Plural: CRDPlural,
				Kind:   reflect.TypeOf(stype.Crdsetting{}).Name(),
				ShortNames: []string{Shortname},
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


// Create a  Rest client with the new CRD Schema
var schemeGroupVersion = schema.GroupVersion{Group: CRDGroup, Version: CRDVersion}

func addKnownTypes(scheme *runtime.Scheme) error {
	scheme.AddKnownTypes(schemeGroupVersion,
		&stype.Crdsetting{},
		&stype.CrdsettingList{},
	)
	meta_v1.AddToGroupVersion(scheme, schemeGroupVersion)
	return nil
}

func SettingWaitCRDCreateDone(clientset apiextcs.Interface) error {
	return commoncrd.WaitCRDCreateDone(clientset, FullCRDName)
}

func SettingNewClient(cfg *rest.Config) (*rest.RESTClient, *runtime.Scheme, error) {
	var schemeBuilder = runtime.NewSchemeBuilder(addKnownTypes)
	return commoncrd.NewClient(cfg, &schemeGroupVersion, &schemeBuilder);
}