package datastore

import (
	"context"
	"fmt"
	"reflect"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
)

func (s *DataStore) ValidateDataEngineModeRestoreCRDs(ctx context.Context, archive *apiextensionsv1.CustomResourceDefinitionList) error {
	mode, err := types.ParseDataEngineMode(string(s.dataEngineMode))
	if err != nil || mode != types.DataEngineModeV2Only {
		return err
	}
	if ctx == nil || s.extensionsClient == nil || archive == nil || len(archive.Items) == 0 || archive.Continue != "" || (archive.RemainingItemCount != nil && *archive.RemainingItemCount != 0) {
		return fmt.Errorf("v2-only restore requires a complete archived CRD inventory and live client")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	live, err := s.extensionsClient.ApiextensionsV1().CustomResourceDefinitions().List(ctx, metav1.ListOptions{Limit: 1000})
	if err != nil || live == nil || live.Continue != "" || (live.RemainingItemCount != nil && *live.RemainingItemCount != 0) || len(live.Items) > 1000 {
		return fmt.Errorf("cannot obtain a complete current CRD inventory for v2-only restore")
	}
	current := map[string]*apiextensionsv1.CustomResourceDefinition{}
	for index := range live.Items {
		crd := &live.Items[index]
		if crd.Spec.Group != "longhorn.io" {
			continue
		}
		if _, duplicate := current[crd.Name]; duplicate {
			return fmt.Errorf("duplicate current Longhorn CRD")
		}
		current[crd.Name] = crd
	}
	if len(current) != len(archive.Items) {
		return fmt.Errorf("archived and current Longhorn CRD inventories differ")
	}
	for _, archived := range archive.Items {
		live := current[archived.Name]
		if err := validateV2OnlyRestoreCRD(live, &archived); err != nil {
			return err
		}
		delete(current, archived.Name)
	}
	return ctx.Err()
}

// GetDataEngineModeRestoreCRD rebinds schema compatibility at the native
// restore write boundary. Its ResourceVersion is used by the subsequent
// update, so a concurrent schema change produces a conflict rather than a
// downgrade. Missing CRDs are never recreated from an archive in v2-only mode.
func (s *DataStore) GetDataEngineModeRestoreCRD(ctx context.Context, archive *apiextensionsv1.CustomResourceDefinition) (*apiextensionsv1.CustomResourceDefinition, error) {
	if ctx == nil || s.extensionsClient == nil || archive == nil {
		return nil, fmt.Errorf("missing v2-only restore CRD binding")
	}
	live, err := s.extensionsClient.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, archive.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("cannot rebind current CRD for v2-only restore")
	}
	if err := validateV2OnlyRestoreCRD(live, archive); err != nil {
		return nil, err
	}
	return live, ctx.Err()
}

func validateV2OnlyRestoreCRD(live, archive *apiextensionsv1.CustomResourceDefinition) error {
	if live == nil || archive == nil || archive.Name == "" || live.Name != archive.Name || live.Namespace != "" || archive.Namespace != "" || archive.Spec.Group != "longhorn.io" || live.UID == "" || live.ResourceVersion == "" || live.DeletionTimestamp != nil || archive.DeletionTimestamp != nil || !reflect.DeepEqual(live.Spec, archive.Spec) {
		return fmt.Errorf("archived CRD is missing or incompatible with the current V2 schema")
	}
	return nil
}
