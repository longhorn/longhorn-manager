package api

import (
	"testing"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestVolumeSchemaDefaultsToTheInstallationEngine(t *testing.T) {
	for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable, types.DataEngineModeV2Only} {
		schemas, err := NewSchemaForDataEngineMode(mode)
		if err != nil {
			t.Fatal(err)
		}
		volume, ok := schemas.CheckSchema("volume")
		if !ok {
			t.Fatal("missing volume schema")
		}
		field, ok := volume.CheckField("dataEngine")
		want := longhorn.DataEngineTypeV1
		if mode == types.DataEngineModeV2Only {
			want = longhorn.DataEngineTypeV2
		}
		if !ok || field.Default != want || !field.Create || !field.Required {
			t.Fatalf("mode=%s has the wrong new-volume engine default: %+v", mode, field)
		}
	}
	if _, err := NewSchemaForDataEngineMode("unknown"); err == nil {
		t.Fatal("unknown capability published an API schema")
	}
	legacy, ok := NewSchema().CheckSchema("volume")
	if !ok || legacy.ResourceFields["dataEngine"].Default != longhorn.DataEngineTypeV1 {
		t.Fatal("V2 schema construction mutated the legacy schema")
	}
}
