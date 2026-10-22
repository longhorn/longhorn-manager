package types

import (
	"testing"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// A stored value persisted before a data engine key was added to the setting
// definition (e.g. across an upgrade) must fall back to the definition
// default for the missing key instead of failing lookups.
func TestParseDataEngineSpecificSettingFillsMissingKeysFromDefault(t *testing.T) {
	definition, ok := GetSettingDefinition(SettingNameGuaranteedInstanceManagerCPU)
	if !ok {
		t.Fatal("setting definition not found")
	}

	values, err := ParseDataEngineSpecificSetting(definition, `{"v1":"20","v2":"20"}`)
	if err != nil {
		t.Fatalf("ParseDataEngineSpecificSetting failed: %v", err)
	}

	if values[longhorn.DataEngineTypeV1] != float64(20) || values[longhorn.DataEngineTypeV2] != float64(20) {
		t.Fatalf("stored values must win over defaults: %v", values)
	}
	if values[longhorn.DataEngineTypeLocal] != float64(1) {
		t.Fatalf("missing local key must fall back to the definition default, got %v", values)
	}
}
