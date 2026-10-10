package instancemanager

import (
	"testing"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestValidateDeprecatedV2DataEngineSpec(t *testing.T) {
	set := longhorn.V2DataEngineSpec{CPUMask: "0x3", CPUIsolationEnabled: longhorn.TrueValue}

	tests := map[string]struct {
		oldSpec longhorn.V2DataEngineSpec
		newSpec longhorn.V2DataEngineSpec
		wantErr bool
	}{
		"allows empty fields":            {},
		"allows unchanged values":        {oldSpec: set, newSpec: set},
		"allows clearing values":         {oldSpec: set},
		"rejects setting the CPU mask":   {newSpec: longhorn.V2DataEngineSpec{CPUMask: "0x3"}, wantErr: true},
		"rejects changing the CPU mask":  {oldSpec: set, newSpec: longhorn.V2DataEngineSpec{CPUMask: "0x1", CPUIsolationEnabled: longhorn.TrueValue}, wantErr: true},
		"rejects setting CPU isolation":  {newSpec: longhorn.V2DataEngineSpec{CPUIsolationEnabled: longhorn.FalseValue}, wantErr: true},
		"rejects changing CPU isolation": {oldSpec: set, newSpec: longhorn.V2DataEngineSpec{CPUMask: "0x3", CPUIsolationEnabled: longhorn.FalseValue}, wantErr: true},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := validateDeprecatedV2DataEngineSpec(test.oldSpec, test.newSpec)
			if test.wantErr && err == nil {
				t.Fatal("expected validation error")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}
