package types

import (
	"testing"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestParseDataEngineMode(t *testing.T) {
	for _, test := range []struct {
		input string
		want  DataEngineMode
		valid bool
	}{
		{"", DataEngineModeV1Capable, true},
		{"v1-capable", DataEngineModeV1Capable, true},
		{"v2-only", DataEngineModeV2Only, true},
		{"v2", "", false},
		{"V2-ONLY", "", false},
		{" v2-only", "", false},
		{"v2-only ", "", false},
		{"disabled", "", false},
	} {
		t.Run(test.input, func(t *testing.T) {
			got, err := ParseDataEngineMode(test.input)
			if (err == nil) != test.valid || got != test.want {
				t.Fatalf("mode %q: got %q, error %v", test.input, got, err)
			}
		})
	}
}

func TestDataEngineModeCapabilityCeiling(t *testing.T) {
	for _, test := range []struct {
		mode    DataEngineMode
		iscsi   bool
		allowed []longhorn.DataEngineType
	}{
		{"", true, []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2, longhorn.DataEngineTypeAll}},
		{DataEngineModeV1Capable, true, []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2, longhorn.DataEngineTypeAll}},
		{DataEngineModeV2Only, false, []longhorn.DataEngineType{longhorn.DataEngineTypeV2}},
		{"invalid-mode", true, nil},
	} {
		t.Run(string(test.mode), func(t *testing.T) {
			if test.mode.RequiresISCSI() != test.iscsi {
				t.Fatal("iSCSI prerequisite did not follow the installation capability")
			}
			for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2, longhorn.DataEngineTypeAll, "unrecognized"} {
				want := false
				for _, allowed := range test.allowed {
					want = want || engine == allowed
				}
				if test.mode.AllowsDataEngine(engine) != want {
					t.Fatalf("mode %q, engine %q: wrong capability ceiling", test.mode, engine)
				}
			}
		})
	}
}
