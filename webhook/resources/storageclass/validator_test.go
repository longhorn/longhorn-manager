package storageclass

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	storagev1 "k8s.io/api/storage/v1"
)

func TestValidateDataLayout(t *testing.T) {
	tests := []struct {
		name            string
		params          map[string]string
		expectedFields  []string
		incorrectFields []string
		reasonKeyword   string
	}{
		{
			name: "all valid parameters passed",
			params: map[string]string{
				"dataLayout.type":         "sharded",
				"dataLayout.mode":         "erasureCoding",
				"dataLayout.dataChunks":   "4",
				"dataLayout.parityChunks": "2",
				"dataLayout.stripSizeKB":  "64",
			},
			expectedFields: []string{
				"dataLayout.type",
				"dataLayout.mode",
				"dataLayout.dataChunks",
				"dataLayout.parityChunks",
				"dataLayout.stripSizeKB",
			},
			incorrectFields: []string{},
		},
		{
			name: "valid dataLayout parameters passed with other SC parameters",
			params: map[string]string{
				"dataEngine":            "v2",
				"dataLayout.type":       "sharded",
				"dataLayout.mode":       "erasureCoding",
				"dataLayout.dataChunks": "4",
			},
			expectedFields: []string{
				"dataEngine",
				"dataLayout.type",
				"dataLayout.mode",
				"dataLayout.dataChunks",
			},
			incorrectFields: []string{},
		},
		{
			name: "only dataLayout with no subfield passed",
			params: map[string]string{
				"dataEngine": "v2",
				"dataLayout": "sharded",
			},
			expectedFields:  []string{"dataEngine"},
			incorrectFields: []string{"dataLayout"},
			reasonKeyword:   "must specify a subfield",
		},
		{
			name: "dataLayout parameters passed without prefix",
			params: map[string]string{
				"dataEngine":   "v2",
				"type":         "sharded",
				"mode":         "erasureCoding",
				"dataChunks":   "4",
				"parityChunks": "2",
				"stripSizeKB":  "64",
			},
			expectedFields: []string{
				"dataEngine",
			},
			incorrectFields: []string{
				"type", "mode", "dataChunks", "parityChunks", "stripSizeKB",
			},
			reasonKeyword: "must be prefixed",
		},
		{
			name: "unknown dataLayout parameters passed",
			params: map[string]string{
				"dataLayout.type":    "sharded",
				"dataLayout.unknown": "foo",
			},
			expectedFields:  []string{"dataLayout.type"},
			incorrectFields: []string{"dataLayout.unknown"},
			reasonKeyword:   "unknown field",
		},
		{
			name: "no dataLayout parameters passed, but other SC parameters",
			params: map[string]string{
				"dataEngine":       "v1",
				"backupTargetName": "default",
				"fsType":           "ext4",
			},
			expectedFields:  []string{"dataEngine", "backupTargetName", "fsType"},
			incorrectFields: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Every incorrectFields entry is expected to produce an error, and reasonKeyword
			// documents what that error should say. A missing reasonKeyword at present means the
			// test case was left incomplete rather than intentionally skipping the message check.
			if len(tc.incorrectFields) > 0 {
				require.NotEmpty(t, tc.reasonKeyword,
					"test case %q has incorrectFields but no reasonKeyword set; was this intentional?", tc.name)
			}

			errs := validateDataLayout(tc.params)

			require.Equal(t, len(tc.expectedFields)+len(tc.incorrectFields), len(tc.params),
				"every param key should be classified as either expected or incorrect")
			require.Len(t, errs, len(tc.incorrectFields),
				"unexpected number of errors.\nexpected fields: %v\ngot errors: %+v",
				tc.incorrectFields, errs)

			gotErrorDetails := make(map[string]string)
			for _, err := range errs {
				fieldName, ok := err.BadValue.(string)
				require.True(t, ok, "BadValue should be a string, got %T (%v)", err.BadValue, err.BadValue)
				gotErrorDetails[fieldName] = err.Detail
			}

			for _, f := range tc.incorrectFields {
				detail, found := gotErrorDetails[f]
				assert.True(t, found, "expected an error for field %q", f)
				if tc.reasonKeyword != "" {
					assert.Contains(t, detail, tc.reasonKeyword, "error for field %q should indicate %q, got: %q", f, tc.reasonKeyword, detail)
				}
			}

			for _, f := range tc.expectedFields {
				_, found := gotErrorDetails[f]
				assert.False(t, found, "did not expect an error for field %q", f)
			}
		})
	}
}

func TestValidateVolumeAntiAffinity(t *testing.T) {
	waitForFirstConsumer := storagev1.VolumeBindingWaitForFirstConsumer
	immediate := storagev1.VolumeBindingImmediate

	tests := []struct {
		name          string
		params        map[string]string
		bindingMode   *storagev1.VolumeBindingMode
		invalidFields []string
	}{
		{
			name:        "parameter omitted",
			params:      map[string]string{"numberOfReplicas": "3"},
			bindingMode: &immediate,
		},
		{
			name:        "disabled",
			params:      map[string]string{"volumeAntiAffinity": "disabled", "numberOfReplicas": "3"},
			bindingMode: &immediate,
		},
		{
			name:        "enabled",
			params:      map[string]string{"volumeAntiAffinity": "enabled", "numberOfReplicas": "1"},
			bindingMode: &waitForFirstConsumer,
		},
		{
			name:          "unknown value",
			params:        map[string]string{"volumeAntiAffinity": "required", "numberOfReplicas": "1"},
			bindingMode:   &waitForFirstConsumer,
			invalidFields: []string{"parameters[volumeAntiAffinity]"},
		},
		{
			name:          "enabled with immediate binding",
			params:        map[string]string{"volumeAntiAffinity": "enabled", "numberOfReplicas": "1"},
			bindingMode:   &immediate,
			invalidFields: []string{"volumeBindingMode"},
		},
		{
			name:          "enabled without replica count",
			params:        map[string]string{"volumeAntiAffinity": "enabled"},
			bindingMode:   &waitForFirstConsumer,
			invalidFields: []string{"parameters[numberOfReplicas]"},
		},
		{
			name:          "enabled with three replicas",
			params:        map[string]string{"volumeAntiAffinity": "enabled", "numberOfReplicas": "3"},
			bindingMode:   nil,
			invalidFields: []string{"volumeBindingMode", "parameters[numberOfReplicas]"},
		},
		{
			name:          "enabled with EC data layout",
			params:        map[string]string{"volumeAntiAffinity": "enabled", "numberOfReplicas": "1", "dataLayout.type": "sharded"},
			bindingMode:   &waitForFirstConsumer,
			invalidFields: []string{"parameters[volumeAntiAffinity]"},
		},
		{
			name:        "disabled with EC data layout",
			params:      map[string]string{"volumeAntiAffinity": "disabled", "numberOfReplicas": "1", "dataLayout.type": "sharded"},
			bindingMode: &waitForFirstConsumer,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sc := &storagev1.StorageClass{Parameters: tc.params, VolumeBindingMode: tc.bindingMode}
			errs := validateVolumeAntiAffinity(sc)

			gotFields := []string{}
			for _, err := range errs {
				gotFields = append(gotFields, err.Field)
			}
			assert.ElementsMatch(t, tc.invalidFields, gotFields, "errors: %v", errs)
		})
	}
}
