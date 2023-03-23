package types

import (
	"fmt"
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
)

func TestParseToleration(t *testing.T) {
	type testCase struct {
		input string

		expectedToleration []corev1.Toleration
		expectError        bool
	}
	testCases := map[string]testCase{
		"valid empty setting": {
			input:              "",
			expectedToleration: []corev1.Toleration{},
			expectError:        false,
		},
		"valid key:NoSchedule": {
			input: "key:NoSchedule",
			expectedToleration: []corev1.Toleration{
				{
					Key:      "key",
					Value:    "",
					Operator: corev1.TolerationOpExists,
					Effect:   corev1.TaintEffectNoSchedule,
				},
			},
			expectError: false,
		},
		"valid key=value:NoExecute": {
			input: "key=value:NoExecute",
			expectedToleration: []corev1.Toleration{
				{
					Key:      "key",
					Value:    "value",
					Operator: corev1.TolerationOpEqual,
					Effect:   corev1.TaintEffectNoExecute,
				},
			},
			expectError: false,
		},
		"valid key=value:PreferNoSchedule": {
			input: "key=value:PreferNoSchedule",
			expectedToleration: []corev1.Toleration{
				{
					Key:      "key",
					Value:    "value",
					Operator: corev1.TolerationOpEqual,
					Effect:   corev1.TaintEffectPreferNoSchedule,
				},
			},
			expectError: false,
		},
		"valid key0:NoSchedule;key1=value:NoExecute": {
			input: "key0:NoSchedule;key1=value:NoExecute",
			expectedToleration: []corev1.Toleration{
				{
					Key:      "key0",
					Value:    "",
					Operator: corev1.TolerationOpExists,
					Effect:   corev1.TaintEffectNoSchedule,
				},
				{
					Key:      "key1",
					Value:    "value",
					Operator: corev1.TolerationOpEqual,
					Effect:   corev1.TaintEffectNoExecute,
				},
			},
			expectError: false,
		},
		"invalid key:InvalidEffect": {
			input:              "key:InvalidEffect",
			expectedToleration: nil,
			expectError:        true,
		},
		"invalid key=value=NoSchedule": {
			input:              "key=value=NoSchedule",
			expectedToleration: nil,
			expectError:        true,
		},
	}

	for name, test := range testCases {
		fmt.Printf("testing %v\n", name)

		toleration, err := UnmarshalTolerations(test.input)
		if !reflect.DeepEqual(toleration, test.expectedToleration) {
			t.Errorf("unexpected toleration:\nGot: %v\nWant: %v", toleration, test.expectedToleration)
		}

		if test.expectError && err == nil {
			t.Errorf("unexpected error: %v", err)
		}
	}
}
