package types

import (
	"reflect"
	"testing"
)

func TestPreferredDataEngineIPFamilySetting(t *testing.T) {
	const expectedName = SettingName("preferred-data-engine-ip-family")

	if SettingNamePreferredDataEngineIPFamily != expectedName {
		t.Fatalf("unexpected setting name: got %q, want %q", SettingNamePreferredDataEngineIPFamily, expectedName)
	}

	definition, ok := GetSettingDefinition(SettingNamePreferredDataEngineIPFamily)
	if !ok {
		t.Fatalf("setting definition %q does not exist", SettingNamePreferredDataEngineIPFamily)
	}

	if definition.Category != SettingCategoryDangerZone {
		t.Errorf("unexpected setting category: got %q, want %q", definition.Category, SettingCategoryDangerZone)
	}
	if definition.Type != SettingTypeString {
		t.Errorf("unexpected setting type: got %q, want %q", definition.Type, SettingTypeString)
	}
	if definition.Default != DataEngineIPFamilyDefault {
		t.Errorf("unexpected setting default: got %q, want %q", definition.Default, DataEngineIPFamilyDefault)
	}
	if definition.Required {
		t.Error("preferred data-engine IP family setting must not be required")
	}
	if definition.ReadOnly {
		t.Error("preferred data-engine IP family setting must not be read-only")
	}
	if definition.DataEngineSpecific {
		t.Error("preferred data-engine IP family setting must not be data-engine specific")
	}

	expectedChoices := []any{DataEngineIPFamilyDefault, DataEngineIPFamilyIPv4, DataEngineIPFamilyIPv6}
	if !reflect.DeepEqual(definition.Choices, expectedChoices) {
		t.Errorf("unexpected setting choices: got %#v, want %#v", definition.Choices, expectedChoices)
	}

	foundInSettingNameList := false
	for _, name := range SettingNameList {
		if name == SettingNamePreferredDataEngineIPFamily {
			foundInSettingNameList = true
			break
		}
	}
	if !foundInSettingNameList {
		t.Errorf("setting %q is missing from SettingNameList", SettingNamePreferredDataEngineIPFamily)
	}

	for _, value := range []string{DataEngineIPFamilyDefault, DataEngineIPFamilyIPv4, DataEngineIPFamilyIPv6} {
		if err := ValidateSetting(string(SettingNamePreferredDataEngineIPFamily), value); err != nil {
			t.Errorf("ValidateSetting rejected valid value %q: %v", value, err)
		}
	}

	for _, value := range []string{"ipv3", "IPv4", ""} {
		if err := ValidateSetting(string(SettingNamePreferredDataEngineIPFamily), value); err == nil {
			t.Errorf("ValidateSetting accepted invalid value %q", value)
		}
	}

	if _, err := ParseDataEngineSpecificSetting(definition, `{"v1":"ipv4"}`); err == nil {
		t.Error("ParseDataEngineSpecificSetting accepted a scalar setting definition")
	}
}

func TestParseDataEngineIPFamilyArgs(t *testing.T) {
	tests := []struct {
		name      string
		args      []string
		family    string
		specified bool
		valid     bool
	}{
		{
			name:      "split ipv4",
			args:      []string{"--ip-family", DataEngineIPFamilyIPv4},
			family:    DataEngineIPFamilyIPv4,
			specified: true,
			valid:     true,
		},
		{
			name:      "split mixed case ipv6",
			args:      []string{"--ip-family", "IPv6"},
			family:    DataEngineIPFamilyIPv6,
			specified: true,
			valid:     true,
		},
		{
			name:      "equals ipv4",
			args:      []string{"--ip-family=ipv4"},
			family:    DataEngineIPFamilyIPv4,
			specified: true,
			valid:     true,
		},
		{
			name:      "equals ipv6",
			args:      []string{"--ip-family=ipv6"},
			family:    DataEngineIPFamilyIPv6,
			specified: true,
			valid:     true,
		},
		{name: "absent", args: []string{"daemon"}, valid: true},
		{name: "missing value", args: []string{"--ip-family"}, specified: true},
		{name: "empty value", args: []string{"--ip-family", ""}, specified: true},
		{name: "empty equals value", args: []string{"--ip-family="}, specified: true},
		{name: "unknown value", args: []string{"--ip-family", "ipv3"}, specified: true},
		{name: "padded value", args: []string{"--ip-family", " ipv4"}, specified: true},
		{
			name:      "duplicate split flags",
			args:      []string{"--ip-family", "ipv4", "--ip-family", "ipv6"},
			specified: true,
		},
		{
			name:      "duplicate mixed forms",
			args:      []string{"--ip-family=ipv4", "--ip-family", "ipv6"},
			specified: true,
		},
		{name: "prefix lookalike", args: []string{"--ip-family-extra", "ipv6"}, valid: true},
		{name: "prefix equals lookalike", args: []string{"--ip-family-extra=ipv6"}, valid: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			family, specified, valid := ParseDataEngineIPFamilyArgs(tc.args)
			if family != tc.family || specified != tc.specified || valid != tc.valid {
				t.Fatalf("ParseDataEngineIPFamilyArgs(%v) = (%q, %t, %t), want (%q, %t, %t)",
					tc.args, family, specified, valid, tc.family, tc.specified, tc.valid)
			}
		})
	}
}
