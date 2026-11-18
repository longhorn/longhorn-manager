package setting

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestChangedDataEngineValues(t *testing.T) {
	for _, tc := range []struct {
		name      string
		setting   types.SettingName
		oldValue  string
		newValue  string
		expected  []longhorn.DataEngineType
		expectErr bool
	}{
		{
			name:     "empty old value is the default",
			oldValue: "",
			newValue: `{"local":"thick"}`,
			expected: []longhorn.DataEngineType{},
		},
		{
			name:     "same JSON value",
			oldValue: `{"local":"thin"}`,
			newValue: `{"local":"thin"}`,
			expected: []longhorn.DataEngineType{},
		},
		{
			name:     "changed JSON value",
			oldValue: `{"local":"thick"}`,
			newValue: `{"local":"thin"}`,
			expected: []longhorn.DataEngineType{longhorn.DataEngineTypeLocal},
		},
		{
			name:     "plain value applies to every supported data engine",
			oldValue: "",
			newValue: "thin",
			expected: []longhorn.DataEngineType{longhorn.DataEngineTypeLocal},
		},
		{
			name:      "unknown setting",
			setting:   types.SettingName("no-such-setting"),
			oldValue:  "",
			newValue:  "thin",
			expectErr: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			setting := tc.setting
			if setting == "" {
				setting = types.SettingNameDataEngineProvisioningMode
			}
			changed, err := changedDataEngineValues(setting, tc.oldValue, tc.newValue)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.expected, changed)
		})
	}
}
