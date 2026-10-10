package v113xto1140

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func newSetting(name types.SettingName, value string) *longhorn.Setting {
	return &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{Name: string(name)},
		Value:      value,
	}
}

func TestMigrateIobufLargePoolSizeSetting(t *testing.T) {
	largePoolSizeDefault := `{"v2":"135168"}`
	largePoolCountDefault := `{"v2":"1024"}`

	tests := []struct {
		name                  string
		settings              []*longhorn.Setting
		expectedNewSetting    *longhorn.Setting
		expectedLargePoolSize string
	}{
		{
			name:     "no large pool size setting",
			settings: []*longhorn.Setting{},
		},
		{
			name: "copy default large pool size to large pool count",
			settings: []*longhorn.Setting{
				newSetting(types.SettingNameDataEngineIobufLargePoolSize, `{"v2":"1024"}`),
			},
			expectedNewSetting:    newSetting(types.SettingNameDataEngineIobufLargePoolCount, `{"v2":"1024"}`),
			expectedLargePoolSize: largePoolSizeDefault,
		},
		{
			name: "copy customized large pool size to large pool count",
			settings: []*longhorn.Setting{
				newSetting(types.SettingNameDataEngineIobufLargePoolSize, `{"v2":"4096"}`),
			},
			expectedNewSetting:    newSetting(types.SettingNameDataEngineIobufLargePoolCount, `{"v2":"4096"}`),
			expectedLargePoolSize: largePoolSizeDefault,
		},
		{
			name: "fall back to default large pool count for invalid value",
			settings: []*longhorn.Setting{
				newSetting(types.SettingNameDataEngineIobufLargePoolSize, `{"v2":"invalid"}`),
			},
			expectedNewSetting:    newSetting(types.SettingNameDataEngineIobufLargePoolCount, largePoolCountDefault),
			expectedLargePoolSize: largePoolSizeDefault,
		},
		{
			name: "large pool count already migrated",
			settings: []*longhorn.Setting{
				newSetting(types.SettingNameDataEngineIobufLargePoolSize, largePoolSizeDefault),
				newSetting(types.SettingNameDataEngineIobufLargePoolCount, `{"v2":"4096"}`),
			},
			expectedLargePoolSize: largePoolSizeDefault,
		},
		{
			name: "large pool count already created but large pool size not reset",
			settings: []*longhorn.Setting{
				newSetting(types.SettingNameDataEngineIobufLargePoolSize, `{"v2":"4096"}`),
				newSetting(types.SettingNameDataEngineIobufLargePoolCount, `{"v2":"4096"}`),
			},
			expectedLargePoolSize: largePoolSizeDefault,
		},
		{
			name: "keep valid large buffer size when large pool count exists",
			settings: []*longhorn.Setting{
				newSetting(types.SettingNameDataEngineIobufLargePoolSize, `{"v2":"270336"}`),
				newSetting(types.SettingNameDataEngineIobufLargePoolCount, `{"v2":"4096"}`),
			},
			expectedLargePoolSize: `{"v2":"270336"}`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			settingMap := map[string]*longhorn.Setting{}
			for _, s := range tc.settings {
				settingMap[s.Name] = s
			}

			newSetting, err := migrateIobufLargePoolSizeSetting(settingMap)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedNewSetting, newSetting)

			if tc.expectedLargePoolSize != "" {
				assert.Equal(t, tc.expectedLargePoolSize, settingMap[string(types.SettingNameDataEngineIobufLargePoolSize)].Value)
			}
		})
	}
}
