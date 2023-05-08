package util

import (
	"context"
	"fmt"
	"sync"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	"github.com/longhorn/longhorn-manager/meta"
	"github.com/longhorn/longhorn-manager/types"

	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"

	"github.com/stretchr/testify/require"
)

const (
	TestNamespace = "longhorn-system"
)

func TestProgressMonitorInc(t *testing.T) {
	expectedTargetValue := 1000
	expectedValue := 737
	expectedCurrentProgressInPercentage := 73.0

	pm := NewProgressMonitor("Test PM", 0, expectedTargetValue)

	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var count int
			switch id {
			case 0:
				count = 250
			case 1:
				count = 247
			case 2:
				count = 240
			}
			for j := 0; j < count; j++ {
				pm.Inc()
			}
		}(i)
	}

	wg.Wait()

	currentValue, targetValue, currentProgressInPercentage := pm.GetCurrentProgress()
	if currentValue != expectedValue {
		t.Fatalf(`currentValue = %v, currentedValue = %v`, currentValue, expectedValue)
	}
	if currentProgressInPercentage != expectedCurrentProgressInPercentage {
		t.Fatalf(`currentProgressInPercentage = %v, expectedCurrentProgressInPercentage = %v`, currentProgressInPercentage, expectedCurrentProgressInPercentage)
	}
	if targetValue != expectedTargetValue {
		t.Fatalf(`targetValue = %v, expectedTargetValue = %v`, targetValue, expectedTargetValue)
	}
}

func TestCheckUpgradePathSupported(t *testing.T) {
	testCases := []struct {
		name           string
		currentVersion string
		upgradeVersion string
		expectError    bool
	}{
		{
			name:           "new install",
			currentVersion: "",
			upgradeVersion: "v1.5.0",
			expectError:    false,
		},
		{
			name:           "major version downgrade",
			currentVersion: "v2.0.0",
			upgradeVersion: "v1.5.0",
			expectError:    true,
		},
		{
			name:           "upgrade across two major versions",
			currentVersion: "v1.5.0",
			upgradeVersion: "v3.0.0",
			expectError:    true,
		},
		{
			name:           "major version upgrade with lower minor version",
			currentVersion: "v1.5.0",
			upgradeVersion: "v2.0.0",
			expectError:    true,
		},
		{
			name:           "major version upgrade with upgradable minor version",
			currentVersion: "v1.30.0",
			upgradeVersion: "v2.0.0",
			expectError:    false,
		},
		{
			name:           "upgrade across two minor versions",
			currentVersion: "v1.3.0-dev",
			upgradeVersion: "v1.5.0",
			expectError:    true,
		},
		{
			name:           "upgradable minor version",
			currentVersion: "v1.4.1",
			upgradeVersion: "v1.5.0",
			expectError:    false,
		},
		{
			name:           "minor version downgrade",
			currentVersion: "v1.5.0-dev",
			upgradeVersion: "v1.4.0",
			expectError:    true,
		},
		{
			name:           "patch version upgrade across one patch version 1",
			currentVersion: "v1.5.0",
			upgradeVersion: "v1.5.1",
			expectError:    false,
		},
		{
			name:           "patch version upgrade across one patch version 2",
			currentVersion: "v1.5.2",
			upgradeVersion: "v1.5.3",
			expectError:    false,
		},
		{
			name:           "patch version upgrade across patch versions",
			currentVersion: "v1.5.0",
			upgradeVersion: "v1.5.9",
			expectError:    false,
		},
		{
			name:           "patch version downgrade",
			currentVersion: "v1.5.1-dev",
			upgradeVersion: "v1.5.0",
			expectError:    true,
		},
		{
			name:           "upgradable pre-release version",
			currentVersion: "v1.5.0-rc1",
			upgradeVersion: "v1.5.0-rc3",
			expectError:    false,
		},
	}

	for _, tt := range testCases {
		lhClient := lhfake.NewSimpleClientset()
		setting := &longhorn.Setting{
			ObjectMeta: metav1.ObjectMeta{
				Name: string(types.SettingNameCurrentLonghornVersion),
			},
			Value: tt.currentVersion,
		}
		lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})

		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)

			meta.Version = tt.upgradeVersion
			err := newCheckUpgradePathSupported(lhClient)
			if tt.expectError {
				fmt.Println(err)
				assert.NotNil(err)
			} else {
				fmt.Println(err)
				assert.Nil(err)
			}
		})
	}
}

func newCheckUpgradePathSupported(lhClient lhclientset.Interface) error {
	return CheckUpgradePathSupported(TestNamespace, lhClient)
}
