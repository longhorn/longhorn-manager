package util

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "gopkg.in/check.v1"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	emeta "github.com/longhorn/longhorn-engine/pkg/meta"

	"github.com/longhorn/longhorn-manager/meta"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	TestNamespace = "longhorn-system"
)

const (
	TestErrErrorFmt = "Unexpected error for test case: %s: %v"
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
		_, err := lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), setting, metav1.CreateOptions{})
		assert.Nil(t, err)

		t.Run(tt.name, func(t *testing.T) {
			assert := require.New(t)

			meta.Version = tt.upgradeVersion
			err := newCheckLHUpgradePathSupported(lhClient)
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

func newCheckLHUpgradePathSupported(lhClient lhclientset.Interface) error {
	return checkLHUpgradePath(TestNamespace, lhClient)
}

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	logrus.SetLevel(logrus.TraceLevel)
}

func (s *TestSuite) TestCheckEngineUpgradePathSupported(c *C) {
	type testCase struct {
		currentVersions []emeta.VersionOutput
		upgradeVersion  *emeta.VersionOutput
		expectError     bool
	}
	testCases := map[string]testCase{
		"checkEngineUpgradePath(...)": {
			currentVersions: []emeta.VersionOutput{
				{
					ControllerAPIVersion: emeta.ControllerAPIMinVersion,
					CLIAPIVersion:        emeta.CLIAPIMinVersion,
				},
			},
		},
		"checkEngineUpgradePath(...): multiple engine": {
			currentVersions: []emeta.VersionOutput{
				{
					ControllerAPIVersion: emeta.ControllerAPIMinVersion + 1,
					CLIAPIVersion:        emeta.CLIAPIVersion,
				},
				{
					ControllerAPIVersion: emeta.ControllerAPIVersion,
					CLIAPIVersion:        emeta.CLIAPIMinVersion + 1,
				},
			},
			upgradeVersion: &emeta.VersionOutput{
				ControllerAPIVersion:    emeta.ControllerAPIVersion + 1,
				ControllerAPIMinVersion: emeta.ControllerAPIMinVersion,
				CLIAPIVersion:           emeta.CLIAPIVersion + 1,
				CLIAPIMinVersion:        emeta.CLIAPIMinVersion,
			},
		},
		"checkEngineUpgradePath(...): single engine upgrade path not supported": {
			currentVersions: []emeta.VersionOutput{
				{
					ControllerAPIVersion: emeta.ControllerAPIMinVersion - 1,
					CLIAPIVersion:        emeta.CLIAPIMinVersion,
				},
			},
			expectError: true,
		},
		"checkEngineUpgradePath(...): multiple engine upgrade path not supported": {
			currentVersions: []emeta.VersionOutput{
				{
					ControllerAPIVersion: emeta.ControllerAPIMinVersion,
					CLIAPIVersion:        emeta.CLIAPIMinVersion,
				},
				{
					ControllerAPIVersion: emeta.ControllerAPIMinVersion,
					CLIAPIVersion:        emeta.CLIAPIMinVersion - 1,
				},
			},
			expectError: true,
		},
		"checkEngineUpgradePath(...): downgrade": {
			currentVersions: []emeta.VersionOutput{
				{
					ControllerAPIVersion: emeta.ControllerAPIVersion + 1,
					CLIAPIVersion:        emeta.CLIAPIVersion + 1,
				},
			},
			expectError: true,
		},
		"checkEngineUpgradePath(...): engine image not found": {
			currentVersions: []emeta.VersionOutput{},
		},
	}

	for testName, testCase := range testCases {
		c.Logf("testing upgrade.util.%v", testName)
		lhClient := lhfake.NewSimpleClientset()

		for i, version := range testCase.currentVersions {
			engineImage := &longhorn.EngineImage{
				ObjectMeta: metav1.ObjectMeta{
					Name: fmt.Sprintf("test-%d", i),
				},
				Status: longhorn.EngineImageStatus{
					EngineVersionDetails: longhorn.EngineVersionDetails{
						ControllerAPIVersion: version.ControllerAPIVersion,
						CLIAPIVersion:        version.CLIAPIVersion,
					},
					RefCount: len(testCase.currentVersions),
				},
			}
			_, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), engineImage, metav1.CreateOptions{})
			c.Assert(err, IsNil)
		}

		engimeImages, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).List(context.TODO(), metav1.ListOptions{})
		c.Assert(err, IsNil)
		c.Assert(len(engimeImages.Items), Equals, len(testCase.currentVersions))

		if testCase.upgradeVersion == nil {
			testCase.upgradeVersion = &emeta.VersionOutput{
				ControllerAPIVersion:    emeta.ControllerAPIVersion,
				ControllerAPIMinVersion: emeta.ControllerAPIMinVersion,
				CLIAPIVersion:           emeta.CLIAPIVersion,
				CLIAPIMinVersion:        emeta.CLIAPIMinVersion,
			}
		}

		err = checkEngineUpgradePath(TestNamespace, lhClient, *testCase.upgradeVersion)
		if testCase.expectError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil, Commentf(TestErrErrorFmt, testName, err))
		}
	}
}
