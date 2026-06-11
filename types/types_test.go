package types

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	corev1 "k8s.io/api/core/v1"
)

const (
	TestErrErrorFmt  = "Unexpected error for test case: %s: %v"
	TestErrResultFmt = "Unexpected result for test case: %s"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	logrus.SetLevel(logrus.DebugLevel)
	c.Assert(os.Unsetenv(LonghornDataPathEnv), IsNil)
	c.Assert(os.Unsetenv(LonghornControlPathEnv), IsNil)
}

func (s *TestSuite) TearDownTest(c *C) {
	c.Assert(os.Unsetenv(LonghornDataPathEnv), IsNil)
	c.Assert(os.Unsetenv(LonghornControlPathEnv), IsNil)
}

func (s *TestSuite) TestGetLonghornDataPath(c *C) {
	c.Assert(GetLonghornDataPath(), Equals, DefaultDataPath)

	customPath := "/data/longhorn/"
	c.Assert(os.Setenv(LonghornDataPathEnv, customPath), IsNil)
	c.Assert(GetLonghornDataPath(), Equals, filepath.Clean(customPath))

	c.Assert(os.Setenv(LonghornDataPathEnv, "relative/path"), IsNil)
	c.Assert(GetLonghornDataPath(), Equals, DefaultDataPath)

	c.Assert(os.Setenv(LonghornDataPathEnv, string(filepath.Separator)), IsNil)
	c.Assert(GetLonghornDataPath(), Equals, DefaultDataPath)

	c.Assert(os.Setenv(LonghornDataPathEnv, "/dev/nvme0n1"), IsNil)
	c.Assert(GetLonghornDataPath(), Equals, "/dev/nvme0n1")

	c.Assert(os.Setenv(LonghornDataPathEnv, "0000:00:1e.0"), IsNil)
	c.Assert(GetLonghornDataPath(), Equals, DefaultDataPath)
}

func (s *TestSuite) TestGetLonghornControlPath(c *C) {
	c.Assert(GetLonghornControlPath(), Equals, DefaultControlPath)

	customPath := "/control/longhorn/"
	c.Assert(os.Setenv(LonghornControlPathEnv, customPath), IsNil)
	c.Assert(GetLonghornControlPath(), Equals, filepath.Clean(customPath))

	c.Assert(os.Setenv(LonghornControlPathEnv, "relative/path"), IsNil)
	c.Assert(GetLonghornControlPath(), Equals, DefaultControlPath)

	c.Assert(os.Setenv(LonghornControlPathEnv, string(filepath.Separator)), IsNil)
	c.Assert(GetLonghornControlPath(), Equals, DefaultControlPath)

	c.Assert(os.Setenv(LonghornControlPathEnv, "/dev/nvme0n1"), IsNil)
	c.Assert(GetLonghornControlPath(), Equals, DefaultControlPath)
}

func (s *TestSuite) TestContainerPathHelpersUseReplicaHostPrefix(c *C) {
	customPath := "/control/longhorn"
	image := "longhornio/longhorn-engine:v1.9.0"

	c.Assert(os.Setenv(LonghornControlPathEnv, customPath), IsNil)

	c.Assert(GetUnixDomainSocketDirectoryInContainer(), Equals,
		filepath.Join(ReplicaHostPrefix, "control/longhorn", UnixDomainSocketDirectorySubpath))
	c.Assert(GetEngineBinaryDirectoryForReplicaManagerContainer(image), Equals,
		filepath.Join(ReplicaHostPrefix, "control/longhorn", EngineBinaryDirectorySubpath, GetImageCanonicalName(image)))
}

func (s *TestSuite) TestParseToleration(c *C) {
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

	for testName, testCase := range testCases {
		fmt.Printf("testing %v\n", testName)

		toleration, err := UnmarshalTolerations(testCase.input)
		if !testCase.expectError {
			c.Assert(err, IsNil, Commentf(TestErrErrorFmt, testName, err))
		} else {
			c.Assert(err, NotNil)
		}

		c.Assert(reflect.DeepEqual(toleration, testCase.expectedToleration), Equals, true, Commentf(TestErrResultFmt, testName))
	}
}

func (s *TestSuite) TestIsSelectorsInTags(c *C) {
	type testCase struct {
		inputTags          []string
		inputSelectors     []string
		allowEmptySelector bool

		expected bool
	}
	testCases := map[string]testCase{
		"selectors exist": {
			inputTags:          []string{"aaa", "bbb", "ccc"},
			inputSelectors:     []string{"aaa", "bbb", "ccc"},
			allowEmptySelector: true,
			expected:           true,
		},
		"selectors mis-matched": {
			inputTags:          []string{"aaa", "bbb", "ccc"},
			inputSelectors:     []string{"aaa", "b", "ccc"},
			allowEmptySelector: true,
			expected:           false,
		},
		"selectors empty and tolerate": {
			inputTags:          []string{"aaa", "bbb", "ccc"},
			inputSelectors:     []string{},
			allowEmptySelector: true,
			expected:           true,
		},
		"selectors empty and not tolerate": {
			inputTags:          []string{"aaa", "bbb", "ccc"},
			inputSelectors:     []string{},
			allowEmptySelector: false,
			expected:           false,
		},
		"tags unsorted": {
			inputTags:          []string{"bbb", "aaa", "ccc"},
			inputSelectors:     []string{"aaa", "bbb", "ccc"},
			allowEmptySelector: true,
			expected:           true,
		},
		"tags empty": {
			inputTags:          []string{},
			inputSelectors:     []string{"aaa", "bbb", "ccc"},
			allowEmptySelector: true,
			expected:           false,
		},
	}

	for testName, testCase := range testCases {
		fmt.Printf("testing %v\n", testName)

		actual := IsSelectorsInTags(testCase.inputTags, testCase.inputSelectors, testCase.allowEmptySelector)
		c.Assert(actual, Equals, testCase.expected, Commentf(TestErrResultFmt, testName))
	}
}

func (s *TestSuite) TestGenerateEngineNameForVolume(c *C) {
	type testCase struct {
		volumeName        string
		currentEngineName string

		expectedEngineName string
	}
	testCases := map[string]testCase{
		"case 1: testvol, new engine": {
			volumeName:         "testvol",
			currentEngineName:  "",
			expectedEngineName: "testvol-e-0",
		},
		"case 2: testvol, new engine from old engine version": {
			volumeName:         "testvol",
			currentEngineName:  "testvol-e-abcdefgh",
			expectedEngineName: "testvol-e-1",
		},
		"case 3: testvol, new engine from new engine version": {
			volumeName:         "testvol",
			currentEngineName:  "testvol-e-0",
			expectedEngineName: "testvol-e-1",
		},
		"case 4: testvol, newer engine from new engine version": {
			volumeName:         "testvol",
			currentEngineName:  "testvol-e-1",
			expectedEngineName: "testvol-e-2",
		},
		"case 5: test-vol, new engine": {
			volumeName:         "test-vol",
			currentEngineName:  "",
			expectedEngineName: "test-vol-e-0",
		},
		"case 6: test-vol, new engine from old engine version": {
			volumeName:         "test-vol",
			currentEngineName:  "test-vol-e-xxxxxxxx",
			expectedEngineName: "test-vol-e-1",
		},
		"case 7: test-vol, new engine from new engine version": {
			volumeName:         "test-vol",
			currentEngineName:  "test-vol-e-0",
			expectedEngineName: "test-vol-e-1",
		},
		"case 8: test-vol, newer engine from new engine version": {
			volumeName:         "test-vol",
			currentEngineName:  "test-vol-e-1",
			expectedEngineName: "test-vol-e-2",
		},
	}

	for testName, testCase := range testCases {
		fmt.Printf("testing %v\n", testName)

		actual := GenerateEngineNameForVolume(testCase.volumeName, testCase.currentEngineName)
		c.Assert(actual, Equals, testCase.expectedEngineName, Commentf(TestErrResultFmt, testName))
	}
}

func (s *TestSuite) TestGenerateEngineFrontendNameForVolume(c *C) {
	type testCase struct {
		volumeName                string
		currentEngineFrontendName string

		expectedEngineFrontendName string
	}
	testCases := map[string]testCase{
		"case 1: testvol, new engine frontend": {
			volumeName:                 "testvol",
			currentEngineFrontendName:  "",
			expectedEngineFrontendName: "testvol-ef-0",
		},
		"case 2: testvol, new engine frontend from old format": {
			volumeName:                 "testvol",
			currentEngineFrontendName:  "testvol-ef",
			expectedEngineFrontendName: "testvol-ef-1",
		},
		"case 3: testvol, next engine frontend": {
			volumeName:                 "testvol",
			currentEngineFrontendName:  "testvol-ef-0",
			expectedEngineFrontendName: "testvol-ef-1",
		},
		"case 4: test-vol, newer engine frontend": {
			volumeName:                 "test-vol",
			currentEngineFrontendName:  "test-vol-ef-1",
			expectedEngineFrontendName: "test-vol-ef-2",
		},
	}

	for testName, testCase := range testCases {
		fmt.Printf("testing %v\n", testName)

		actual := GenerateEngineFrontendNameForVolume(testCase.volumeName, testCase.currentEngineFrontendName)
		c.Assert(actual, Equals, testCase.expectedEngineFrontendName, Commentf(TestErrResultFmt, testName))
	}
}

func (s *TestSuite) TestValidateManagerURL(c *C) {
	type testCase struct {
		input       string
		expectError bool
	}
	testCases := map[string]testCase{
		"empty URL is valid (disabled)": {
			input:       "",
			expectError: false,
		},
		"valid https URL": {
			input:       "https://longhorn.example.com",
			expectError: false,
		},
		"valid http URL": {
			input:       "http://longhorn.example.com",
			expectError: false,
		},
		"valid URL with custom port": {
			input:       "https://longhorn.example.com:8443",
			expectError: false,
		},
		"valid URL with default https port": {
			input:       "https://longhorn.example.com:443",
			expectError: false,
		},
		"valid URL with default http port": {
			input:       "http://longhorn.example.com:80",
			expectError: false,
		},
		"valid IPv6 URL": {
			input:       "http://[2001:db8::1]:9500",
			expectError: false,
		},
		"valid IPv6 URL without port": {
			input:       "http://[::1]",
			expectError: false,
		},
		"invalid scheme": {
			input:       "ftp://longhorn.example.com",
			expectError: true,
		},
		"missing scheme": {
			input:       "longhorn.example.com",
			expectError: true,
		},
		"URL with path": {
			input:       "https://longhorn.example.com/api",
			expectError: true,
		},
		"URL with query": {
			input:       "https://longhorn.example.com?foo=bar",
			expectError: true,
		},
		"URL with fragment": {
			input:       "https://longhorn.example.com#section",
			expectError: true,
		},
		"URL with trailing slash": {
			input:       "https://longhorn.example.com/",
			expectError: false,
		},
		"malformed URL": {
			input:       "://invalid",
			expectError: true,
		},
	}

	for testName, testCase := range testCases {
		fmt.Printf("testing %v\n", testName)

		err := ValidateManagerURL(testCase.input)
		if !testCase.expectError {
			c.Assert(err, IsNil, Commentf(TestErrErrorFmt, testName, err))
		} else {
			c.Assert(err, NotNil, Commentf("Expected error for test case: %s", testName))
		}
	}
}

func (s *TestSuite) TestCPUListToHexMask(c *C) {
	type testCase struct {
		input       string
		expected    string
		expectError bool
	}
	testCases := map[string]testCase{
		"single cpu 0": {
			input:    "0",
			expected: "0x1",
		},
		"single cpu 1": {
			input:    "1",
			expected: "0x2",
		},
		"single cpu 7": {
			input:    "7",
			expected: "0x80",
		},
		"range 0-3": {
			input:    "0-3",
			expected: "0xf",
		},
		"range 1-3": {
			input:    "1-3",
			expected: "0xe",
		},
		"comma separated": {
			input:    "1,2,3",
			expected: "0xe",
		},
		"mixed range and individual": {
			input:    "1-3,5,7",
			expected: "0xae",
		},
		"parenthesized group": {
			input:    "(0-3)",
			expected: "0xf",
		},
		"multiple parenthesized groups": {
			input:    "(1-3),(5)",
			expected: "0x2e",
		},
		"high cpu numbers": {
			input:    "32,63",
			expected: "0x8000000100000000",
		},
		"cpu beyond 64": {
			input:    "64,127",
			expected: "0x80000000000000010000000000000000",
		},
		"large range 0-127": {
			input:    "0-127",
			expected: "0xffffffffffffffffffffffffffffffff",
		},
		"empty string": {
			input:       "",
			expectError: true,
		},
		"leading comma": {
			input:       ",1",
			expectError: true,
		},
		"trailing comma": {
			input:       "1,",
			expectError: true,
		},
		"empty middle entry": {
			input:       "1,,3",
			expectError: true,
		},
		"whitespace-only middle entry": {
			input:       "1,   ,3",
			expectError: true,
		},
		"invalid format": {
			input:       "abc",
			expectError: true,
		},
		"invalid range reversed": {
			input:       "5-3",
			expectError: true,
		},
		"cpu number too high": {
			input:       "1024",
			expectError: true,
		},
	}

	for testName, testCase := range testCases {
		result, err := CPUListToHexMask(testCase.input)
		if testCase.expectError {
			c.Assert(err, NotNil, Commentf("Expected error for test case: %s", testName))
		} else {
			c.Assert(err, IsNil, Commentf(TestErrErrorFmt, testName, err))
			c.Assert(result, Equals, testCase.expected, Commentf(TestErrResultFmt, testName))
		}
	}
}

func (s *TestSuite) TestNormalizeCPUMask(c *C) {
	type testCase struct {
		input       string
		expected    string
		expectError bool
	}
	testCases := map[string]testCase{
		"hex mask lowercase": {
			input:    "0xff",
			expected: "0xff",
		},
		"hex mask uppercase": {
			input:    "0xFF",
			expected: "0xFF",
		},
		"hex mask single": {
			input:    "0x1",
			expected: "0x1",
		},
		"cpu list single": {
			input:    "0",
			expected: "0x1",
		},
		"cpu list range": {
			input:    "0-3",
			expected: "0xf",
		},
		"cpu list mixed": {
			input:    "1-3,5",
			expected: "0x2e",
		},
		"cpu list beyond 64": {
			input:    "64,65",
			expected: "0x30000000000000000",
		},
		"hex mask zero": {
			input:       "0x0",
			expectError: true,
		},
		"hex mask large": {
			input:    "0xffffffffffffffffffffffffffffffff",
			expected: "0xffffffffffffffffffffffffffffffff",
		},
		"hex mask exceeds max cpu": {
			input:       "0x" + strings.Repeat("f", 257), // 1028 bits, exceeds maxCPU (1023)
			expectError: true,
		},
		"empty string": {
			input:       "",
			expectError: true,
		},
		"invalid hex": {
			input:       "0xZZ",
			expectError: true,
		},
	}

	for testName, testCase := range testCases {
		result, err := NormalizeCPUMask(testCase.input)
		if testCase.expectError {
			c.Assert(err, NotNil, Commentf("Expected error for test case: %s", testName))
		} else {
			c.Assert(err, IsNil, Commentf(TestErrErrorFmt, testName, err))
			c.Assert(result, Equals, testCase.expected, Commentf(TestErrResultFmt, testName))
		}
	}
}

func (s *TestSuite) TestIsHexCPUMask(c *C) {
	type testCase struct {
		input    string
		expected bool
	}
	testCases := map[string]testCase{
		"valid 0x1":      {input: "0x1", expected: true},
		"valid 0xff":     {input: "0xff", expected: true},
		"valid 0xFF":     {input: "0xFF", expected: true},
		"valid 0X1":      {input: "0X1", expected: true},
		"just 0x":        {input: "0x", expected: false},
		"no prefix":      {input: "ff", expected: false},
		"cpu list":       {input: "1-3,5", expected: false},
		"decimal number": {input: "123", expected: false},
		"empty":          {input: "", expected: false},
		"invalid hex":    {input: "0xGG", expected: false},
	}

	for testName, testCase := range testCases {
		result := IsHexCPUMask(testCase.input)
		c.Assert(result, Equals, testCase.expected, Commentf(TestErrResultFmt, testName))
	}
}
