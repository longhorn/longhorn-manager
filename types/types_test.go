package types

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/sirupsen/logrus"

	corev1 "k8s.io/api/core/v1"

	. "gopkg.in/check.v1"
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
