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
