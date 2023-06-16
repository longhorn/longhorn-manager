package types

import (
	"fmt"
	"testing"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"
)

const (
	TestErrResultFmt = "Unexpected result for test case: %s"
)

func Test(t *testing.T) { TestingT(t) }

type TestSuite struct {
}

var _ = Suite(&TestSuite{})

func (s *TestSuite) SetUpTest(c *C) {
	logrus.SetLevel(logrus.DebugLevel)
}

func (s *TestSuite) TestIsSelectorsInTags(c *C) {
	type testCase struct {
		inputTags      []string
		inputSelectors []string

		expected bool
	}
	testCases := map[string]testCase{
		"selectors exist": {
			inputTags:      []string{"aaa", "bbb", "ccc"},
			inputSelectors: []string{"aaa", "bbb", "ccc"},
			expected:       true,
		},
		"selectors mis-matched": {
			inputTags:      []string{"aaa", "bbb", "ccc"},
			inputSelectors: []string{"aaa", "b", "ccc"},
			expected:       false,
		},
		"selectors empty": {
			inputTags:      []string{"aaa", "bbb", "ccc"},
			inputSelectors: []string{},
			expected:       true,
		},
		"tags unsorted": {
			inputTags:      []string{"bbb", "aaa", "ccc"},
			inputSelectors: []string{"aaa", "bbb", "ccc"},
			expected:       true,
		},
		"tags empty": {
			inputTags:      []string{},
			inputSelectors: []string{"aaa", "bbb", "ccc"},
			expected:       false,
		},
	}

	for testName, testCase := range testCases {
		fmt.Printf("testing %v\n", testName)

		actual := IsSelectorsInTags(testCase.inputTags, testCase.inputSelectors)
		c.Assert(actual, Equals, testCase.expected, Commentf(TestErrResultFmt, testName))
	}
}
