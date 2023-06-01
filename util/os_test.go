package util

import (
	"bytes"
	"fmt"

	. "gopkg.in/check.v1"
)

func (s *TestSuite) TestGetHostOSDistro(c *C) {
	type testCase struct {
		mockExecute func([]string, string, ...string) (string, error)

		expected    string
		expectError bool
	}
	testCases := map[string]testCase{
		"get host OS distro": {
			mockExecute: func([]string, string, ...string) (string, error) {
				buffer := bytes.NewBufferString(`NAME="SLES"
VERSION="15-SP3"
VERSION_ID="15.3"
PRETTY_NAME="SUSE Linux Enterprise Server 15 SP3"
ID="sles"
ID_LIKE="suse"`)
				return buffer.String(), nil
			},
			expected:    "sles",
			expectError: false,
		},
		"missing host OS distro": {
			mockExecute: func([]string, string, ...string) (string, error) {
				buffer := bytes.NewBufferString(`NAME="SLES"
VERSION="15-SP3"
VERSION_ID="15.3"
PRETTY_NAME="SUSE Linux Enterprise Server 15 SP3"
ID_LIKE="suse"`)
				return buffer.String(), nil
			},
			expected:    "",
			expectError: true,
		},
	}

	for testName, testCase := range testCases {
		fmt.Printf("testing %v\n", testName)

		Execute = testCase.mockExecute

		actual, err := GetHostOSDistro()
		if !testCase.expectError {
			c.Assert(err, IsNil, Commentf(TestErrErrorFmt, testName, err))
		} else {
			c.Assert(err, NotNil)
		}

		c.Assert(actual, Equals, testCase.expected, Commentf(TestErrResultFmt, testName))
	}
}
