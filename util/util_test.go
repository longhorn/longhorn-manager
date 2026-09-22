package util

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	. "gopkg.in/check.v1"

	lhtypes "github.com/longhorn/go-common-libs/types"

	"github.com/longhorn/longhorn-manager/util/fake"
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

func TestConvertSize(t *testing.T) {
	assert := require.New(t)

	size, err := ConvertSize("0m")
	assert.Nil(err)
	assert.Equal(int64(0), size)

	size, err = ConvertSize("0Mi")
	assert.Nil(err)
	assert.Equal(int64(0), size)

	size, err = ConvertSize("1024k")
	assert.Nil(err)
	assert.Equal(int64(1024*1000), size)

	size, err = ConvertSize("1024Ki")
	assert.Nil(err)
	assert.Equal(int64(1024*1024), size)

	size, err = ConvertSize("1024")
	assert.Nil(err)
	assert.Equal(int64(1024), size)

	size, err = ConvertSize("1Gi")
	assert.Nil(err)
	assert.Equal(int64(1024*1024*1024), size)

	size, err = ConvertSize("1G")
	assert.Nil(err)
	assert.Equal(int64(1e9), size)
}

func TestRoundUpSize(t *testing.T) {
	assert := require.New(t)

	assert.Equal(int64(SizeAlignment), RoundUpSize(0))
	assert.Equal(int64(2*SizeAlignment), RoundUpSize(SizeAlignment+1))
}

func TestDeterministicUUID(t *testing.T) {
	assert := require.New(t)

	dataUsedToGenerate := "Each time DeterministicUUID is called on this data, it outputs the same UUID."
	assert.Equal(DeterministicUUID(dataUsedToGenerate), DeterministicUUID(dataUsedToGenerate))
}

func (s *TestSuite) TestGetValidMountPoint(c *C) {
	// Check if the /host/proc directory exists in container
	if _, err := os.Stat(lhtypes.HostProcDirectory); os.IsNotExist(err) {
		// Create a symbolic link from /proc to /host/proc
		err := os.Symlink("/proc", "/host/proc")
		c.Assert(err, IsNil)
		defer func() {
			_ = os.Remove(lhtypes.HostProcDirectory)
		}()
	}

	fakeDir := fake.CreateTempDirectory("", c)
	defer func() {
		_ = os.RemoveAll(fakeDir)
	}()

	fakeVolumeName := "volume"
	fakeMountFileName := "mount-file"
	fakeProcMountFile := func(procDir, mountFilePath string, isEncryptedDevice bool) {
		// Create a proc PID directory
		procPidDir := filepath.Join(procDir, "1")
		err := os.Mkdir(procPidDir, 0755)
		c.Assert(err, IsNil)

		// Create a mount file
		fakeProcMountFile := fake.CreateTempFile(procPidDir, "mounts", "mock\n", c)

		// Seek to the end of the file and write a byte
		_, err = fakeProcMountFile.Seek(0, io.SeekEnd)
		c.Assert(err, IsNil)

		// Define the device path
		devicePath := filepath.Join("/dev/longhorn", fakeVolumeName)
		if isEncryptedDevice {
			devicePath = filepath.Join("/dev/mapper", fakeVolumeName)
		}

		content := fmt.Sprintf("%s %s ext4 rw,relatime 0 0", devicePath, mountFilePath)
		_, err = fakeProcMountFile.WriteString(content)
		c.Assert(err, IsNil)

		// Read the file content
		readContent, err := os.ReadFile(fakeProcMountFile.Name())
		c.Assert(err, IsNil)

		// Convert the read content to string and split by lines
		lines := strings.Split(string(readContent), "\n")

		// Assert the number of lines and their content
		c.Assert(len(lines), Equals, 2)

		err = fakeProcMountFile.Close()
		c.Assert(err, IsNil)
	}

	type testCase struct {
		isEncryptedDevice       bool
		isInvalidMountPath      bool
		isInvalidMountPointPath bool
		isExpectingError        bool
	}
	testCases := map[string]testCase{
		"getValidMountPoint(...)": {
			isEncryptedDevice: false,
		},
		"getValidMountPoint(...) with encrypted device": {
			isEncryptedDevice: true,
		},
		"getValidMountPoint(...) with invalid mount path": {
			isInvalidMountPath: true,
			isExpectingError:   true,
		},
		"getValidMountPoint(...) with invalid mount point path": {
			isInvalidMountPointPath: true,
			isExpectingError:        true,
		},
	}
	for testName, testCase := range testCases {
		c.Logf("testing util.%v", testName)

		fakeProcDir := fake.CreateTempDirectory(fakeDir, c)

		expectedMountPointPath := filepath.Join(fakeProcDir, fakeMountFileName)

		if !testCase.isInvalidMountPath {
			fakeProcMountFile(fakeProcDir, expectedMountPointPath, testCase.isEncryptedDevice)
		}

		if !testCase.isInvalidMountPointPath {
			fakeMountFile := fake.CreateTempFile(fakeProcDir, fakeMountFileName, "mock", c)
			err := fakeMountFile.Close()
			c.Assert(err, IsNil)
		}

		validMountPoint, err := getValidMountPoint(fakeVolumeName, fakeProcDir, testCase.isEncryptedDevice)
		if testCase.isExpectingError {
			c.Assert(err, NotNil)
		} else {
			c.Assert(err, IsNil)
			c.Assert(validMountPoint, Equals, expectedMountPointPath)
		}
	}
}

func TestParseLabels(t *testing.T) {
	tests := map[string]struct {
		input       []string
		expected    map[string]string
		expectError bool
	}{
		"empty input": {
			input:    []string{},
			expected: map[string]string{},
		},
		"single valid label": {
			input:    []string{"key=value"},
			expected: map[string]string{"key": "value"},
		},
		"multiple valid labels": {
			input:    []string{"key1=value1", "key2=value2"},
			expected: map[string]string{"key1": "value1", "key2": "value2"},
		},
		"value with equals sign": {
			input:    []string{"key=val=val2"},
			expected: map[string]string{"key": "val=val2"},
		},
		"missing equals sign": {
			input:       []string{"noequalssign"},
			expectError: true,
		},
		"empty value": {
			input:       []string{"key="},
			expectError: true,
		},
		"empty key": {
			input:       []string{"=value"},
			expectError: true,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)
			result, err := ParseLabels(tc.input)
			if tc.expectError {
				assert.Error(err)
			} else {
				assert.NoError(err)
				assert.Equal(tc.expected, result)
			}
		})
	}
}

func TestSplitStringToMap(t *testing.T) {
	tests := map[string]struct {
		str       string
		separator string
		expected  map[string]struct{}
	}{
		"empty string": {
			str: "", separator: ",", expected: map[string]struct{}{},
		},
		"single item": {
			str: "a", separator: ",",
			expected: map[string]struct{}{"a": {}},
		},
		"multiple items": {
			str: "a,b,c", separator: ",",
			expected: map[string]struct{}{"a": {}, "b": {}, "c": {}},
		},
		"items with whitespace": {
			str: " a , b , c ", separator: ",",
			expected: map[string]struct{}{"a": {}, "b": {}, "c": {}},
		},
		"trailing separator": {
			str: "a,b,", separator: ",",
			expected: map[string]struct{}{"a": {}, "b": {}},
		},
		"duplicates": {
			str: "a,b,a", separator: ",",
			expected: map[string]struct{}{"a": {}, "b": {}},
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)
			assert.Equal(tc.expected, SplitStringToMap(tc.str, tc.separator))
		})
	}
}

func TestConvertToCamel(t *testing.T) {
	tests := map[string]struct {
		input     string
		separator string
		expected  string
	}{
		"hyphen separated": {
			input: "hello-world", separator: "-", expected: "HelloWorld",
		},
		"underscore separated": {
			input: "foo_bar_baz", separator: "_", expected: "FooBarBaz",
		},
		"single word": {
			input: "hello", separator: "-", expected: "Hello",
		},
		"empty string": {
			input: "", separator: "-", expected: "",
		},
		"trailing separator": {
			input: "hello-", separator: "-", expected: "Hello",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)
			assert.Equal(tc.expected, ConvertToCamel(tc.input, tc.separator))
		})
	}
}

func TestConvertFirstCharToLower(t *testing.T) {
	tests := map[string]struct {
		input    string
		expected string
	}{
		"uppercase first": {
			input: "Hello", expected: "hello",
		},
		"already lowercase": {
			input: "hello", expected: "hello",
		},
		"single character": {
			input: "H", expected: "h",
		},
		"all uppercase": {
			input: "HELLO", expected: "hELLO",
		},
		"empty string": {
			input: "", expected: "",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)
			assert.Equal(tc.expected, ConvertFirstCharToLower(tc.input))
		})
	}
}

func TestTimestampAfterTimestamp(t *testing.T) {
	tests := map[string]struct {
		timestamp1 string
		timestamp2 string
		want       bool
		wantErr    bool
	}{
		"timestamp1BadFormat": {"2024-01-02T18:37Z", "2024-01-02T18:16:37Z", false, true},
		"timestamp2BadFormat": {"2024-01-02T18:16:37Z", "2024-01-02T18:37Z", false, true},
		"timestamp1After":     {"2024-01-02T18:17:37Z", "2024-01-02T18:16:37Z", true, false},
		"timestamp1NotAfter":  {"2024-01-02T18:16:37Z", "2024-01-02T18:17:37Z", false, false},
		"sameTime":            {"2024-01-02T18:16:37Z", "2024-01-02T18:16:37Z", false, false},
	}

	assert := assert.New(t)
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := TimestampAfterTimestamp(tc.timestamp1, tc.timestamp2)
			assert.Equal(tc.want, got)
			if tc.wantErr {
				assert.Error(err)
			} else {
				assert.NoError(err)
			}
		})
	}
}

func TestSanitizeVolatileErrorContent(t *testing.T) {
	tests := []struct {
		name string
		// inputVariants differ only in volatile, per-attempt content (request IDs, timestamps).
		// They must sanitize to the same expected string,
		// so repeated failures compare equal under reflect.DeepEqual and don't trigger a reconcile storm.
		inputVariants []string
		expected      string
	}{
		{
			name:          "empty string",
			inputVariants: []string{""},
			expected:      "",
		},
		{
			name:          "non-volatile content is returned as is",
			inputVariants: []string{"failed to connect to backup target: connection refused"},
			expected:      "failed to connect to backup target: connection refused",
		},
		{
			name:          "HTTP status plus hex request ID (backupstore parseAwsError)",
			inputVariants: []string{"error: 403 1eed0c50c2cb9133", "error: 403 9a8b7c6d5e4f3a2b"},
			expected:      "error: <redacted>",
		},
		{
			name:          "status plus request ID followed by a quote",
			inputVariants: []string{`error="AWS Error: 403 1eed0c50c2cb9133"`, `error="AWS Error: 403 aaaabbbbccccdddd"`},
			expected:      `error="AWS Error: <redacted>"`,
		},
		{
			name:          "literal backslash-n escape directly before status code",
			inputVariants: []string{`failed\n403 1eed0c50c2cb9133`, `failed\n403 ffffeeeeddddcccc`},
			expected:      `failed\n<redacted>`,
		},
		{
			name:          "RequestId in the middle of a sentence keeps surrounding text",
			inputVariants: []string{"operation failed (RequestId: ABC123xyz) please retry", "operation failed (RequestId: ZZZ999abc) please retry"},
			expected:      "operation failed (<redacted>) please retry",
		},
		{
			name:          "multiple timestamps are all replaced",
			inputVariants: []string{`time="2026-07-24T16:31:27.1Z" a time="2026-07-24T16:31:28.2Z" b`},
			expected:      `time="<timestamp>" a time="<timestamp>" b`,
		},
		{
			name:          "short numbers are not treated as request IDs",
			inputVariants: []string{"listening on port 8080, exit status 127 and retrying"},
			expected:      "listening on port 8080, exit status 127 and retrying",
		},
		{
			name: "subprocess log line with timestamp and request ID",
			inputVariants: []string{
				`time="2026-07-24T16:31:27.852675962Z" level=error msg="error: 403 1eed0c50c2cb9133"`,
				`time="2026-07-24T16:36:27.111111111Z" level=error msg="error: 403 aaaabbbbccccdddd"`,
			},
			expected: `time="<timestamp>" level=error msg="error: <redacted>"`,
		},
		{
			name: "sanitize bucket & delimiter addresses",
			inputVariants: []string{
				"{Bucket:0x17df648243b04442587FB7D0A2F9:<nil> Delimiter:0x17df648243c8}",
				"{Bucket:0xc000a1b2c3d0:<nil> Delimiter:0xc000a1b2c3e8}",
			},
			expected: "{Bucket:<redacted>:<nil> Delimiter:<redacted>}",
		},
		{
			name: "non-volatile message untouched, with all pointer addresses, requestID redacted",
			inputVariants: []string{
				`failed to list objects with param: &{Bucket:0x17df64824 RequestId: ABC123xyz:<nil> Delimiter:0x17df648243b0 " +
					"EncodingType: ExpectedBucketOwner:<nil> FetchOwner:<nil> MaxKeys:<nil> OptionalObjectAttributes:[] " +
					"Prefix:0x17df648243a0 RequestPayer: StartAfter:<nil> noSmithyDocumentSerde:{}} " +
					"error: AWS HTTP Error: 0 request send failed, " +
					"Get \\\"https://example.com:9000/backupbucket?delimiter=%2F&list-type=2&prefix=%2F\\\": " +
					"tls: failed to verify certificate: x509: certificate signed by unknown authority\"`,
			},
			expected: `failed to list objects with param: &{Bucket:<redacted> <redacted>:<nil> Delimiter:<redacted> " +
					"EncodingType: ExpectedBucketOwner:<nil> FetchOwner:<nil> MaxKeys:<nil> OptionalObjectAttributes:[] " +
					"Prefix:<redacted> RequestPayer: StartAfter:<nil> noSmithyDocumentSerde:{}} " +
					"error: AWS HTTP Error: 0 request send failed, " +
					"Get \\\"https://example.com:9000/backupbucket?delimiter=%2F&list-type=2&prefix=%2F\\\": " +
					"tls: failed to verify certificate: x509: certificate signed by unknown authority\"`,
		},
		{
			name:          "short hex literal is not treated as a pointer",
			inputVariants: []string{"flags=0x1F"},
			expected:      "flags=0x1F",
		},
		{
			// a pointer address is redacted only when the character before "0x" is
			// a non-word character (e.g. ':', ' ', '=').
			// Here the literal `\n` leaves 'n' directly before "0x", so there is no
			// boundary and the address is intentionally left as is.
			name: "pointer address following a word is untouched",
			inputVariants: []string{
				`failed\n0xc000a1b2c3d0 bucket missing`,
			},
			expected: `failed\n0xc000a1b2c3d0 bucket missing`,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for _, variant := range tc.inputVariants {
				if got := SanitizeVolatileErrorContent(variant); got != tc.expected {
					t.Errorf("SanitizeVolatileErrorContent(%q) = %q, want %q", variant, got, tc.expected)
				}
			}
		})
	}
}
