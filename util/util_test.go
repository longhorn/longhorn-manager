package util

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
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

func TestPathSplitJoin(t *testing.T) {
	tests := []struct {
		input string
		want  []string
	}{
		// Basic
		{"a", []string{"a"}},
		{" a ", []string{"a"}},

		// Multi entries
		{"b;a;c", []string{"a", "b", "c"}},

		// Whitespace around entries
		{" b ;   a ;  c  ", []string{"a", "b", "c"}},

		// Empty segments
		{"a;;b", []string{"a", "b"}},
		{"a; ;b", []string{"a", "b"}},

		// All empty
		{";;  ;", nil},

		// PCI BDF formats
		{"0000:00:1e.0", []string{"0000:00:1e.0"}},
		{"0000:5a:04.7", []string{"0000:5a:04.7"}},

		// NVMe controller paths
		{"nvme0n1", []string{"nvme0n1"}},
		{"nvme1c2n1", []string{"nvme1c2n1"}},
		{"nvme-subsys1", []string{"nvme-subsys1"}},

		// /dev paths
		{"/dev/loop14", []string{"/dev/loop14"}},
		{"/dev/sda", []string{"/dev/sda"}},
		{"/dev/nvme0n1p1", []string{"/dev/nvme0n1p1"}},

		// Device mapper
		{"/dev/mapper/vg0-lv_home", []string{"/dev/mapper/vg0-lv_home"}},
		{"/dev/dm-12", []string{"/dev/dm-12"}},

		// by-path / by-id
		{"/dev/disk/by-path/pci-0000:00:1e.0-nvme-1",
			[]string{"/dev/disk/by-path/pci-0000:00:1e.0-nvme-1"}},
		{"/dev/disk/by-id/ata-Samsung_SSD_850",
			[]string{"/dev/disk/by-id/ata-Samsung_SSD_850"}},

		// Weird characters
		{"weird#name?with$bad@chars", []string{"weird#name?with$bad@chars"}},

		// Multiple invalid chars
		{"--abc..def//ghi__", []string{"--abc..def//ghi__"}},

		// Spaces inside path
		{"name with space", []string{"name with space"}},

		// Complex mixed case
		{"/complex/path/0000:00:1f.0/nvme#n1",
			[]string{"/complex/path/0000:00:1f.0/nvme#n1"}},

		// Multiple real paths
		{
			"0000:00:1e.0;   0000:00:1f.0",
			[]string{"0000:00:1e.0", "0000:00:1f.0"},
		},
		// Duplicate paths
		{
			"nvme0n1; nvme0n1; /dev/nvme0n1p1; /dev/nvme0n1p1",
			[]string{"/dev/nvme0n1p1", "nvme0n1"},
		},
	}

	for _, tt := range tests {
		split := SplitPaths(tt.input)
		if !reflect.DeepEqual(split, tt.want) {
			t.Errorf("SplitPaths(%q) = %#v, want %#v", tt.input, split, tt.want)
		}

		// Round-trip test only when expected output non-nil
		if tt.want != nil {
			joined := JoinPaths(split)
			split2 := SplitPaths(joined)

			if !reflect.DeepEqual(split2, tt.want) {
				t.Errorf("RoundTrip mismatch: input=%q split=%#v joined=%q split2=%#v",
					tt.input, split, joined, split2)
			}
		}
	}
}
