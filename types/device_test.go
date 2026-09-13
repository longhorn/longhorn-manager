package types

import (
	"os"
	"path/filepath"

	. "gopkg.in/check.v1"
)

// TestGetDevicePathOfResolvesSymlink verifies that when /proc/mounts reports
// a device path that is a symbolic link (e.g. /dev/mapper/vg1-longhorn ->
// ../dm-2), getDevicePathOf returns the resolved (real) device path. The
// matching entry under /sys/class/block is for the resolved device, so the
// symlink must be followed before the caller looks anything up in /sys.
//
// Regression test for https://github.com/longhorn/longhorn/issues/9479.
func (s *TestSuite) TestGetDevicePathOfResolvesSymlink(c *C) {
	tmp := c.MkDir()

	// Real device backing file (stands in for /dev/dm-2).
	realDevice := filepath.Join(tmp, "dm-2")
	c.Assert(os.WriteFile(realDevice, nil, 0o644), IsNil)

	// Symlink in the same dir (stands in for /dev/mapper/vg1-longhorn).
	symlinkPath := filepath.Join(tmp, "vg1-longhorn")
	c.Assert(os.Symlink(realDevice, symlinkPath), IsNil)

	// Synthetic /proc/mounts mapping /var/lib/longhorn -> the symlink.
	procMounts := filepath.Join(tmp, "mounts")
	c.Assert(os.WriteFile(procMounts, []byte(symlinkPath+" /var/lib/longhorn xfs rw,relatime 0 0\n"), 0o644), IsNil)

	got, err := getDevicePathOf("/var/lib/longhorn", procMounts)
	c.Assert(err, IsNil)
	// The expected value is EvalSymlinks(realDevice), not the literal
	// realDevice: on systems where the temp dir lives under a symlinked
	// prefix (e.g. macOS /var/folders/... -> /private/var/folders/...),
	// EvalSymlinks resolves the whole path, including the prefix.
	expected, err := filepath.EvalSymlinks(realDevice)
	c.Assert(err, IsNil)
	c.Assert(got, Equals, expected)
}

// TestGetDevicePathOfKeepsNonSymlink verifies that the symlink resolution is
// a no-op for regular device paths: the original path is returned unchanged
// so existing behavior is preserved.
func (s *TestSuite) TestGetDevicePathOfKeepsNonSymlink(c *C) {
	tmp := c.MkDir()
	device := filepath.Join(tmp, "sda1")
	c.Assert(os.WriteFile(device, nil, 0o644), IsNil)

	procMounts := filepath.Join(tmp, "mounts")
	c.Assert(os.WriteFile(procMounts, []byte(device+" /mnt xfs rw,relatime 0 0\n"), 0o644), IsNil)

	got, err := getDevicePathOf("/mnt", procMounts)
	c.Assert(err, IsNil)
	// Compare with the EvalSymlinks-resolved form for the same macOS
	// reason as the symlink test: a literal comparison would flake when
	// c.MkDir() returns a path under a symlinked prefix.
	expected, err := filepath.EvalSymlinks(device)
	c.Assert(err, IsNil)
	c.Assert(got, Equals, expected)
}

// TestGetDevicePathOfFallsBackOnBrokenSymlink verifies that when the
// symlink target is missing (dangling link) the function still returns the
// original device path instead of an error, so the existing "log a warning
// and mark the disk type as unknown" behavior is preserved for unusual
// mount configurations.
func (s *TestSuite) TestGetDevicePathOfFallsBackOnBrokenSymlink(c *C) {
	tmp := c.MkDir()
	// Create a symlink whose target does not exist.
	symlinkPath := filepath.Join(tmp, "vg1-longhorn")
	c.Assert(os.Symlink(filepath.Join(tmp, "dm-missing"), symlinkPath), IsNil)

	procMounts := filepath.Join(tmp, "mounts")
	c.Assert(os.WriteFile(procMounts, []byte(symlinkPath+" /var/lib/longhorn xfs rw,relatime 0 0\n"), 0o644), IsNil)

	got, err := getDevicePathOf("/var/lib/longhorn", procMounts)
	c.Assert(err, IsNil)
	c.Assert(got, Equals, symlinkPath)
}
