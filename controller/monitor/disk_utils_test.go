package monitor

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestSyncDirectory covers longhorn/longhorn#14102: the disk config file must be durable before its UUID is
// recorded, so syncing the data path has to work on a directory and report a path that cannot be synced.
func TestSyncDirectory(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "longhorn-disk.cfg"), []byte("{}"), 0644))
	require.NoError(t, os.Mkdir(filepath.Join(dir, "replicas"), 0755))

	require.NoError(t, syncDirectory(dir))

	require.Error(t, syncDirectory(filepath.Join(dir, "missing")))
}
