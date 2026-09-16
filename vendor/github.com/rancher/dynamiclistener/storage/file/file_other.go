//go:build !windows

package file

import (
	"os"

	"github.com/google/renameio/v2"
)

func writeFile(name string, data []byte, perm os.FileMode) error {
	return renameio.WriteFile(name, data, perm, renameio.IgnoreUmask())
}
