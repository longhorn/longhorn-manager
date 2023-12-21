package fake

import (
	"fmt"
	"io/fs"
	"strings"
)

// FileSystem is a mock implementation of the fs.FS interface
type FileSystem struct {
	DirEntries []fs.DirEntry
	Data       []byte
}

func (fs *FileSystem) ReadDir(string) ([]fs.DirEntry, error) {
	return fs.DirEntries, nil
}

func (fs *FileSystem) ReadFile(filePath string) ([]byte, error) {
	if fs.Data != nil {
		return fs.Data, nil
	}

	for i, dirEntry := range fs.DirEntries {
		if strings.Contains(filePath, dirEntry.Name()) {
			return []byte(fmt.Sprintf("8:%d\n", i)), nil
		}
	}

	return nil, nil
}

func DirEntry(name string, isDir bool) fs.DirEntry {
	return &mockFile{name: name, isDir: isDir}
}

type mockFile struct {
	name  string
	isDir bool
}

func (m *mockFile) Name() string               { return m.name }
func (m *mockFile) IsDir() bool                { return m.isDir }
func (m *mockFile) Type() fs.FileMode          { return 0 }
func (m *mockFile) Info() (fs.FileInfo, error) { return nil, nil }
