package fake

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func CreateTempDirectory(parentDirectory string, t *testing.T) string {
	if parentDirectory != "" {
		_, err := os.Stat(parentDirectory)
		assert.NoError(t, err)
	}

	tempDir, err := os.MkdirTemp(parentDirectory, "test-")
	assert.NoError(t, err)
	return tempDir
}

func CreateTempFile(directory, fileName, content string, t *testing.T) *os.File {
	var err error
	var tempFile *os.File
	if fileName != "" {
		tempFile, err = os.Create(filepath.Join(directory, fileName))
		assert.NoError(t, err)
	} else {
		tempFile, err = os.CreateTemp(directory, "test-")
		assert.NoError(t, err)
	}

	_, err = tempFile.WriteString(content)
	assert.NoError(t, err)

	return tempFile
}

func CreateTempSparseFile(directory, fileName string, fileSize int64, content string, t *testing.T) *os.File {
	tempFile := CreateTempFile(directory, fileName, content, t)

	err := tempFile.Truncate(fileSize)
	assert.NoError(t, err)

	return tempFile
}
