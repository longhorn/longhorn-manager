package fake

import (
	"os"
	"path/filepath"

	"gopkg.in/check.v1"
)

func CreateTempDirectory(parentDirectory string, c *check.C) string {
	if parentDirectory != "" {
		_, err := os.Stat(parentDirectory)
		c.Assert(err, check.IsNil)
	}

	tempDir, err := os.MkdirTemp(parentDirectory, "test-")
	c.Assert(err, check.IsNil)
	return tempDir
}

func CreateTempFile(directory, fileName, content string, c *check.C) *os.File {
	var err error
	var tempFile *os.File
	if fileName != "" {
		tempFile, err = os.Create(filepath.Join(directory, fileName))
		c.Assert(err, check.IsNil)
	} else {
		tempFile, err = os.CreateTemp(directory, "test-")
		c.Assert(err, check.IsNil)
	}

	_, err = tempFile.WriteString(content)
	c.Assert(err, check.IsNil)

	return tempFile
}
