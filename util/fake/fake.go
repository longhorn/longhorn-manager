package fake

import (
	"os"
	"path/filepath"

	checkv1 "gopkg.in/check.v1"
)

func CreateTempDirectory(parentDirectory string, c *checkv1.C) string {
	if parentDirectory != "" {
		_, err := os.Stat(parentDirectory)
		c.Assert(err, checkv1.IsNil)
	}

	tempDir, err := os.MkdirTemp(parentDirectory, "test-")
	c.Assert(err, checkv1.IsNil)
	return tempDir
}

func CreateTempFile(directory, fileName, content string, c *checkv1.C) *os.File {
	var err error
	var tempFile *os.File
	if fileName != "" {
		tempFile, err = os.Create(filepath.Join(directory, fileName))
		c.Assert(err, checkv1.IsNil)
	} else {
		tempFile, err = os.CreateTemp(directory, "test-")
		c.Assert(err, checkv1.IsNil)
	}

	_, err = tempFile.WriteString(content)
	c.Assert(err, checkv1.IsNil)

	return tempFile
}
