package util

import (
	"github.com/sirupsen/logrus"
	"io"
	"os"
)

func NewLogger(logFile string) (logrus.FieldLogger, error) {
	file, err := os.Create(logFile)
	if err != nil {
		return nil, err
	}

	// the debug level is enabled based on a global cli var
	// and it will be set on the package logger
	logger := logrus.New()
	logger.SetLevel(logrus.GetLevel())
	logger.SetOutput(io.MultiWriter(file, os.Stdout))
	logger.Infof("Storing logs at path: %v", logFile)
	return logger, nil
}
