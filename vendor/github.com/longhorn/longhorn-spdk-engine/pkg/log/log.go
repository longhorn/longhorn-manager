package log

import (
	"reflect"
	"sync"

	"github.com/jinzhu/copier"
	"github.com/sirupsen/logrus"
)

type SafeLogger struct {
	sync.RWMutex

	logger logrus.Ext1FieldLogger
}

// NewSafeLogger creates a new thread-safe logger instance
func NewSafeLogger(logger logrus.Ext1FieldLogger) *SafeLogger {
	if logger == nil {
		logger = logrus.StandardLogger()
	}
	return &SafeLogger{
		logger: logger,
	}
}

// WithError adds an error field to the logger thread-safely
func (s *SafeLogger) WithError(err error) logrus.Ext1FieldLogger {
	s.RLock()
	defer s.RUnlock()
	return s.logger.WithError(err)
}

// Trace logs a trace message thread-safely
func (s *SafeLogger) Trace(args ...interface{}) {
	s.Lock()
	defer s.Unlock()
	s.logger.Trace(args...)
}

// Tracef logs a formatted trace message thread-safely
func (s *SafeLogger) Tracef(format string, args ...interface{}) {
	s.Lock()
	defer s.Unlock()
	s.logger.Tracef(format, args...)
}

// Debug logs a debug message thread-safely
func (s *SafeLogger) Debug(args ...interface{}) {
	s.Lock()
	defer s.Unlock()
	s.logger.Debug(args...)
}

// Debugf logs a formatted debug message thread-safely
func (s *SafeLogger) Debugf(format string, args ...interface{}) {
	s.Lock()
	defer s.Unlock()
	s.logger.Debugf(format, args...)
}

// Info logs an info message thread-safely
func (s *SafeLogger) Info(args ...interface{}) {
	s.Lock()
	defer s.Unlock()
	s.logger.Info(args...)
}

// Infof logs a formatted info message thread-safely
func (s *SafeLogger) Infof(format string, args ...interface{}) {
	s.Lock()
	defer s.Unlock()
	s.logger.Infof(format, args...)
}

// Warn logs a warning message thread-safely
func (s *SafeLogger) Warn(args ...interface{}) {
	s.Lock()
	defer s.Unlock()
	s.logger.Warn(args...)
}

// Warnf logs a formatted warning message thread-safely
func (s *SafeLogger) Warnf(format string, args ...interface{}) {
	s.Lock()
	defer s.Unlock()
	s.logger.Warnf(format, args...)
}

// Error logs an error message thread-safely
func (s *SafeLogger) Error(args ...interface{}) {
	s.Lock()
	defer s.Unlock()
	s.logger.Error(args...)
}

// Errorf logs a formatted error message thread-safely
func (s *SafeLogger) Errorf(format string, args ...interface{}) {
	s.Lock()
	defer s.Unlock()
	s.logger.Errorf(format, args...)
}

// WithField adds a field to the logger thread-safely
func (s *SafeLogger) WithField(key string, value interface{}) logrus.Ext1FieldLogger {
	s.RLock()
	defer s.RUnlock()
	return s.logger.WithField(key, value)
}

// WithFields adds multiple fields to the logger thread-safely
func (s *SafeLogger) WithFields(fields logrus.Fields) logrus.Ext1FieldLogger {
	s.RLock()
	defer s.RUnlock()
	return s.logger.WithFields(fields)
}

// UpdateLogger updates the logger with new fields thread-safely
func (s *SafeLogger) UpdateLogger(fields logrus.Fields) error {
	s.Lock()
	defer s.Unlock()

	newFields := make(logrus.Fields)
	for k, v := range fields {
		if reflect.TypeOf(v).Kind() == reflect.Map && reflect.TypeOf(v).Key().Kind() == reflect.String {
			newMap := make(map[string]interface{})
			err := copier.Copy(&newMap, v)
			if err != nil {
				return err
			}
			newFields[k] = newMap
		} else {
			newFields[k] = v
		}
	}

	s.logger = s.logger.WithFields(newFields)

	return nil
}
