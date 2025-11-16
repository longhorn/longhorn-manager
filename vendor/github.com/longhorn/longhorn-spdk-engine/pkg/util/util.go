package util

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"go.uber.org/multierr"

	"k8s.io/apimachinery/pkg/util/validation"

	"github.com/longhorn/go-common-libs/proc"
)

// Param represents a named string parameter.
type Param struct {
	Name  string
	Value string
}

// VerifyParams checks that none of the provided Params are empty.
// It returns an error listing all missing parameter names, or nil if all are present.
func VerifyParams(params ...Param) error {
	var missing []string
	for _, p := range params {
		if strings.TrimSpace(p.Value) == "" {
			missing = append(missing, p.Name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required parameter(s): %s", strings.Join(missing, ", "))
	}
	return nil
}

func RoundUp(num, base uint64) uint64 {
	if num <= 0 {
		return base
	}
	r := num % base
	if r == 0 {
		return num
	}
	return num - r + base
}

const (
	EngineRandomIDLenth = 8
	EngineSuffix        = "-e"
)

func GetVolumeNameFromEngineName(engineName string) string {
	reg := regexp.MustCompile(fmt.Sprintf(`([^"]*)%s-[A-Za-z0-9]{%d,%d}$`, EngineSuffix, EngineRandomIDLenth, EngineRandomIDLenth))
	return reg.ReplaceAllString(engineName, "${1}")
}

func BytesToMiB(bytes uint64) uint64 {
	return bytes / 1024 / 1024
}

func RemovePrefix(path, prefix string) string {
	if strings.HasPrefix(path, prefix) {
		return strings.TrimPrefix(path, prefix)
	}
	return path
}

func UUID() string {
	return uuid.New().String()
}

func IsSPDKTargetProcessRunning() (bool, error) {
	cmd := exec.Command("pgrep", "-f", "spdk_tgt")
	if _, err := cmd.Output(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			status, ok := exitErr.Sys().(syscall.WaitStatus)
			if ok {
				exitCode := status.ExitStatus()
				if exitCode == 1 {
					return false, nil
				}
			}
		}
		return false, errors.Wrap(err, "failed to check spdk_tgt process")
	}
	return true, nil
}

func StartSPDKTgtDaemon() error {
	cmd := exec.Command("spdk_tgt")

	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setsid: true,
	}

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		return fmt.Errorf("failed to start spdk_tgt daemon: %w", err)
	}

	return nil
}

func ForceStopSPDKTgtDaemon(timeout time.Duration) error {
	return stopSPDKTgtDaemon(timeout, syscall.SIGKILL)
}

func StopSPDKTgtDaemon(timeout time.Duration) error {
	return stopSPDKTgtDaemon(timeout, syscall.SIGTERM)
}

func stopSPDKTgtDaemon(timeout time.Duration, signal syscall.Signal) error {
	processes, err := proc.FindProcessByCmdline("spdk_tgt")
	if err != nil {
		return errors.Wrap(err, "failed to find spdk_tgt")
	}

	var errs error
	for _, process := range processes {
		logrus.Infof("Sending signal %v to spdk_tgt %v", signal, process.Pid)
		if err := process.Signal(signal); err != nil {
			errs = multierr.Append(errs, errors.Wrapf(err, "failed to send signal %v to spdk_tgt %v", signal, process.Pid))
		} else {
			done := make(chan error, 1)
			go func() {
				_, err := process.Wait()
				done <- err
				close(done)
			}()

			select {
			case <-time.After(timeout):
				logrus.Warnf("spdk_tgt %v failed to exit in time, sending signal %v", process.Pid, signal)
				err = process.Signal(signal)
				if err != nil {
					errs = multierr.Append(errs, errors.Wrapf(err, "failed to send signal %v to spdk_tgt %v", signal, process.Pid))
				}
			case err := <-done:
				if err != nil {
					errs = multierr.Append(errs, errors.Wrapf(err, "spdk_tgt %v exited with error", process.Pid))
				} else {
					logrus.Infof("spdk_tgt %v exited successfully", process.Pid)
				}
			}
		}
	}

	return errs
}

func GetFileChunkChecksum(filePath string, start, size int64) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer func() {
		if errClose := f.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close file %s", filePath)
		}
	}()

	if _, err = f.Seek(start, 0); err != nil {
		return "", err
	}

	h := sha512.New()
	if _, err := io.CopyN(h, f, size); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
func ParseLabels(labels []string) (map[string]string, error) {
	result := map[string]string{}
	for _, label := range labels {
		kv := strings.SplitN(label, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid label not in <key>=<value> format %v", label)
		}
		key := kv[0]
		value := kv[1]
		if errList := validation.IsQualifiedName(key); len(errList) > 0 {
			return nil, fmt.Errorf("invalid key %v for label: %v", key, errList[0])
		}
		// We don't need to validate the Label value since we're allowing for any form of data to be stored, similar
		// to Kubernetes Annotations. Of course, we should make sure it isn't empty.
		if value == "" {
			return nil, fmt.Errorf("invalid empty value for label with key %v", key)
		}
		result[key] = value
	}
	return result, nil
}

func Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func UnescapeURL(url string) string {
	// Deal with escape in url inputted from bash
	result := strings.Replace(url, "\\u0026", "&", 1)
	result = strings.Replace(result, "u0026", "&", 1)
	result = strings.TrimLeft(result, "\"'")
	result = strings.TrimRight(result, "\"'")
	return result
}

func CombineErrors(errorList ...error) (retErr error) {
	for _, err := range errorList {
		if err != nil {
			if retErr != nil {
				retErr = fmt.Errorf("%v, %v", retErr, err)
			} else {
				retErr = err
			}
		}
	}
	return retErr
}

func Min(x, y uint64) uint64 {
	if x < y {
		return x
	}
	return y
}

type ReplicaError struct {
	Address string
	Message string
}

func NewReplicaError(address string, err error) ReplicaError {
	return ReplicaError{
		Address: address,
		Message: err.Error(),
	}
}

func (e ReplicaError) Error() string {
	return fmt.Sprintf("%v: %v", e.Address, e.Message)
}

type TaskError struct {
	ReplicaErrors []ReplicaError
}

func NewTaskError(res ...ReplicaError) *TaskError {
	return &TaskError{
		ReplicaErrors: append([]ReplicaError{}, res...),
	}
}

func (t *TaskError) Error() string {
	var errs []string
	for _, re := range t.ReplicaErrors {
		errs = append(errs, re.Error())
	}

	if errs == nil {
		return "Unknown"
	}
	if len(errs) == 1 {
		return errs[0]
	}
	return strings.Join(errs, "; ")
}

func (t *TaskError) Append(re ReplicaError) {
	t.ReplicaErrors = append(t.ReplicaErrors, re)
}

func (t *TaskError) HasError() bool {
	return len(t.ReplicaErrors) != 0
}

func GetFileChecksum(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer func() {
		if errClose := f.Close(); errClose != nil {
			logrus.WithError(errClose).Errorf("Failed to close file %s", filePath)
		}
	}()

	h := sha512.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}
