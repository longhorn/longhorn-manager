package util

import (
	"crypto/md5"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/docker/go-units"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
)

const (
	VolumeStackPrefix     = "volume-"
	ControllerServiceName = "controller"
	ReplicaServiceName    = "replica"
)

var (
	cmdTimeout = time.Minute // one minute by default
)

type MetadataConfig struct {
	DriverName          string
	Image               string
	OrcImage            string
	DriverContainerName string
}

func VolumeStackName(volumeName string) string {
	nameNoUnderscores := strings.Replace(volumeName, "_", "-", -1)
	stackName := VolumeStackPrefix + nameNoUnderscores
	if len(stackName) > 63 {
		hash := fmt.Sprintf("%x", md5.Sum([]byte(nameNoUnderscores)))
		leftover := 63 - (len(VolumeStackPrefix) + len(hash) + 1)
		partialName := nameNoUnderscores[0:leftover]
		stackName = VolumeStackPrefix + partialName + "-" + hash
	}
	return stackName
}

func ControllerAddress(volumeName string) string {
	return fmt.Sprintf("%s.%s.rancher.internal", ControllerServiceName, VolumeStackName(volumeName))
}

func ReplicaAddress(name, volumeName string) string {
	return fmt.Sprintf("%s.rancher.internal", name)
}

func ReplicaName(address, volumeName string) string {
	s := strings.TrimSuffix(strings.TrimPrefix(address, "tcp://"), ":9502")
	s = strings.TrimSuffix(s, ".rancher.internal")
	return strings.TrimSuffix(s, "."+VolumeStackName(volumeName))
}

func ConvertSize(size interface{}) (int64, error) {
	switch size := size.(type) {
	case int64:
		return size, nil
	case int:
		return int64(size), nil
	case string:
		if size == "" {
			return 0, nil
		}
		sizeInBytes, err := units.RAMInBytes(size)
		if err != nil {
			return 0, errors.Wrapf(err, "error parsing size '%s'", size)
		}
		return sizeInBytes, nil
	}
	return 0, errors.Errorf("could not parse size '%v'", size)
}

func RoundUpSize(size int64) int64 {
	if size <= 0 {
		return 4096
	}
	r := size % 4096
	if r == 0 {
		return size
	}
	return size - r + 4096
}

func Backoff(maxDuration time.Duration, timeoutMessage string, f func() (bool, error)) error {
	startTime := time.Now()
	waitTime := 150 * time.Millisecond
	maxWaitTime := 2 * time.Second
	for {
		if time.Now().Sub(startTime) > maxDuration {
			return errors.New(timeoutMessage)
		}

		if done, err := f(); err != nil {
			return err
		} else if done {
			return nil
		}

		time.Sleep(waitTime)

		waitTime *= 2
		if waitTime > maxWaitTime {
			waitTime = maxWaitTime
		}
	}
}

func UUID() string {
	return uuid.NewV4().String()
}

// WaitForDevice timeout in second
func WaitForDevice(dev string, timeout int) error {
	for i := 0; i < timeout; i++ {
		st, err := os.Stat(dev)
		if err == nil {
			if st.Mode()&os.ModeDevice == 0 {
				return fmt.Errorf("Invalid mode for %v: 0x%x", dev, st.Mode())
			}
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for %v", dev)
}

func RandomID() string {
	return UUID()[:18]
}

func GetLocalIPs() ([]string, error) {
	results := []string{}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil, err
	}
	for _, addr := range addrs {
		if ip, ok := addr.(*net.IPNet); ok && !ip.IP.IsLoopback() {
			if ip.IP.To4() != nil {
				results = append(results, ip.IP.String())
			}
		}
	}
	return results, nil
}

// WaitForAPI timeout in second
func WaitForAPI(url string, timeout int) error {
	for i := 0; i < timeout; i++ {
		resp, err := http.Get(url)
		if err == nil {
			resp.Body.Close()
			return nil
		}
		time.Sleep(1 * time.Second)
	}
	return fmt.Errorf("timeout waiting for %v", url)
}

func Now() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func ParseTime(t string) (time.Time, error) {
	return time.Parse(time.RFC3339, t)

}

func Execute(binary string, args ...string) (string, error) {
	return ExecuteWithTimeout(cmdTimeout, binary, args...)
}

func ExecuteWithTimeout(timeout time.Duration, binary string, args ...string) (string, error) {
	var output []byte
	var err error
	cmd := exec.Command(binary, args...)
	done := make(chan struct{})

	go func() {
		output, err = cmd.CombinedOutput()
		done <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(cmdTimeout):
		if cmd.Process != nil {
			if err := cmd.Process.Kill(); err != nil {
				logrus.Warnf("Problem killing process pid=%v: %s", cmd.Process.Pid, err)
			}

		}
		return "", fmt.Errorf("Timeout executing: %v %v, output %v, error %v", binary, args, string(output), err)
	}

	if err != nil {
		return "", fmt.Errorf("Failed to execute: %v %v, output %v, error %v", binary, args, string(output), err)
	}
	return string(output), nil
}

func TimestampAfterTimeout(ts string, timeoutInSeconds int) bool {
	return false
}
