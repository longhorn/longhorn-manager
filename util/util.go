package util

import (
	"crypto/md5"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	iscsi_util "github.com/rancher/go-iscsi-helper/util"
	"github.com/satori/go.uuid"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	VolumeStackPrefix     = "volume-"
	ControllerServiceName = "controller"
	ReplicaServiceName    = "replica"

	BackupStoreTypeS3 = "s3"
	AWSAccessKey      = "AWS_ACCESS_KEY_ID"
	AWSSecretKey      = "AWS_SECRET_ACCESS_KEY"
	AWSEndPoint       = "AWS_ENDPOINTS"
)

var (
	cmdTimeout = time.Minute // one minute by default

	ConflictRetryInterval = 20 * time.Millisecond
	ConflictRetryCounts   = 100
)

type MetadataConfig struct {
	DriverName          string
	Image               string
	OrcImage            string
	DriverContainerName string
}

type DiskInfo struct {
	Fsid             string
	Path             string
	Type             string
	FreeBlock        int64
	TotalBlock       int64
	BlockSize        int64
	StorageMaximum   int64
	StorageAvailable int64
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
		quantity, err := resource.ParseQuantity(size)
		if err != nil {
			return 0, errors.Wrapf(err, "error parsing size '%s'", size)
		}
		return quantity.Value(), nil
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
	return UUID()[:8]
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
	case <-time.After(timeout):
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

func TimestampAfterTimeout(ts string, timeout time.Duration) bool {
	now := time.Now()
	t, err := time.Parse(time.RFC3339, ts)
	if err != nil {
		logrus.Errorf("Cannot parse time %v", ts)
		return false
	}
	deadline := t.Add(timeout)
	return now.After(deadline)
}

func ValidateName(name string) bool {
	validName := regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]+$`)
	return validName.MatchString(name)
}

func GetBackupID(backupURL string) (string, error) {
	u, err := url.Parse(backupURL)
	if err != nil {
		return "", err
	}
	v := u.Query()
	volumeName := v.Get("volume")
	backupName := v.Get("backup")
	if !ValidateName(volumeName) || !ValidateName(backupName) {
		return "", fmt.Errorf("Invalid name parsed, got %v and %v", backupName, volumeName)
	}
	return backupName, nil
}

func GetRequiredEnv(key string) (string, error) {
	env := os.Getenv(key)
	if env == "" {
		return "", fmt.Errorf("can't get required environment variable, env %v wasn't set", key)
	}
	return env, nil
}

func ParseLabels(labels []string) (map[string]string, error) {
	result := map[string]string{}
	for _, label := range labels {
		kv := strings.Split(label, "=")
		if len(kv) != 2 {
			return nil, fmt.Errorf("Invalid label not in <key>=<value> format %v", label)
		}
		key := kv[0]
		value := kv[1]
		if !ValidateName(key) {
			return nil, fmt.Errorf("Invalid key %v for label %v", key, label)
		}
		if !ValidateName(value) {
			return nil, fmt.Errorf("Invalid value %v for label %v", value, label)
		}
		result[key] = value
	}
	return result, nil
}

func RegisterShutdownChannel(done chan struct{}) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		logrus.Infof("Receive %v to exit", sig)
		close(done)
	}()
}

func SplitStringToMap(str, separator string) map[string]struct{} {
	ret := map[string]struct{}{}
	splits := strings.Split(str, separator)
	for _, str := range splits {
		// splits can have empty member
		str = strings.TrimSpace(str)
		if str == "" {
			continue
		}
		ret[str] = struct{}{}
	}
	return ret
}

func GetStringChecksum(data string) string {
	return GetChecksumSHA512([]byte(data))
}

func GetChecksumSHA512(data []byte) string {
	checksum := sha512.Sum512(data)
	return hex.EncodeToString(checksum[:])
}

func CheckBackupType(backupTarget string) (string, error) {
	u, err := url.Parse(backupTarget)
	if err != nil {
		return "", err
	}

	return u.Scheme, nil
}

func ConfigBackupCredential(backupTarget string, credential map[string]string) error {
	backupType, err := CheckBackupType(backupTarget)
	if err != nil {
		return err
	}
	if backupType == BackupStoreTypeS3 {
		// environment variable has been set in cronjob
		if credential != nil && credential[AWSAccessKey] != "" && credential[AWSSecretKey] != "" {
			os.Setenv(AWSAccessKey, credential[AWSAccessKey])
			os.Setenv(AWSSecretKey, credential[AWSSecretKey])
			os.Setenv(AWSEndPoint, credential[AWSEndPoint])
		} else if os.Getenv(AWSAccessKey) == "" || os.Getenv(AWSSecretKey) == "" {
			return fmt.Errorf("Could not backup for %s without credential secret", backupType)
		}
	}
	return nil
}

func ConfigEnvWithCredential(backupTarget string, credentialSecret string, container *v1.Container) error {
	backupType, err := CheckBackupType(backupTarget)
	if err != nil {
		return err
	}
	if backupType == BackupStoreTypeS3 && credentialSecret != "" {
		accessKeyEnv := v1.EnvVar{
			Name: AWSAccessKey,
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: credentialSecret,
					},
					Key: AWSAccessKey,
				},
			},
		}
		container.Env = append(container.Env, accessKeyEnv)
		secreKeyEnv := v1.EnvVar{
			Name: AWSSecretKey,
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: credentialSecret,
					},
					Key: AWSSecretKey,
				},
			},
		}
		container.Env = append(container.Env, secreKeyEnv)
		endpointEnv := v1.EnvVar{
			Name: AWSEndPoint,
			ValueFrom: &v1.EnvVarSource{
				SecretKeyRef: &v1.SecretKeySelector{
					LocalObjectReference: v1.LocalObjectReference{
						Name: credentialSecret,
					},
					Key: AWSEndPoint,
				},
			},
		}
		container.Env = append(container.Env, endpointEnv)
	}
	return nil
}

func GetInitiatorNSPath() string {
	initiatorNSPath := "/host/proc/1/ns"
	pf := iscsi_util.NewProcessFinder("/host/proc")
	ps, err := pf.FindAncestorByName("dockerd")
	if err != nil {
		logrus.Warnf("Failed to find dockerd in the process ancestors, fall back to use pid 1: %v", err)
	} else {
		initiatorNSPath = fmt.Sprintf("/host/proc/%d/ns", ps.Pid)
	}
	return initiatorNSPath
}

func GetDiskInfo(directory string) (*DiskInfo, error) {
	initiatorNSPath := GetInitiatorNSPath()
	mountPath := fmt.Sprintf("--mount=%s/mnt", initiatorNSPath)
	output, err := Execute("nsenter", mountPath, "stat", "-fc", "{\"path\":\"%n\",\"fsid\":\"%i\",\"type\":\"%T\",\"freeBlock\":%f,\"totalBlock\":%b,\"blockSize\":%S}", directory)
	if err != nil {
		return nil, err
	}
	output = strings.Replace(output, "\n", "", -1)

	diskInfo := &DiskInfo{}
	err = json.Unmarshal([]byte(output), diskInfo)
	if err != nil {
		return nil, err
	}

	diskInfo.StorageMaximum = diskInfo.TotalBlock * diskInfo.BlockSize
	diskInfo.StorageAvailable = diskInfo.FreeBlock * diskInfo.BlockSize

	return diskInfo, nil
}

func RetryOnConflictCause(fn func() (interface{}, error)) (obj interface{}, err error) {
	for i := 0; i < ConflictRetryCounts; i++ {
		obj, err = fn()
		if err == nil {
			return obj, nil
		}
		if !apierrors.IsConflict(errors.Cause(err)) {
			return nil, err
		}
		time.Sleep(ConflictRetryInterval)
	}
	return nil, err
}
