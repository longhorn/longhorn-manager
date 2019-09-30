package util

import (
	"bytes"
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/handlers"
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"

	"k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/util/validation"

	iscsi_util "github.com/longhorn/go-iscsi-helper/util"
)

const (
	VolumeStackPrefix     = "volume-"
	ControllerServiceName = "controller"
	ReplicaServiceName    = "replica"

	BackupStoreTypeS3 = "s3"
	AWSAccessKey      = "AWS_ACCESS_KEY_ID"
	AWSSecretKey      = "AWS_SECRET_ACCESS_KEY"
	AWSEndPoint       = "AWS_ENDPOINTS"

	HostProcPath     = "/host/proc"
	ReplicaDirectory = "/replicas/"

	DefaultKubernetesTolerationKey = "kubernetes.io"
)

var (
	cmdTimeout     = time.Minute // one minute by default
	reservedLabels = []string{"KubernetesStatus", "RecurringJob", "ranchervm-base-image"}

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
	var err error
	cmd := exec.Command(binary, args...)
	done := make(chan struct{})

	var output, stderr bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &stderr

	go func() {
		err = cmd.Run()
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
		return "", fmt.Errorf("Timeout executing: %v %v, output %s, stderr, %s, error %v",
			binary, args, output.String(), stderr.String(), err)
	}

	if err != nil {
		return "", fmt.Errorf("Failed to execute: %v %v, output %s, stderr, %s, error %v",
			binary, args, output.String(), stderr.String(), err)
	}
	return output.String(), nil
}

func ExecuteWithoutTimeout(binary string, args ...string) (string, error) {
	cmd := exec.Command(binary, args...)

	var output, stderr bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to execute: %v %v, output %s, stderr, %s, error %v",
			binary, args, output.String(), stderr.String(), err)
	}
	return output.String(), nil
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

// ParseLabels parses the provided Labels based on longhorn-engine's implementation:
// https://github.com/longhorn/longhorn-engine/blob/master/util/util.go
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

func ConfigEnvWithCredential(backupTarget string, credentialSecret string, hasEndpoint bool, container *v1.Container) error {
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
		secretKeyEnv := v1.EnvVar{
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
		container.Env = append(container.Env, secretKeyEnv)
		if hasEndpoint {
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
	}
	return nil
}

func GetDiskInfo(directory string) (info *DiskInfo, err error) {
	defer func() {
		err = errors.Wrapf(err, "cannot get disk info of directory %v", directory)
	}()
	initiatorNSPath := iscsi_util.GetHostNamespacePath(HostProcPath)
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
	return nil, errors.Wrapf(err, "cannot finish API request due to too many conflicts")
}

func RunAsync(wg *sync.WaitGroup, f func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		f()
	}()
}

func RemoveHostDirectoryContent(directory string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to remove host directory %v", directory)
	}()

	dir, err := filepath.Abs(filepath.Clean(directory))
	if err != nil {
		return err
	}
	if strings.Count(dir, "/") < 2 {
		return fmt.Errorf("prohibit removing the top level of directory %v", dir)
	}
	initiatorNSPath := iscsi_util.GetHostNamespacePath(HostProcPath)
	nsExec, err := iscsi_util.NewNamespaceExecutor(initiatorNSPath)
	if err != nil {
		return err
	}
	// check if the directory already deleted
	if _, err := nsExec.Execute("ls", []string{dir}); err != nil {
		logrus.Warnf("cannot find host directory %v for removal", dir)
		return nil
	}
	if _, err := nsExec.Execute("rm", []string{"-rf", dir}); err != nil {
		return err
	}
	return nil
}

type filteredLoggingHandler struct {
	filteredPaths  map[string]struct{}
	handler        http.Handler
	loggingHandler http.Handler
}

func FilteredLoggingHandler(filteredPaths map[string]struct{}, writer io.Writer, router http.Handler) http.Handler {

	return filteredLoggingHandler{
		filteredPaths:  filteredPaths,
		handler:        router,
		loggingHandler: handlers.CombinedLoggingHandler(writer, router),
	}
}

func (h filteredLoggingHandler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		if _, exists := h.filteredPaths[req.URL.Path]; exists {
			h.handler.ServeHTTP(w, req)
			return
		}
	}
	h.loggingHandler.ServeHTTP(w, req)
}

func ValidateSnapshotLabels(labels map[string]string) (map[string]string, error) {
	validLabels := make(map[string]string)
	for key, val := range labels {
		if errList := validation.IsQualifiedName(key); len(errList) > 0 {
			return nil, fmt.Errorf("at least one error encountered while validating backup label with key %v: %v",
				key, errList[0])
		}
		if val == "" {
			return nil, fmt.Errorf("value for label with key %v cannot be empty", key)
		}
		validLabels[key] = val
	}

	for _, key := range reservedLabels {
		if _, ok := validLabels[key]; ok {
			return nil, fmt.Errorf("specified snapshot backup labels contain reserved keyword %v", key)
		}
	}

	return validLabels, nil
}

func ValidateTags(inputTags []string) ([]string, error) {
	foundTags := make(map[string]struct{})
	var tags []string
	for _, tag := range inputTags {
		if _, ok := foundTags[tag]; ok {
			continue
		}
		errList := validation.IsQualifiedName(tag)
		if len(errList) > 0 {
			return nil, fmt.Errorf("at least one error encountered while validating tags: %v", errList[0])
		}
		foundTags[tag] = struct{}{}
		tags = append(tags, tag)
	}

	sort.Strings(tags)

	return tags, nil
}

func CreateDiskPath(path string) error {
	nsPath := iscsi_util.GetHostNamespacePath(HostProcPath)
	nsExec, err := iscsi_util.NewNamespaceExecutor(nsPath)
	if err != nil {
		return err
	}
	if _, err := nsExec.Execute("mkdir", []string{"-p", filepath.Join(path + ReplicaDirectory)}); err != nil {
		return errors.Wrapf(err, "error creating data path %v on host", path)
	}

	return nil
}

func CheckDiskPathReplicaSubdirectory(diskPath string) (bool, error) {
	nsPath := iscsi_util.GetHostNamespacePath(HostProcPath)
	nsExec, err := iscsi_util.NewNamespaceExecutor(nsPath)
	if err != nil {
		return false, err
	}
	if _, err := nsExec.Execute("ls", []string{filepath.Join(diskPath, ReplicaDirectory)}); err != nil {
		return false, nil
	}

	return true, nil
}

func IsKubernetesDefaultToleration(toleration v1.Toleration) bool {
	if strings.Contains(toleration.Key, DefaultKubernetesTolerationKey) {
		return true
	}
	return false
}

func AreIdenticalTolerations(oldTolerations, newTolerations map[string]v1.Toleration) bool {
	// modified existing tolerations
	for name := range oldTolerations {
		if !IsKubernetesDefaultToleration(oldTolerations[name]) && !reflect.DeepEqual(newTolerations[name], oldTolerations[name]) {
			return false
		}
	}
	// appended new tolerations
	for name := range newTolerations {
		if _, exist := oldTolerations[name]; !exist {
			return false
		}
	}

	return true
}

func TolerationListToMap(tolerationList []v1.Toleration) map[string]v1.Toleration {
	res := map[string]v1.Toleration{}
	for _, t := range tolerationList {
		res[t.Key] = t
	}
	return res
}
