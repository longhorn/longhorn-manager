package manager

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"github.com/rancher/longhorn-manager/util"
)

var VERSION = "v0.3.0"

func (m *VolumeManager) GetLonghornEventList() (*v1.EventList, error) {
	return m.ds.GetLonghornEventList()
}

type BundleMeta struct {
	LonghornVersion       string `json:"longhornVersion"`
	KubernetesVersion     string `json:"kubernetesVersion"`
	LonghornNamespaceUUID string `json:"longhornNamspaceUUID"`
	BundleCreatedAt       string `json:"bundleCreatedAt"`
	IssueURL              string `json:"issueURL"`
	IssueDescription      string `json:"issueDescription"`
}

// GenerateSupportBundle covers:
// 1. YAMLs of the Longhorn related CRDs
// 2. YAMLs of pods, services, daemonset, deployment in longhorn namespace
// 3. All the logs of pods in the longhorn namespace
// 4. Recent events happens in the longhorn namespace
//
// Directories are organized like this:
// root
// |- yamls
//   |- events.yaml
//   |- crds
//     |- <list of top level CRD objects>
//       |- <name of each crd>.yaml
//   |- pods
//     |- <name of each pod>.yaml
//   |- <other workloads>...
// |- logs
//   |- <name of each pod>.log
//   |- <directory by the name of pod, if multiple containers exists>
//     |- <container1>.log
//     |- ...
// The bundle would be compressed to a zip file for download.
func (m *VolumeManager) GenerateSupportBundle(issueURL string, description string) (*SupportBundle, error) {
	namespace, err := m.ds.GetLonghornNamespace()
	if err != nil {
		return nil, errors.Wrap(err, "cannot get longhorn namespace")
	}
	kubeVersion, err := m.ds.GetKubernetesVersion()
	if err != nil {
		return nil, errors.Wrap(err, "cannot get kubernetes version")
	}

	bundleMeta := &BundleMeta{
		LonghornVersion:       VERSION,
		KubernetesVersion:     kubeVersion.GitVersion,
		LonghornNamespaceUUID: string(namespace.UID),
		BundleCreatedAt:       util.Now(),
		IssueURL:              issueURL,
		IssueDescription:      description,
	}

	bundleName := "longhorn-support-bundle_" + bundleMeta.LonghornNamespaceUUID + "_" +
		strings.Replace(bundleMeta.BundleCreatedAt, ":", "-", -1)
	bundleFileName := bundleName + ".zip"

	go func() {
		bundleDir := filepath.Join("/tmp", bundleName)
		bundleFile := filepath.Join("/tmp", bundleFileName)
		if err := os.MkdirAll(bundleDir, os.FileMode(0755)); err != nil {
			m.sb.Error = BundleMkdirFailed
			m.sb.State = BundleStateError
			return
		}
		m.generateSupportBundle(bundleDir, bundleMeta)
		cmd := exec.Command("zip", "-r", bundleFileName, bundleName)
		cmd.Dir = "/tmp"
		if err := cmd.Run(); err != nil {
			m.sb.Error = BundleZipFailed
			m.sb.State = BundleStateError
			return
		}
		f, err := os.Stat(bundleFile)
		if err != nil {
			m.sb.Error = BundleStatFailed
			m.sb.State = BundleStateError
			return
		}

		m.sb.Size = f.Size()
		m.sb.createTime = time.Now()
		m.sb.State = BundleReadyForDownload
	}()

	return NewSupportBundle(BundleStateInProgress, bundleFileName), nil
}

func (m *VolumeManager) generateSupportBundle(bundleDir string, bundleMeta *BundleMeta) {
	errLog, err := os.Create(filepath.Join(bundleDir, "bundleGenerationError.log"))
	if err != nil {
		logrus.Errorf("Failed to create bundle generation log")
		return
	}
	defer errLog.Close()

	metaFile := filepath.Join(bundleDir, "metadata.yaml")
	encodeToYAMLFile(bundleMeta, metaFile, errLog)

	yamlsDir := filepath.Join(bundleDir, "yamls")
	m.generateSupportBundleYAMLs(yamlsDir, errLog)
	logsDir := filepath.Join(bundleDir, "logs")
	m.generateSupportBundleLogs(logsDir, errLog)
}

type GetObjectMapFunc func() (interface{}, error)
type GetRuntimeObjectListFunc func() (runtime.Object, error)

func (m *VolumeManager) generateSupportBundleYAMLs(yamlsDir string, errLog io.Writer) {
	kubernetesDir := filepath.Join(yamlsDir, "kubernetes")
	m.generateSupportBundleYAMLsForKubernetes(kubernetesDir, errLog)
	longhornDir := filepath.Join(yamlsDir, "longhorn")
	m.generateSupportBundleYAMLsForLonghorn(longhornDir, errLog)
}

func (m *VolumeManager) generateSupportBundleYAMLsForKubernetes(dir string, errLog io.Writer) {
	getListAndEncodeToYAML("events", m.ds.GetAllEventsList, dir, errLog)
	getListAndEncodeToYAML("pods", m.ds.GetAllPodsList, dir, errLog)
	getListAndEncodeToYAML("services", m.ds.GetAllServicesList, dir, errLog)
	getListAndEncodeToYAML("deployments", m.ds.GetAllDeploymentsList, dir, errLog)
	getListAndEncodeToYAML("daemonsets", m.ds.GetAllDaemonSetsList, dir, errLog)
	getListAndEncodeToYAML("statefulsets", m.ds.GetAllStatefulSetsList, dir, errLog)
	getListAndEncodeToYAML("jobs", m.ds.GetAllJobsList, dir, errLog)
	getListAndEncodeToYAML("cronjobs", m.ds.GetAllCronJobsList, dir, errLog)
	getListAndEncodeToYAML("nodes", m.ds.GetAllNodesList, dir, errLog)
}

func getListAndEncodeToYAML(name string, getListFunc GetRuntimeObjectListFunc, yamlsDir string, errLog io.Writer) {
	obj, err := getListFunc()
	if err != nil {
		fmt.Fprintf(errLog, "Support Bundle: failed to get %v: %v\n", name, err)
	}
	encodeToYAMLFile(obj, filepath.Join(yamlsDir, name+".yaml"), errLog)
}

func (m *VolumeManager) generateSupportBundleYAMLsForLonghorn(dir string, errLog io.Writer) {
	getObjectMapAndEncodeToYAML("settings", func() (interface{}, error) {
		return m.ds.ListSettings()
	}, dir, errLog)
	getObjectMapAndEncodeToYAML("engineimages", func() (interface{}, error) {
		return m.ds.ListEngineImages()
	}, dir, errLog)
	getObjectMapAndEncodeToYAML("nodes", func() (interface{}, error) {
		return m.ds.ListNodes()
	}, dir, errLog)
	getObjectMapAndEncodeToYAML("volumes", func() (interface{}, error) {
		return m.ds.ListVolumes()
	}, dir, errLog)
	getObjectMapAndEncodeToYAML("engines", func() (interface{}, error) {
		return m.ds.ListEngines()
	}, dir, errLog)
	getObjectMapAndEncodeToYAML("replicas", func() (interface{}, error) {
		return m.ds.ListReplicas()
	}, dir, errLog)
}

func getObjectMapAndEncodeToYAML(name string, getMapFunc GetObjectMapFunc, yamlsDir string, errLog io.Writer) {
	objMap, err := getMapFunc()
	if err != nil {
		fmt.Fprintf(errLog, "Support Bundle: failed to get %v: %v\n", name, err)
	}
	encodeMapToYAMLFile(objMap, filepath.Join(yamlsDir, name+".yaml"), errLog)
}

func encodeMapToYAMLFile(objMap interface{}, path string, errLog io.Writer) {
	objV := reflect.ValueOf(objMap)
	if objV.Kind() != reflect.Map {
		fmt.Fprintf(errLog, "Support Bundle: obj %v is not a map\n", objMap)
		return
	}
	keys := objV.MapKeys()
	list := make([]interface{}, objV.Len())
	for i := 0; i < objV.Len(); i++ {
		list[i] = objV.MapIndex(keys[i]).Interface()
	}
	encodeToYAMLFile(list, path, errLog)
}

func encodeToYAMLFile(obj interface{}, path string, errLog io.Writer) {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(errLog, "Support Bundle: failed to generate %v: %v\n", path, err)
		}
	}()
	err = os.MkdirAll(filepath.Dir(path), os.FileMode(0755))
	if err != nil {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	encoder := yaml.NewEncoder(f)
	if err = encoder.Encode(obj); err != nil {
		return
	}
	if err = encoder.Close(); err != nil {
		return
	}
}

func (m *VolumeManager) generateSupportBundleLogs(logsDir string, errLog io.Writer) {
	list, err := m.ds.GetAllPodsList()
	if err != nil {
		fmt.Fprintf(errLog, "Support bundle: cannot get pod list: %v\n", err)
		return
	}
	podList, ok := list.(*v1.PodList)
	if !ok {
		fmt.Fprintf(errLog, "BUG: Support bundle: didn't get pod list\n")
		return
	}
	for _, pod := range podList.Items {
		podName := pod.Name
		podDir := filepath.Join(logsDir, podName)
		for _, container := range pod.Spec.Containers {
			req := m.ds.GetPodContainerLogRequest(podName, container.Name)
			logFileName := filepath.Join(podDir, container.Name+".log")
			stream, err := req.Context(context.Background()).Stream()
			if err != nil {
				fmt.Fprintf(errLog, "BUG: Support bundle: cannot get log for pod %v container %v: %v\n",
					podName, container.Name, err)
				continue
			}
			streamLogToFile(stream, logFileName, errLog)
			stream.Close()
		}
	}
}

func streamLogToFile(logStream io.ReadCloser, path string, errLog io.Writer) {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(errLog, "Support Bundle: failed to generate %v: %v\n", path, err)
		}
	}()
	err = os.MkdirAll(filepath.Dir(path), os.FileMode(0755))
	if err != nil {
		return
	}
	f, err := os.Create(path)
	if err != nil {
		return
	}
	defer f.Close()
	_, err = io.Copy(f, logStream)
	if err != nil {
		return
	}
}

func (m *VolumeManager) InitSupportBundle(issueURL string, description string) (*SupportBundle, error) {
	if m.sb != nil {
		if m.sb.State == BundleStateInProgress {
			return nil, errors.Errorf("longhorn-manager is busy processing another support bundle request")
		}
		if m.isPreviousSupportBundleExpired() == false {
			return nil, errors.Errorf("current support bundle has not expired.")
		}
		// Clear the object and references as previous support bundle got expired
		m.DeleteSupportBundle()
	}

	sb, err := m.GenerateSupportBundle(issueURL, description)
	if err != nil {
		return nil, err
	}
	m.sb = sb
	return m.sb, nil
}
