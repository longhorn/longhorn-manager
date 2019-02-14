package manager

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/Sirupsen/logrus"
	"gopkg.in/yaml.v2"

	"k8s.io/api/core/v1"
)

func (m *VolumeManager) ListEvent() ([]*v1.Event, error) {
	eventList, err := m.ds.ListEvents()
	if err != nil {
		return nil, err
	}
	return eventList, nil
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
func (m *VolumeManager) GenerateSupportBundle() (io.ReadCloser, int64, error) {
	bundleDir := "longhorn-support-bundle"
	bundleFullPath := filepath.Join("/tmp", bundleDir)
	err := os.MkdirAll(bundleFullPath, os.FileMode(0755))
	if err != nil {
		return nil, 0, err
	}
	bundleFile := "/tmp/longhorn-support-bundle.zip"

	m.generateSupportBundle(bundleFullPath)

	cmd := exec.Command("zip", "-r", bundleFile, bundleDir)
	cmd.Dir = "/tmp"
	if err := cmd.Run(); err != nil {
		return nil, 0, err
	}
	f, err := os.Open(bundleFile)
	if err != nil {
		return nil, 0, err
	}

	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	size := info.Size()
	return f, size, nil
}

func (m *VolumeManager) generateSupportBundle(bundleDir string) {
	errLogFile, err := os.Create(filepath.Join(bundleDir, "bundleGenerationError.log"))
	if err != nil {
		logrus.Errorf("Failed to create bundle generation log")
		return
	}
	defer errLogFile.Close()

	m.generateSupportBundleYAMLs(bundleDir, errLogFile)
}

func (m *VolumeManager) generateSupportBundleYAMLs(bundleRootDir string, errLog io.Writer) {
	yamlsDir := filepath.Join(bundleRootDir, "yamls")

	events, err := m.ds.ListEvents()
	if err != nil {
		fmt.Fprintf(errLog, "Support Bundle: failed to get events: %v", err)
	}
	encodeToYAMLFile(events, filepath.Join(yamlsDir, "events.yaml"), errLog)
}

func encodeToYAMLFile(obj interface{}, path string, errLog io.Writer) {
	var err error
	defer func() {
		if err != nil {
			fmt.Fprintf(errLog, "Support Bundle: failed to generate %v: %v", path, err)
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
