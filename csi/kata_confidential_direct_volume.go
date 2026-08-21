package csi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilexec "k8s.io/utils/exec"

	"github.com/longhorn/longhorn-manager/types"

	longhornclient "github.com/longhorn/longhorn-manager/client"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	kataConfidentialDirectVolumeParameter   = "kataConfidentialDirectVolume"
	kataConfidentialStorageKeyURIAnnotation = "io.katacontainers.storage/confidential-key-uri"
	kataConfidentialStorageFSType           = "confidential-storage"
	kataConfidentialStorageProfile          = "luks2-integrity-ext4"
	kataConfidentialStorageVolumeIDMaxBytes = 256
	kataConfidentialStorageKeyURIMaxBytes   = 2048

	csiPVCNameKey      = "csi.storage.k8s.io/pvc/name"
	csiPVCNamespaceKey = "csi.storage.k8s.io/pvc/namespace"

	kataConfidentialDirectVolumeStateDir = "/var/lib/longhorn/kata-confidential-direct-volumes"
	kataCtlPath                          = "/opt/kata/bin/kata-ctl"
	nsMounterPath                        = "/usr/local/sbin/nsmounter"

	kataConfidentialDirectVolumeResizeUnsupported = "Kata confidential direct volumes do not support resize"
)

type kataConfidentialDirectVolumeMountInfo struct {
	VolumeType          string                           `json:"volume-type"`
	Device              string                           `json:"device"`
	FsType              string                           `json:"fstype"`
	ConfidentialStorage *kataConfidentialStorageContract `json:"confidential-storage,omitempty"`
}

type kataConfidentialStorageContract struct {
	Profile  string `json:"profile"`
	VolumeID string `json:"volume-id"`
	KeyURI   string `json:"key-uri"`
}

type kataConfidentialDirectVolumeState struct {
	VolumeID          string   `json:"volumeID"`
	StagingTargetPath string   `json:"stagingTargetPath"`
	DevicePath        string   `json:"devicePath"`
	PublishedPaths    []string `json:"publishedPaths,omitempty"`
}

type kataDirectVolumeRuntime interface {
	Add(ctx context.Context, targetPath string, mountInfo kataConfidentialDirectVolumeMountInfo) error
	Remove(ctx context.Context, targetPath string) error
	Stats(ctx context.Context, targetPath string) ([]byte, error)
}

type kataConfidentialDirectVolumeOperations interface {
	IsManaged(volumeID string) (bool, error)
	Stage(volumeID, stagingTargetPath, devicePath string) error
	Publish(ctx context.Context, volumeID, targetPath string, mountInfo kataConfidentialDirectVolumeMountInfo) error
	Unpublish(ctx context.Context, volumeID, targetPath string) error
	Unstage(ctx context.Context, volumeID string) error
	Stats(ctx context.Context, volumeID, targetPath string) (*csi.NodeGetVolumeStatsResponse, error)
}

type hostKataCtl struct {
	run func(ctx context.Context, command string, args ...string) ([]byte, error)
}

func (r *hostKataCtl) command(ctx context.Context, args ...string) ([]byte, error) {
	commandArgs := []string{"--host-root", kataCtlPath, "direct-volume"}
	commandArgs = append(commandArgs, args...)
	run := r.run
	if run == nil {
		run = func(ctx context.Context, command string, args ...string) ([]byte, error) {
			return utilexec.New().CommandContext(ctx, command, args...).CombinedOutput()
		}
	}
	output, err := run(ctx, nsMounterPath, commandArgs...)
	if err != nil {
		action := "unknown"
		if len(args) != 0 {
			action = args[0]
		}
		return nil, fmt.Errorf("host Kata direct-volume %s failed: %w; output: %s", action, err, boundedKataCommandOutput(output))
	}
	return output, nil
}

func boundedKataCommandOutput(output []byte) string {
	const maxBytes = 8192
	truncated := len(output) > maxBytes
	if truncated {
		output = output[:maxBytes]
	}
	message := strings.Map(func(character rune) rune {
		if character == '\n' || character == '\t' || character >= ' ' {
			return character
		}
		return '�'
	}, strings.TrimSpace(string(output)))
	if truncated {
		message += " [truncated]"
	}
	return message
}

func (r *hostKataCtl) Add(ctx context.Context, targetPath string, mountInfo kataConfidentialDirectVolumeMountInfo) error {
	encoded, err := json.Marshal(mountInfo)
	if err != nil {
		return fmt.Errorf("failed to encode Kata direct-volume mount metadata: %w", err)
	}
	_, err = r.command(ctx, "add", "--volume-path", targetPath, "--mount-info", string(encoded))
	return err
}

func (r *hostKataCtl) Remove(ctx context.Context, targetPath string) error {
	_, err := r.command(ctx, "remove", "--volume-path", targetPath)
	return err
}

func (r *hostKataCtl) Stats(ctx context.Context, targetPath string) ([]byte, error) {
	return r.command(ctx, "stats", "--volume-path", targetPath)
}

type kataConfidentialDirectVolumeManager struct {
	mu       sync.Mutex
	stateDir string
	runtime  kataDirectVolumeRuntime
}

func newKataConfidentialDirectVolumeManager() *kataConfidentialDirectVolumeManager {
	return &kataConfidentialDirectVolumeManager{
		stateDir: kataConfidentialDirectVolumeStateDir,
		runtime:  &hostKataCtl{},
	}
}

func (m *kataConfidentialDirectVolumeManager) statePath(volumeID string) string {
	encoded := base64.RawURLEncoding.EncodeToString([]byte(volumeID))
	return filepath.Join(m.stateDir, encoded+".json")
}

func (m *kataConfidentialDirectVolumeManager) loadLocked(volumeID string) (*kataConfidentialDirectVolumeState, error) {
	data, err := os.ReadFile(m.statePath(volumeID))
	if err != nil {
		return nil, err
	}
	state := &kataConfidentialDirectVolumeState{}
	if err := json.Unmarshal(data, state); err != nil {
		return nil, fmt.Errorf("invalid confidential direct-volume lifecycle metadata: %w", err)
	}
	if state.VolumeID != volumeID || state.StagingTargetPath == "" || state.DevicePath == "" {
		return nil, fmt.Errorf("invalid confidential direct-volume lifecycle metadata for volume %q", volumeID)
	}
	return state, nil
}

func (m *kataConfidentialDirectVolumeManager) saveLocked(state *kataConfidentialDirectVolumeState) (err error) {
	if err := os.MkdirAll(m.stateDir, 0700); err != nil {
		return fmt.Errorf("failed to create confidential direct-volume state directory: %w", err)
	}
	if err := os.Chmod(m.stateDir, 0700); err != nil {
		return fmt.Errorf("failed to protect confidential direct-volume state directory: %w", err)
	}
	data, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("failed to encode confidential direct-volume lifecycle metadata: %w", err)
	}
	tmp, err := os.CreateTemp(m.stateDir, ".kata-confidential-direct-volume-*")
	if err != nil {
		return fmt.Errorf("failed to create confidential direct-volume state file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() {
		if cleanupErr := os.Remove(tmpPath); cleanupErr != nil && !os.IsNotExist(cleanupErr) && err == nil {
			err = fmt.Errorf("failed to remove temporary confidential direct-volume state file: %w", cleanupErr)
		}
	}()
	if err := tmp.Chmod(0600); err != nil {
		return closeTemporaryStateFile(tmp, "failed to protect confidential direct-volume state file", err)
	}
	if _, err := tmp.Write(data); err != nil {
		return closeTemporaryStateFile(tmp, "failed to write confidential direct-volume lifecycle metadata", err)
	}
	if err := tmp.Sync(); err != nil {
		return closeTemporaryStateFile(tmp, "failed to sync confidential direct-volume lifecycle metadata", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to close confidential direct-volume state file: %w", err)
	}
	if err := os.Rename(tmpPath, m.statePath(state.VolumeID)); err != nil {
		return fmt.Errorf("failed to persist confidential direct-volume lifecycle metadata: %w", err)
	}
	dir, err := os.Open(m.stateDir)
	if err != nil {
		return fmt.Errorf("failed to open confidential direct-volume state directory: %w", err)
	}
	if err := dir.Sync(); err != nil {
		if closeErr := dir.Close(); closeErr != nil {
			err = errors.Join(err, fmt.Errorf("failed to close confidential direct-volume state directory: %w", closeErr))
		}
		return fmt.Errorf("failed to sync confidential direct-volume state directory: %w", err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("failed to close confidential direct-volume state directory: %w", err)
	}
	return nil
}

func closeTemporaryStateFile(file *os.File, operation string, operationErr error) error {
	if closeErr := file.Close(); closeErr != nil {
		operationErr = errors.Join(operationErr, fmt.Errorf("failed to close confidential direct-volume state file: %w", closeErr))
	}
	return fmt.Errorf("%s: %w", operation, operationErr)
}

func (m *kataConfidentialDirectVolumeManager) IsManaged(volumeID string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, err := m.loadLocked(volumeID)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (m *kataConfidentialDirectVolumeManager) Stage(volumeID, stagingTargetPath, devicePath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, err := m.loadLocked(volumeID)
	if err == nil {
		if state.StagingTargetPath != stagingTargetPath || state.DevicePath != devicePath {
			return fmt.Errorf("confidential direct volume %q is already staged with different non-secret metadata", volumeID)
		}
		return nil
	}
	if !os.IsNotExist(err) {
		return err
	}
	return m.saveLocked(&kataConfidentialDirectVolumeState{
		VolumeID:          volumeID,
		StagingTargetPath: stagingTargetPath,
		DevicePath:        devicePath,
	})
}

func (m *kataConfidentialDirectVolumeManager) Publish(ctx context.Context, volumeID, targetPath string, mountInfo kataConfidentialDirectVolumeMountInfo) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, err := m.loadLocked(volumeID)
	if err != nil {
		return err
	}
	if state.DevicePath != mountInfo.Device {
		return fmt.Errorf("confidential direct volume %q device changed between stage and publish", volumeID)
	}
	knownTarget := false
	for _, publishedPath := range state.PublishedPaths {
		if publishedPath == targetPath {
			knownTarget = true
			break
		}
	}
	if !knownTarget {
		state.PublishedPaths = append(state.PublishedPaths, targetPath)
		sort.Strings(state.PublishedPaths)
		// Persist the cleanup intent before invoking Kata. If registration fails,
		// Unpublish/Unstage can still remove any partial runtime state.
		if err := m.saveLocked(state); err != nil {
			return err
		}
	}
	return m.runtime.Add(ctx, targetPath, mountInfo)
}

func (m *kataConfidentialDirectVolumeManager) Unpublish(ctx context.Context, volumeID, targetPath string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, err := m.loadLocked(volumeID)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	knownTarget := false
	for _, publishedPath := range state.PublishedPaths {
		if publishedPath == targetPath {
			knownTarget = true
			break
		}
	}
	if !knownTarget {
		return nil
	}
	if err := m.runtime.Remove(ctx, targetPath); err != nil {
		return err
	}
	paths := state.PublishedPaths[:0]
	for _, publishedPath := range state.PublishedPaths {
		if publishedPath != targetPath {
			paths = append(paths, publishedPath)
		}
	}
	state.PublishedPaths = paths
	return m.saveLocked(state)
}

func (m *kataConfidentialDirectVolumeManager) Unstage(ctx context.Context, volumeID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	state, err := m.loadLocked(volumeID)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, targetPath := range state.PublishedPaths {
		if err := m.runtime.Remove(ctx, targetPath); err != nil {
			return err
		}
	}
	if err := os.Remove(m.statePath(volumeID)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove confidential direct-volume lifecycle metadata: %w", err)
	}
	return nil
}

func (m *kataConfidentialDirectVolumeManager) Stats(ctx context.Context, volumeID, targetPath string) (*csi.NodeGetVolumeStatsResponse, error) {
	m.mu.Lock()
	state, err := m.loadLocked(volumeID)
	m.mu.Unlock()
	if err != nil {
		return nil, err
	}
	knownTarget := false
	for _, publishedPath := range state.PublishedPaths {
		if publishedPath == targetPath {
			knownTarget = true
			break
		}
	}
	if !knownTarget {
		return nil, fmt.Errorf("confidential direct volume %q is not published at the requested path", volumeID)
	}
	data, err := m.runtime.Stats(ctx, targetPath)
	if err != nil {
		return nil, err
	}
	var stats struct {
		Usage []struct {
			Available uint64 `json:"available"`
			Total     uint64 `json:"total"`
			Used      uint64 `json:"used"`
			Unit      int32  `json:"unit"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(data, &stats); err != nil {
		return nil, fmt.Errorf("failed to decode redacted Kata direct-volume statistics: %w", err)
	}
	response := &csi.NodeGetVolumeStatsResponse{}
	for _, usage := range stats.Usage {
		if usage.Available > math.MaxInt64 || usage.Total > math.MaxInt64 || usage.Used > math.MaxInt64 {
			return nil, fmt.Errorf("kata direct-volume statistics exceed CSI signed integer range")
		}
		unit := csi.VolumeUsage_Unit(usage.Unit)
		if unit != csi.VolumeUsage_BYTES && unit != csi.VolumeUsage_INODES {
			return nil, fmt.Errorf("kata direct-volume statistics contain unsupported unit %d", usage.Unit)
		}
		response.Usage = append(response.Usage, &csi.VolumeUsage{
			Available: int64(usage.Available),
			Total:     int64(usage.Total),
			Used:      int64(usage.Used),
			Unit:      unit,
		})
	}
	return response, nil
}

func kataConfidentialDirectVolumeRequested(volumeContext map[string]string) (bool, error) {
	value, exists := volumeContext[kataConfidentialDirectVolumeParameter]
	if !exists {
		return false, nil
	}
	if value != "true" {
		return false, status.Errorf(codes.InvalidArgument, "%s must be exactly true when present", kataConfidentialDirectVolumeParameter)
	}
	return true, nil
}

func (ns *NodeServer) kataConfidentialDirectVolumeMode(volumeID string, volumeContext map[string]string) (bool, error) {
	requested, err := kataConfidentialDirectVolumeRequested(volumeContext)
	if err != nil {
		return false, err
	}
	if requested {
		return true, nil
	}
	managed, err := ns.directVolumes.IsManaged(volumeID)
	if err != nil {
		return false, status.Errorf(codes.Internal, "failed to read confidential direct-volume lifecycle metadata: %v", err)
	}
	if managed {
		return false, status.Error(codes.FailedPrecondition, "confidential direct-volume marker is missing from a managed volume")
	}
	return false, nil
}

func validateKataConfidentialDirectVolume(volume *longhornclient.Volume, capability *csi.VolumeCapability) (string, error) {
	if volume == nil {
		return "", status.Error(codes.NotFound, "confidential direct volume is missing")
	}
	if !canonicalKataConfidentialVolumeID(volume.Name) {
		return "", status.Error(codes.InvalidArgument, "confidential direct volume has an invalid volume ID")
	}
	if capability.GetMount() == nil {
		return "", status.Error(codes.InvalidArgument, "confidential direct volumes require filesystem volume mode")
	}
	if capability.GetAccessMode() == nil || capability.GetAccessMode().GetMode() != csi.VolumeCapability_AccessMode_SINGLE_NODE_SINGLE_WRITER {
		return "", status.Error(codes.InvalidArgument, "confidential direct volumes require ReadWriteOncePod access")
	}
	if capability.GetMount().GetFsType() != defaultFsType {
		return "", status.Errorf(codes.InvalidArgument, "confidential direct volumes require %s", defaultFsType)
	}
	if len(capability.GetMount().GetMountFlags()) != 0 {
		return "", status.Error(codes.InvalidArgument, "confidential direct volumes do not accept host mount flags")
	}
	if volume.AccessMode != string(longhorn.AccessModeReadWriteOncePod) {
		return "", status.Error(codes.InvalidArgument, "Longhorn volume is not ReadWriteOncePod")
	}
	if volume.Encrypted {
		return "", status.Error(codes.InvalidArgument, "Longhorn host encryption is forbidden for confidential direct volumes")
	}
	if volume.Migratable {
		return "", status.Error(codes.InvalidArgument, "migration is forbidden for confidential direct volumes")
	}
	if volume.NumberOfReplicas != 1 {
		return "", status.Error(codes.InvalidArgument, "confidential direct volumes require exactly one Longhorn replica")
	}
	if !types.IsDataEngineV1(longhorn.DataEngineType(volume.DataEngine)) {
		return "", status.Error(codes.InvalidArgument, "confidential direct volumes require Longhorn data engine v1")
	}
	if volume.DisableFrontend || volume.Frontend != string(longhorn.VolumeFrontendBlockDev) {
		return "", status.Error(codes.InvalidArgument, "confidential direct volumes require the raw block-device frontend")
	}
	if len(volume.Controllers) != 1 {
		return "", status.Errorf(codes.InvalidArgument, "confidential direct volume has invalid controller count %d", len(volume.Controllers))
	}
	if volume.State != string(longhorn.VolumeStateAttached) || !volume.Ready {
		return "", status.Error(codes.FailedPrecondition, "confidential direct volume is not attached and ready")
	}
	devicePath := volume.Controllers[0].Endpoint
	if !filepath.IsAbs(devicePath) || filepath.Clean(devicePath) != devicePath || devicePath == "/" {
		return "", status.Error(codes.InvalidArgument, "confidential direct volume has an invalid raw endpoint")
	}
	if devicePath != filepath.Join("/dev/longhorn", volume.Name) {
		return "", status.Error(codes.InvalidArgument, "confidential direct volume endpoint is not the expected Longhorn raw device")
	}
	return devicePath, nil
}

func canonicalKataConfidentialVolumeID(value string) bool {
	if value == "" || len(value) > kataConfidentialStorageVolumeIDMaxBytes {
		return false
	}
	for _, component := range strings.Split(value, "/") {
		if component == "" || component == "." || component == ".." {
			return false
		}
	}
	return strings.IndexFunc(value, func(character rune) bool {
		return !(character >= 'a' && character <= 'z' ||
			character >= 'A' && character <= 'Z' ||
			character >= '0' && character <= '9' ||
			strings.ContainsRune("-_.:/@", character))
	}) == -1
}

func canonicalKataConfidentialKeyURI(value string) bool {
	return len(value) > len("kbs:///") &&
		len(value) <= kataConfidentialStorageKeyURIMaxBytes &&
		strings.HasPrefix(value, "kbs:///") &&
		strings.IndexFunc(value, func(character rune) bool {
			return character <= ' ' || character > '~'
		}) == -1
}

func (ns *NodeServer) kataConfidentialStorageKeyURI(ctx context.Context, volumeContext map[string]string) (string, error) {
	pvcName := volumeContext[csiPVCNameKey]
	pvcNamespace := volumeContext[csiPVCNamespaceKey]
	if pvcName == "" || pvcNamespace == "" {
		return "", status.Error(codes.InvalidArgument, "confidential direct volume is missing external-provisioner PVC metadata")
	}
	claim, err := ns.kubeClient.CoreV1().PersistentVolumeClaims(pvcNamespace).Get(ctx, pvcName, metav1.GetOptions{})
	if err != nil {
		return "", status.Errorf(codes.Internal, "failed to read confidential direct-volume PVC metadata: %v", err)
	}
	if len(claim.Spec.AccessModes) != 1 || claim.Spec.AccessModes[0] != corev1.ReadWriteOncePod {
		return "", status.Error(codes.InvalidArgument, "confidential direct-volume PVC must use only ReadWriteOncePod")
	}
	if claim.Spec.VolumeMode == nil || *claim.Spec.VolumeMode != corev1.PersistentVolumeFilesystem {
		return "", status.Error(codes.InvalidArgument, "confidential direct-volume PVC must explicitly use Filesystem volume mode")
	}
	keyURI := claim.Annotations[kataConfidentialStorageKeyURIAnnotation]
	if !canonicalKataConfidentialKeyURI(keyURI) {
		return "", status.Error(codes.InvalidArgument, "confidential storage key URI must be bounded canonical kbs metadata")
	}
	return keyURI, nil
}

func (ns *NodeServer) nodeStageKataConfidentialDirectVolume(ctx context.Context, req *csi.NodeStageVolumeRequest, volume *longhornclient.Volume) (*csi.NodeStageVolumeResponse, error) {
	if len(req.GetSecrets()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "confidential direct volumes reject CSI node secrets")
	}
	devicePath, err := validateKataConfidentialDirectVolume(volume, req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}
	if !cleanAbsolutePath(req.GetStagingTargetPath()) {
		return nil, status.Error(codes.InvalidArgument, "confidential direct volume has an invalid staging target path")
	}
	if _, err := ns.kataConfidentialStorageKeyURI(ctx, req.GetVolumeContext()); err != nil {
		return nil, err
	}
	if err := ns.directVolumes.Stage(req.GetVolumeId(), req.GetStagingTargetPath(), devicePath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to persist confidential direct-volume lifecycle metadata: %v", err)
	}
	return &csi.NodeStageVolumeResponse{}, nil
}

func (ns *NodeServer) nodePublishKataConfidentialDirectVolume(ctx context.Context, req *csi.NodePublishVolumeRequest, volume *longhornclient.Volume) (*csi.NodePublishVolumeResponse, error) {
	if req.GetReadonly() {
		return nil, status.Error(codes.InvalidArgument, "confidential direct volumes do not support read-only publication")
	}
	if len(req.GetSecrets()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "confidential direct volumes reject CSI node secrets")
	}
	devicePath, err := validateKataConfidentialDirectVolume(volume, req.GetVolumeCapability())
	if err != nil {
		return nil, err
	}
	if !cleanAbsolutePath(req.GetStagingTargetPath()) || !cleanAbsolutePath(req.GetTargetPath()) {
		return nil, status.Error(codes.InvalidArgument, "confidential direct volume has an invalid publish path")
	}
	keyURI, err := ns.kataConfidentialStorageKeyURI(ctx, req.GetVolumeContext())
	if err != nil {
		return nil, err
	}
	// Kubelet can call Publish without a fresh Stage after plugin restart. Stage
	// is metadata-only for this mode, so repairing the lifecycle record here is
	// safe and does not touch the raw device.
	if err := ns.directVolumes.Stage(req.GetVolumeId(), req.GetStagingTargetPath(), devicePath); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to persist confidential direct-volume lifecycle metadata: %v", err)
	}
	mountInfo := kataConfidentialDirectVolumeMountInfo{
		VolumeType: "directvol",
		Device:     devicePath,
		FsType:     kataConfidentialStorageFSType,
		ConfidentialStorage: &kataConfidentialStorageContract{
			Profile:  kataConfidentialStorageProfile,
			VolumeID: volume.Name,
			KeyURI:   keyURI,
		},
	}
	if err := ns.directVolumes.Publish(ctx, req.GetVolumeId(), req.GetTargetPath(), mountInfo); err != nil {
		return nil, status.Errorf(codes.Internal, "failed to register confidential direct volume with Kata: %v", err)
	}
	return &csi.NodePublishVolumeResponse{}, nil
}

func cleanAbsolutePath(path string) bool {
	return filepath.IsAbs(path) && filepath.Clean(path) == path && path != "/"
}
