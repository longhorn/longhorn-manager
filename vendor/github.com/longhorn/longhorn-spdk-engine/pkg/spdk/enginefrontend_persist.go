package spdk

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

const (
	engineFrontendSubDir  = "enginefrontends"
	engineFrontendRecFile = "enginefrontend.json"
)

// EngineFrontendRecord holds the minimal metadata needed to recover an
// EngineFrontend after an instance-manager restart. It is persisted to
// <metadataDir>/enginefrontends/<volumeName>/enginefrontend.json.
type EngineFrontendRecord struct {
	Name       string `json:"name"`
	EngineName string `json:"engineName"`
	VolumeName string `json:"volumeName"`
	Frontend   string `json:"frontend"`
	SpecSize   uint64 `json:"specSize"`
	TargetIP   string `json:"targetIP"`
	TargetPort int32  `json:"targetPort"`
}

// engineFrontendRecordDir returns the directory path for a volume's record.
func engineFrontendRecordDir(metadataDir, volumeName string) string {
	return filepath.Join(metadataDir, engineFrontendSubDir, volumeName)
}

// engineFrontendRecordPath returns the full file path for a volume's record.
func engineFrontendRecordPath(metadataDir, volumeName string) string {
	return filepath.Join(engineFrontendRecordDir(metadataDir, volumeName), engineFrontendRecFile)
}

// saveEngineFrontendRecord persists the engine frontend metadata to disk.
// It writes to a temporary file first and then renames for atomicity.
func saveEngineFrontendRecord(metadataDir string, ef *EngineFrontend) error {
	if metadataDir == "" {
		return nil
	}

	// UBLK frontends cannot be recovered after restart, so skip persistence.
	if types.IsUblkFrontend(ef.Frontend) {
		return nil
	}

	var targetIP string
	var targetPort int32
	if ef.NvmeTcpFrontend != nil {
		targetIP = ef.NvmeTcpFrontend.TargetIP
		targetPort = ef.NvmeTcpFrontend.TargetPort
	}

	record := &EngineFrontendRecord{
		Name:       ef.Name,
		EngineName: ef.EngineName,
		VolumeName: ef.VolumeName,
		Frontend:   ef.Frontend,
		SpecSize:   ef.SpecSize,
		TargetIP:   targetIP,
		TargetPort: targetPort,
	}

	dir := engineFrontendRecordDir(metadataDir, ef.VolumeName)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create engine frontend record directory %s: %w", dir, err)
	}

	data, err := json.MarshalIndent(record, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal engine frontend record for %s: %w", ef.Name, err)
	}

	targetPath := engineFrontendRecordPath(metadataDir, ef.VolumeName)
	tmpPath := targetPath + ".tmp"

	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write engine frontend record temp file %s: %w", tmpPath, err)
	}

	if err := os.Rename(tmpPath, targetPath); err != nil {
		// Best effort cleanup of temp file.
		if errRemove := os.Remove(tmpPath); errRemove != nil {
			logrus.WithError(errRemove).Warnf("Failed to remove engine frontend record temp file %s", tmpPath)
		}
		return fmt.Errorf("failed to rename engine frontend record %s -> %s: %w", tmpPath, targetPath, err)
	}

	return nil
}

// removeEngineFrontendRecord removes the persisted engine frontend record
// for the given volume name.
func removeEngineFrontendRecord(metadataDir, volumeName string) error {
	if metadataDir == "" {
		return nil
	}

	dir := engineFrontendRecordDir(metadataDir, volumeName)
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("failed to remove engine frontend record directory %s: %w", dir, err)
	}

	return nil
}

// loadEngineFrontendRecords scans the engine frontend records directory
// and returns all valid records. Invalid or corrupted records are logged
// and skipped.
func loadEngineFrontendRecords(metadataDir string) ([]*EngineFrontendRecord, error) {
	if metadataDir == "" {
		return nil, nil
	}

	baseDir := filepath.Join(metadataDir, engineFrontendSubDir)

	var entries []os.DirEntry
	var readErr error
	for attempt := 0; attempt < 3; attempt++ {
		entries, readErr = os.ReadDir(baseDir)
		if readErr == nil {
			break
		}
		if os.IsNotExist(readErr) {
			return nil, nil
		}
		logrus.WithError(readErr).Warnf("Failed to read engine frontend records directory %s (attempt %d/3)", baseDir, attempt+1)
		time.Sleep(500 * time.Millisecond)
	}
	if readErr != nil {
		return nil, fmt.Errorf("failed to read engine frontend records directory %s after retries: %w", baseDir, readErr)
	}

	var records []*EngineFrontendRecord
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		volumeName := entry.Name()
		recordPath := engineFrontendRecordPath(metadataDir, volumeName)

		data, err := os.ReadFile(recordPath)
		if err != nil {
			if os.IsNotExist(err) {
				logrus.Warnf("Engine frontend record directory %s exists but has no %s, skipping", volumeName, engineFrontendRecFile)
			} else {
				logrus.WithError(err).Warnf("Failed to read engine frontend record %s, skipping", recordPath)
			}
			continue
		}

		record := &EngineFrontendRecord{}
		if err := json.Unmarshal(data, record); err != nil {
			logrus.WithError(err).Warnf("Failed to parse engine frontend record %s, removing corrupted record", recordPath)
			if removeErr := os.RemoveAll(filepath.Join(baseDir, volumeName)); removeErr != nil {
				logrus.WithError(removeErr).Warnf("Failed to remove corrupted engine frontend record directory %s", volumeName)
			}
			continue
		}

		if record.Name == "" || record.VolumeName == "" {
			logrus.Warnf("Engine frontend record %s has empty name or volume name, removing invalid record", recordPath)
			if removeErr := os.RemoveAll(filepath.Join(baseDir, volumeName)); removeErr != nil {
				logrus.WithError(removeErr).Warnf("Failed to remove invalid engine frontend record directory %s", volumeName)
			}
			continue
		}

		records = append(records, record)
	}

	return records, nil
}
