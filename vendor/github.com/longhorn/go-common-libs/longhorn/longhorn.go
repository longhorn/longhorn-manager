package longhorn

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/errors"
)

// GetVolumeNameFromReplicaDataDirectoryName extracts the volume name from the replica
// data directory name.
// The replica data directory name format is expected to follow "<volume name>-<8 character hash>".
// Note: The replica data directory name is not the same as the Kubernetes Replica
// custom resource (CR) object name.
func GetVolumeNameFromReplicaDataDirectoryName(replicaName string) (string, error) {
	parts := strings.Split(replicaName, "-")
	if len(parts) > 1 && len(parts[len(parts)-1]) == 8 {
		return strings.Join(parts[:len(parts)-1], "-"), nil
	}

	return "", errors.Errorf("failed to get volume name from replica data directory name %s", replicaName)
}

// replicaCRNameRegex matches the Kubernetes Replica custom resource object name
// format "<volume name>-r-<8 character random ID>", as built by longhorn-manager's
// GenerateReplicaNameForVolume. It is anchored at the end so a volume whose own name
// contains "-r-<8 characters>" keeps that part, only the trailing replica suffix is
// removed.
var replicaCRNameRegex = regexp.MustCompile(`^(.+)-r-[A-Za-z0-9]{8}$`)

// GetVolumeNameFromReplicaCRName extracts the volume name from the Kubernetes Replica
// custom resource (CR) object name.
// The replica CR name format is expected to follow "<volume name>-r-<8 character random ID>".
// Note: this is not the same as the replica data directory name, which has no "-r"
// infix. Use GetVolumeNameFromReplicaDataDirectoryName for the latter.
// An error is returned when the name does not follow the convention, so callers do not
// silently act on a wrong volume name.
func GetVolumeNameFromReplicaCRName(replicaCRName string) (string, error) {
	match := replicaCRNameRegex.FindStringSubmatch(replicaCRName)
	if match == nil {
		return "", errors.Errorf("failed to get volume name from replica CR name %s", replicaCRName)
	}

	return match[1], nil
}

// IsEngineProcess distinguish if the process is a engine process by its name.
func IsEngineProcess(processName string) bool {
	// engine process name example: pvc-5a8ee916-5989-46c6-bafc-ddbf7c802499-e-0
	return regexp.MustCompile(`.+?-e-[^-]*$`).MatchString(processName)
}
