package types

import (
	"fmt"
	"strings"

	lhbackup "github.com/longhorn/go-common-libs/backup"
)

// backupParameterLabelPrefix is the reserved DNS-qualified prefix used to
// smuggle well-known backup parameters through the v2 (SPDK) gRPC Labels
// wire field. Namespacing under "longhorn.io/" avoids colliding with a
// plain user label that happens to share a parameter's bare name (e.g. a
// caller tagging their backup with "backup-mode=full" for their own
// bookkeeping) — such a label is left untouched because it lacks this
// prefix. It does not protect against a caller deliberately setting a
// label using the fully-qualified reserved key itself.
const backupParameterLabelPrefix = "longhorn.io/backup-parameter."

// wellKnownBackupParameterKeys enumerates the backup parameter keys that are
// forwarded from callers (longhorn-manager via longhorn-instance-manager) into
// backupstore.DeltaBackupConfig.Parameters for v2 (SPDK) backups.
//
// The v2 gRPC BackupCreateRequest (spdkrpc.BackupCreateRequest) has no
// dedicated field for backup parameters, so these keys are smuggled through
// the existing Labels field under backupParameterLabelPrefix. See
// EncodeBackupParametersIntoLabels / ExtractBackupParametersFromLabels for
// the paired encoder/decoder. Add new keys here to have them flow end-to-end
// without touching either side individually.
//
// Kept unexported so external packages cannot mutate the recognised-key set
// at runtime; go through the encoder/decoder helpers instead.
var wellKnownBackupParameterKeys = []string{
	lhbackup.LonghornBackupParameterBackupMode,
	lhbackup.LonghornBackupParameterBackupBlockSize,
}

func isWellKnownBackupParameterKey(key string) bool {
	for _, k := range wellKnownBackupParameterKeys {
		if k == key {
			return true
		}
	}
	return false
}

// EncodeBackupParametersIntoLabels appends reserved
// "<backupParameterLabelPrefix><key>=<value>" entries to labels for every
// well-known parameter present in parameters. It returns labels unchanged if
// parameters is nil or contains no well-known key. Non-well-known parameter
// keys are ignored so the wire contract stays explicit.
func EncodeBackupParametersIntoLabels(labels []string, parameters map[string]string) []string {
	for _, key := range wellKnownBackupParameterKeys {
		if value, ok := parameters[key]; ok {
			labels = append(labels, fmt.Sprintf("%s%s=%s", backupParameterLabelPrefix, key, value))
		}
	}
	return labels
}

// ExtractBackupParametersFromLabels removes every reserved
// backupParameterLabelPrefix entry with a well-known suffix from labelMap
// and returns them as a plain parameters map keyed by the parameter name
// (prefix stripped). Entries outside the reserved namespace, or reserved
// entries with unrecognised suffixes, are left untouched so callers'
// ordinary labels are preserved verbatim. Returns an empty (non-nil) map
// if labelMap contains no matching entry.
func ExtractBackupParametersFromLabels(labelMap map[string]string) map[string]string {
	parameters := map[string]string{}
	for k, v := range labelMap {
		key, ok := strings.CutPrefix(k, backupParameterLabelPrefix)
		if !ok || !isWellKnownBackupParameterKey(key) {
			continue
		}
		parameters[key] = v
		delete(labelMap, k)
	}
	return parameters
}
