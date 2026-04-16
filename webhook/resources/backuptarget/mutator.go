package backuptarget

import (
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"

	"github.com/cockroachdb/errors"

	"k8s.io/apimachinery/pkg/runtime"

	admissionregv1 "k8s.io/api/admissionregistration/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/webhook/admission"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	common "github.com/longhorn/longhorn-manager/webhook/common"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

type backupTargetMutator struct {
	admission.DefaultMutator
	ds *datastore.DataStore
}

func NewMutator(ds *datastore.DataStore) admission.Mutator {
	return &backupTargetMutator{ds: ds}
}

func (b *backupTargetMutator) Resource() admission.Resource {
	return admission.Resource{
		Name:       "backuptargets",
		Scope:      admissionregv1.NamespacedScope,
		APIGroup:   longhorn.SchemeGroupVersion.Group,
		APIVersion: longhorn.SchemeGroupVersion.Version,
		ObjectType: &longhorn.BackupTarget{},
		OperationTypes: []admissionregv1.OperationType{
			admissionregv1.Create,
			admissionregv1.Update,
		},
	}
}

func (b *backupTargetMutator) Create(request *admission.Request, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

func (b *backupTargetMutator) Update(request *admission.Request, oldObj runtime.Object, newObj runtime.Object) (admission.PatchOps, error) {
	return mutate(newObj)
}

// mutate contains functionality shared by Create and Update.
func mutate(newObj runtime.Object) (admission.PatchOps, error) {
	backupTarget, ok := newObj.(*longhorn.BackupTarget)
	if !ok {
		return nil, werror.NewInvalidError(fmt.Sprintf("%v is not a *longhorn.BackupTarget", newObj), "")
	}

	var patchOps admission.PatchOps

	patchOp, err := common.GetLonghornFinalizerPatchOpIfNeeded(backupTarget)
	if err != nil {
		err = errors.Wrapf(err, "failed to get finalizer patch for backupTarget %v", backupTarget.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if patchOp != "" {
		patchOps = append(patchOps, patchOp)
	}

	newURL, changed, err := applyNFSSoftMountDefaults(backupTarget.Spec.BackupTargetURL)
	if err != nil {
		err = errors.Wrapf(err, "failed to apply NFS soft-mount defaults for backupTarget %v", backupTarget.Name)
		return nil, werror.NewInvalidError(err.Error(), "")
	}
	if changed {
		marshaledURL, err := json.Marshal(newURL)
		if err != nil {
			err = errors.Wrapf(err, "failed to marshal backupTargetURL for backupTarget %v", backupTarget.Name)
			return nil, werror.NewInvalidError(err.Error(), "")
		}
		patchOps = append(patchOps, fmt.Sprintf(`{"op": "replace", "path": "/spec/backupTargetURL", "value": %s}`, string(marshaledURL)))
	}

	return patchOps, nil
}

// applyNFSSoftMountDefaults enforces safe NFS mount options when nfsOptions is
// explicitly set in an NFS BackupTarget URL.
//
// Returns the (possibly updated) URL, a boolean indicating whether the URL was
// changed, and any error.
//
// Rules:
//   - Non-NFS URLs (empty, s3://, cifs://, etc.) are returned unchanged.
//   - If nfsOptions is absent, the URL is returned unchanged. The underlying
//     NFS driver will use its built-in defaults (actimeo=1,soft,timeo=300,retry=2).
//   - If nfsOptions is present but blank, the parameter is stripped from the URL.
//   - If nfsOptions is present, the following are merged in to ensure the mount
//     can time out instead of blocking indefinitely:
//   - "hard" is removed.
//   - "soft" is added if absent.
//   - "timeo=<N>" is kept if already present; otherwise "timeo=300" is added.
//   - "retry=<N>" is kept if already present; otherwise "retry=2" is added.
func applyNFSSoftMountDefaults(rawURL string) (string, bool, error) {
	if rawURL == "" {
		return rawURL, false, nil
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		// Best-effort: leave the URL unchanged rather than blocking
		// unrelated admission operations (e.g. credentialSecret updates).
		return rawURL, false, nil
	}

	if u.Scheme != types.BackupStoreTypeNFS {
		return rawURL, false, nil
	}

	const NFSOptionsKey = "nfsOptions"

	q := u.Query()
	rawValues, exists := q[NFSOptionsKey]
	if !exists {
		return rawURL, false, nil
	}

	// Flatten both the repeated-key form (?nfsOptions=a&nfsOptions=b) and
	// the comma-separated form (?nfsOptions=a,b) into one option list.
	opts := splitNFSOptionsValues(rawValues)
	if len(opts) == 0 {
		// nfsOptions key is present but all values are blank -- strip it.
		q.Del(NFSOptionsKey)
		u.RawQuery = q.Encode()
		return u.String(), true, nil
	}

	merged := mergeNFSSoftMountOptions(opts)
	if slices.Equal(opts, merged) {
		return rawURL, false, nil
	}

	q.Set(NFSOptionsKey, strings.Join(merged, ","))
	u.RawQuery = q.Encode()
	return u.String(), true, nil
}

// splitNFSOptionsValues flattens repeated nfsOptions query values into one
// slice of individual mount options. It handles both the repeated-key form
// (?nfsOptions=a&nfsOptions=b) and the comma-separated form (?nfsOptions=a,b),
// and guarantees that no empty strings are returned.
func splitNFSOptionsValues(values []string) []string {
	result := make([]string, 0, len(values))
	for _, v := range values {
		for _, opt := range strings.Split(v, ",") {
			if opt != "" {
				result = append(result, opt)
			}
		}
	}
	return result
}

// mergeNFSSoftMountOptions enforces the soft-mount policy on a slice of NFS
// options and returns the updated slice.
func mergeNFSSoftMountOptions(opts []string) []string {
	hasSoft := false
	hasTimeo := false
	hasRetry := false

	var result []string
	for _, opt := range opts {
		if opt == "hard" {
			continue // always remove "hard"
		}
		if opt == "soft" {
			hasSoft = true
		}
		if strings.HasPrefix(opt, "timeo=") {
			hasTimeo = true
		}
		if strings.HasPrefix(opt, "retry=") {
			hasRetry = true
		}
		result = append(result, opt)
	}

	if !hasSoft {
		result = append(result, "soft")
	}
	if !hasTimeo {
		result = append(result, "timeo=300")
	}
	if !hasRetry {
		result = append(result, "retry=2")
	}
	return result
}
