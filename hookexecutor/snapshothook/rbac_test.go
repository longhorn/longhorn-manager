package snapshothook

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authorizationv1 "k8s.io/api/authorization/v1"

	"github.com/longhorn/longhorn-manager/hookexecutor"
	"github.com/longhorn/longhorn-manager/hookexecutor/podexec"
)

func TestExecutorGrants(t *testing.T) {
	grants := ExecutorGrants("app")
	base := podexec.Grants("app")
	require.Len(t, grants, len(base)+1)
	assert.Equal(t, base, grants[:len(base)])
	assert.Equal(t, authorizationv1.ResourceAttributes{
		Namespace: "app", Group: "longhorn.io", Verb: "get", Resource: "volumehookpolicies",
	}, grants[len(base)])
}

func TestVerifyExecutorAccess(t *testing.T) {
	t.Run("every grant held is no denial", func(t *testing.T) {
		var reviewed []authorizationv1.ResourceAttributes
		denial, err := VerifyExecutorAccess("app", "longhorn-system", func(attributes authorizationv1.ResourceAttributes) (bool, error) {
			reviewed = append(reviewed, attributes)
			return true, nil
		})
		require.NoError(t, err)
		assert.Nil(t, denial)
		assert.Equal(t, ExecutorGrants("app"), reviewed)
	})
	t.Run("the first missing grant is the denial and places the RoleBinding next to the policy", func(t *testing.T) {
		denial, err := VerifyExecutorAccess("app", "longhorn-system", func(attributes authorizationv1.ResourceAttributes) (bool, error) {
			return attributes.Resource != "volumehookpolicies", nil
		})
		require.NoError(t, err)
		require.NotNil(t, denial)
		assert.Contains(t, denial.Reason, "may not get volumehookpolicies in namespace app")
		assert.Contains(t, denial.Reason, "RoleBinding next to the VolumeHookPolicy")
		assert.NotContains(t, denial.Reason, "kind: RoleBinding")
		assert.Equal(t, hookexecutor.RoleBindingManifest("app", "longhorn-system"), denial.Manifest)
	})
	t.Run("a review that cannot be completed is an error, not a denial", func(t *testing.T) {
		denial, err := VerifyExecutorAccess("app", "longhorn-system", func(authorizationv1.ResourceAttributes) (bool, error) {
			return false, errors.New("apiserver unreachable")
		})
		require.Error(t, err)
		assert.Nil(t, denial)
		assert.Contains(t, err.Error(), "could not verify that the hook executor may create pods/exec in namespace app")
		assert.Contains(t, err.Error(), "apiserver unreachable")
		assert.NotContains(t, err.Error(), "kind: RoleBinding")
	})
}
