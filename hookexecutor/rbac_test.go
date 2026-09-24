package hookexecutor

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	authorizationv1 "k8s.io/api/authorization/v1"
)

func TestVerifyAccess(t *testing.T) {
	grants := []authorizationv1.ResourceAttributes{
		{Namespace: "app", Verb: "create", Resource: "pods", Subresource: "exec"},
		{Namespace: "app", Verb: "get", Resource: "widgets"},
	}

	t.Run("every grant held is no denial", func(t *testing.T) {
		var reviewed []authorizationv1.ResourceAttributes
		denial, err := VerifyAccess("app", "longhorn-system", grants, "next to the Widget", func(attributes authorizationv1.ResourceAttributes) (bool, error) {
			reviewed = append(reviewed, attributes)
			return true, nil
		})
		require.NoError(t, err)
		assert.Nil(t, denial)
		assert.Equal(t, grants, reviewed)
	})
	t.Run("the first missing grant is denied with the reason and the RoleBinding as separate fields", func(t *testing.T) {
		denial, err := VerifyAccess("app", "longhorn-system", grants, "next to the Widget", func(attributes authorizationv1.ResourceAttributes) (bool, error) {
			return attributes.Resource != "widgets", nil
		})
		require.NoError(t, err)
		require.NotNil(t, denial)
		assert.Equal(t, "the hook executor may not get widgets in namespace app; grant it by applying this RoleBinding next to the Widget", denial.Reason)
		assert.Equal(t, RoleBindingManifest("app", "longhorn-system"), denial.Manifest)
	})
	t.Run("the message is the reason followed by the manifest", func(t *testing.T) {
		// The full text is pinned because admission returns it as the
		// rejection and the HookReleaseBlocked event carries the same
		// manifest. Changing it changes what operators see and apply.
		denial, err := VerifyAccess("app", "longhorn-system", grants, "next to the Widget", func(attributes authorizationv1.ResourceAttributes) (bool, error) {
			return attributes.Resource != "widgets", nil
		})
		require.NoError(t, err)
		require.NotNil(t, denial)
		assert.Equal(t, `the hook executor may not get widgets in namespace app; grant it by applying this RoleBinding next to the Widget:
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: longhorn-hook-executor
  namespace: app
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ClusterRole, name: longhorn-hook-executor}
subjects:
- {kind: ServiceAccount, name: longhorn-hook-executor, namespace: longhorn-system}
`, denial.Message())
	})
	t.Run("a review that cannot be completed is an error, not a denial", func(t *testing.T) {
		denial, err := VerifyAccess("app", "longhorn-system", grants, "next to the Widget", func(authorizationv1.ResourceAttributes) (bool, error) {
			return false, errors.New("apiserver unreachable")
		})
		require.Error(t, err)
		assert.Nil(t, denial)
		assert.Contains(t, err.Error(), "could not verify that the hook executor may create pods/exec in namespace app")
		assert.Contains(t, err.Error(), "apiserver unreachable")
		assert.NotContains(t, err.Error(), "kind: RoleBinding")
	})
}
