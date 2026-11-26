package snapshothook

import (
	"testing"

	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestPolicyRef(t *testing.T) {
	policy := &longhorn.VolumeHookPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "mongo", UID: "uid-mongo", Generation: 4}}
	assert.Equal(t, "default/mongo", PolicyNamespacedName(policy))

	ref := PolicyRef(policy)
	assert.Equal(t, &longhorn.VolumeHookPolicyRef{Namespace: "default", Name: "mongo", UID: "uid-mongo", Generation: 4}, ref)
	assert.Equal(t, "default/mongo", PolicyRefNamespacedName(ref))
	assert.Equal(t, "none", PolicyRefNamespacedName(nil))

	assert.Equal(t, "app/data", PVCRefNamespacedName(&longhorn.PVCRef{Namespace: "app", Name: "data"}))
	assert.Equal(t, "", PVCRefNamespacedName(nil))
}
