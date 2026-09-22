package fake

import (
	"strconv"

	"github.com/cockroachdb/errors"

	"k8s.io/apimachinery/pkg/runtime"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	clienttesting "k8s.io/client-go/testing"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

// NewEngineConflictClient returns a client on the shared tracker that rejects
// an engine update based on a stale resourceVersion like the API server. A
// separate client per actor lets a reactor of one actor call the other.
func NewEngineConflictClient(tracker clienttesting.ObjectTracker) *lhfake.Clientset {
	gvr := longhorn.SchemeGroupVersion.WithResource("engines")
	client := &lhfake.Clientset{}
	client.AddReactor("update", "engines", func(action clienttesting.Action) (bool, runtime.Object, error) {
		engine := action.(clienttesting.UpdateAction).GetObject().(*longhorn.Engine)
		obj, err := tracker.Get(gvr, engine.Namespace, engine.Name)
		if err != nil {
			return true, nil, err
		}
		stored := obj.(*longhorn.Engine)
		if engine.ResourceVersion != stored.ResourceVersion {
			return true, nil, apierrors.NewConflict(longhorn.Resource("engines"), engine.Name, errors.New("the object has been modified"))
		}
		updated := stored.DeepCopy()
		if action.GetSubresource() == "status" {
			updated.Status = engine.Status
		} else {
			updated.ObjectMeta = engine.ObjectMeta
			updated.Spec = engine.Spec
		}
		resourceVersion, err := strconv.Atoi(stored.ResourceVersion)
		if err != nil {
			return true, nil, err
		}
		updated.ResourceVersion = strconv.Itoa(resourceVersion + 1)
		if err := tracker.Update(gvr, updated, engine.Namespace); err != nil {
			return true, nil, err
		}
		return true, updated, nil
	})
	client.AddReactor("*", "*", clienttesting.ObjectReaction(tracker))
	return client
}
