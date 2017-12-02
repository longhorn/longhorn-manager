package manager

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"k8s.io/client-go/tools/cache"

	"github.com/rancher/longhorn-manager/types"

	"github.com/rancher/longhorn-manager/k8s"
	longhornv1alpha1 "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	longhornClientset "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned"
	informers "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

type TargetWatcher struct {
	nodeID    string
	eventChan chan Event
	done      chan struct{}
}

// NewTargetWatcher will create a new TargetWatcher to watch the change of
// volume TargetNode field, and notify the current node if it's pointed here
func NewTargetWatcher(nodeID string) *TargetWatcher {
	return &TargetWatcher{
		nodeID: nodeID,
	}
}

func (t *TargetWatcher) Start(ch chan Event) error {
	// Supports in-cluster running only
	config, err := k8s.GetClientConfig("")
	if err != nil {
		return errors.Wrapf(err, "unable to get client config")
	}

	longhornClient, err := longhornClientset.NewForConfig(config)
	if err != nil {
		return err
	}

	t.eventChan = ch

	informerFactory := informers.NewSharedInformerFactory(longhornClient, time.Second*30)
	volumeInformer := informerFactory.Longhorn().V1alpha1().Volumes()
	volumeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    t.onAdd,
		UpdateFunc: t.onUpdate,
		DeleteFunc: t.onDelete,
	})

	go informerFactory.Start(t.done)
	return nil
}

func (t *TargetWatcher) Stop() {
	t.done <- struct{}{}
}

func (t *TargetWatcher) GetPort() int {
	return -1
}

func (t *TargetWatcher) NodeNotify(address string, event *Event) error {
	return errors.Errorf("Target Watcher cannot notify other nodes")
}

func (t *TargetWatcher) onAdd(obj interface{}) {
	volume, ok := obj.(*longhornv1alpha1.Volume)
	if !ok {
		logrus.Errorf("BUG: fail to convert resource object to volume on add")
	}
	if volume.Spec.TargetNodeID == t.nodeID {
		var event Event
		// Don't call Create for to be deleted reconstructed volume
		if volume.Spec.DesireState != types.VolumeStateDeleted {
			event = Event{
				Type:       EventTypeCreate,
				VolumeName: volume.Name,
			}
		} else {
			event = Event{
				Type:       EventTypeNotify,
				VolumeName: volume.Name,
			}
		}
		t.eventChan <- event
	}
}

func (t *TargetWatcher) onUpdate(oldObj, newObj interface{}) {
	// Don't need notify old volume since the old owner will notice it after
	// ReconcileInterval
	newVolume, ok := newObj.(*longhornv1alpha1.Volume)
	if !ok {
		logrus.Errorf("BUG: fail to convert new resource object to volume on update")
	}
	if newVolume.Spec.TargetNodeID == t.nodeID {
		event := Event{
			Type:       EventTypeNotify,
			VolumeName: newVolume.Name,
		}
		t.eventChan <- event
	}
}

func (t *TargetWatcher) onDelete(obj interface{}) {
	volume, ok := obj.(*longhornv1alpha1.Volume)
	if !ok {
		logrus.Errorf("BUG: fail to convert resource object to volume on delete")
	}
	if volume.Spec.TargetNodeID == t.nodeID {
		// We're going to reconstruct the volume and change state to deleted
		var event Event
		if volume.Spec.DesireState != types.VolumeStateDeleted {
			v := &types.VolumeInfo{
				VolumeSpec:   volume.Spec,
				VolumeStatus: volume.Status,
				Metadata: types.Metadata{
					Name:            volume.Name,
					ResourceVersion: volume.ResourceVersion,
				},
			}
			event = Event{
				Type:   EventTypeDelete,
				Volume: v,
			}
		} else {
			event = Event{
				Type:       EventTypeNotify,
				VolumeName: volume.Name,
			}
		}
		t.eventChan <- event
	}
}
