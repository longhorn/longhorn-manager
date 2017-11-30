package manager

import (
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"k8s.io/client-go/tools/cache"

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
	if volume.TargetNodeID == t.nodeID {
		event := Event{
			Type:       EventTypeNotify,
			VolumeName: volume.Name,
		}
		t.eventChan <- event
	}
}

func (t *TargetWatcher) onUpdate(oldObj, newObj interface{}) {
	oldVolume, ok := oldObj.(*longhornv1alpha1.Volume)
	if !ok {
		logrus.Errorf("BUG: fail to convert new resource object to volume on update")
	}
	if oldVolume.TargetNodeID == t.nodeID {
		event := Event{
			Type:       EventTypeNotify,
			VolumeName: oldVolume.Name,
		}
		t.eventChan <- event
	}

	newVolume, ok := newObj.(*longhornv1alpha1.Volume)
	if !ok {
		logrus.Errorf("BUG: fail to convert new resource object to volume on update")
	}
	if newVolume.TargetNodeID == t.nodeID {
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
	//FIXME Delete won't work if CRD was deleted directly
	if volume.TargetNodeID == t.nodeID {
		event := Event{
			Type:       EventTypeNotify,
			VolumeName: volume.Name,
		}
		t.eventChan <- event
	}
}
