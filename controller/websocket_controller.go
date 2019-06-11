package controller

import (
	"sync"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions/longhorn/v1alpha1"
)

type SimpleResourceEventHandler struct{ ChangeFunc func() }

func (s SimpleResourceEventHandler) OnAdd(obj interface{})               { s.ChangeFunc() }
func (s SimpleResourceEventHandler) OnUpdate(oldObj, newObj interface{}) { s.ChangeFunc() }
func (s SimpleResourceEventHandler) OnDelete(obj interface{})            { s.ChangeFunc() }

type Watcher struct {
	eventChan  chan struct{}
	resources  []string
	controller *WebsocketController
}

func (w *Watcher) Events() <-chan struct{} {
	return w.eventChan
}

func (w *Watcher) Close() {
	close(w.eventChan)
}

type WebsocketController struct {
	volumeSynced      cache.InformerSynced
	engineSynced      cache.InformerSynced
	replicaSynced     cache.InformerSynced
	settingSynced     cache.InformerSynced
	engineImageSynced cache.InformerSynced
	nodeSynced        cache.InformerSynced

	watchers    []*Watcher
	watcherLock sync.Mutex
}

func NewWebsocketController(
	volumeInformer lhinformers.VolumeInformer,
	engineInformer lhinformers.EngineInformer,
	replicaInformer lhinformers.ReplicaInformer,
	settingInformer lhinformers.SettingInformer,
	engineImageInformer lhinformers.EngineImageInformer,
	nodeInformer lhinformers.NodeInformer,
) *WebsocketController {

	wc := &WebsocketController{
		volumeSynced:      volumeInformer.Informer().HasSynced,
		engineSynced:      engineInformer.Informer().HasSynced,
		replicaSynced:     replicaInformer.Informer().HasSynced,
		settingSynced:     settingInformer.Informer().HasSynced,
		engineImageSynced: engineImageInformer.Informer().HasSynced,
		nodeSynced:        nodeInformer.Informer().HasSynced,
	}

	volumeInformer.Informer().AddEventHandler(wc.notifyWatchersHandler("volume"))
	engineInformer.Informer().AddEventHandler(wc.notifyWatchersHandler("engine"))
	replicaInformer.Informer().AddEventHandler(wc.notifyWatchersHandler("replica"))
	settingInformer.Informer().AddEventHandler(wc.notifyWatchersHandler("setting"))
	engineImageInformer.Informer().AddEventHandler(wc.notifyWatchersHandler("engineImage"))
	nodeInformer.Informer().AddEventHandler(wc.notifyWatchersHandler("node"))

	return wc
}

func (wc *WebsocketController) NewWatcher(resources ...string) *Watcher {
	wc.watcherLock.Lock()
	defer wc.watcherLock.Unlock()

	w := &Watcher{
		eventChan:  make(chan struct{}, 2),
		resources:  resources,
		controller: wc,
	}
	wc.watchers = append(wc.watchers, w)
	return w
}

func (wc *WebsocketController) Run(stopCh <-chan struct{}) {
	defer wc.Close()

	logrus.Infof("Start Longhorn websocket controller")
	defer logrus.Infof("Shutting down Longhorn websocket controller")

	if !controller.WaitForCacheSync("longhorn websocket", stopCh,
		wc.volumeSynced, wc.engineSynced, wc.replicaSynced,
		wc.settingSynced, wc.engineImageSynced, wc.nodeSynced) {
		return
	}

	<-stopCh
}

func (wc *WebsocketController) Close() {
	wc.watcherLock.Lock()
	defer wc.watcherLock.Unlock()

	for _, w := range wc.watchers {
		w.Close()
	}
	wc.watchers = wc.watchers[:0]
}

func (wc *WebsocketController) notifyWatchersHandler(resource string) cache.ResourceEventHandler {
	return SimpleResourceEventHandler{
		ChangeFunc: wc.notifyWatchersFunc(resource),
	}
}

func (wc *WebsocketController) notifyWatchersFunc(resource string) func() {
	return func() {
		wc.watcherLock.Lock()
		defer wc.watcherLock.Unlock()
		for _, w := range wc.watchers {
			for _, r := range w.resources {
				if r == resource {
					select {
					case w.eventChan <- struct{}{}:
					default:
					}
					break
				}
			}
		}
	}
}
