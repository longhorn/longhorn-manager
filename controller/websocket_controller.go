package controller

import (
	"sync"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/tools/cache"

	"github.com/longhorn/longhorn-manager/datastore"
)

type SimpleResourceEventHandler struct{ ChangeFunc func() }

func (s SimpleResourceEventHandler) OnAdd(obj interface{}, isInInitialList bool) { s.ChangeFunc() }
func (s SimpleResourceEventHandler) OnUpdate(oldObj, newObj interface{})         { s.ChangeFunc() }
func (s SimpleResourceEventHandler) OnDelete(obj interface{})                    { s.ChangeFunc() }

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
	*baseController
	cacheSyncs []cache.InformerSynced

	watchers    []*Watcher
	watcherLock sync.Mutex
}

func NewWebsocketController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
) (*WebsocketController, error) {

	wc := &WebsocketController{
		baseController: newBaseController("longhorn-websocket", logger),
	}

	var err error
	if _, err = ds.VolumeInformer.AddEventHandler(wc.notifyWatchersHandler("volume")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.VolumeInformer.HasSynced)
	if _, err = ds.EngineInformer.AddEventHandler(wc.notifyWatchersHandler("engine")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.EngineInformer.HasSynced)
	if _, err = ds.ReplicaInformer.AddEventHandler(wc.notifyWatchersHandler("replica")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.ReplicaInformer.HasSynced)
	if _, err = ds.SettingInformer.AddEventHandler(wc.notifyWatchersHandler("setting")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.SettingInformer.HasSynced)
	if _, err = ds.EngineImageInformer.AddEventHandler(wc.notifyWatchersHandler("engineImage")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.EngineImageInformer.HasSynced)
	if _, err = ds.BackingImageInformer.AddEventHandler(wc.notifyWatchersHandler("backingImage")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.BackingImageInformer.HasSynced)
	if _, err = ds.NodeInformer.AddEventHandler(wc.notifyWatchersHandler("node")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.NodeInformer.HasSynced)
	if _, err = ds.BackupTargetInformer.AddEventHandler(wc.notifyWatchersHandler("backupTarget")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.BackupTargetInformer.HasSynced)
	if _, err = ds.BackupVolumeInformer.AddEventHandler(wc.notifyWatchersHandler("backupVolume")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.BackupVolumeInformer.HasSynced)
	if _, err = ds.BackupInformer.AddEventHandler(wc.notifyWatchersHandler("backup")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.BackupInformer.HasSynced)
	if _, err = ds.RecurringJobInformer.AddEventHandler(wc.notifyWatchersHandler("recurringJob")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.RecurringJobInformer.HasSynced)
	if _, err = ds.SystemBackupInformer.AddEventHandler(wc.notifyWatchersHandler("systemBackup")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.SystemBackupInformer.HasSynced)
	if _, err = ds.SystemRestoreInformer.AddEventHandler(wc.notifyWatchersHandler("systemRestore")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.SystemRestoreInformer.HasSynced)

	if _, err = ds.BackupBackingImageInformer.AddEventHandler(wc.notifyWatchersHandler("backupBackingImage")); err != nil {
		return nil, err
	}
	wc.cacheSyncs = append(wc.cacheSyncs, ds.BackupBackingImageInformer.HasSynced)

	return wc, nil
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

	wc.logger.Info("Starting Longhorn websocket controller")
	defer wc.logger.Info("Shut down Longhorn websocket controller")

	if !cache.WaitForNamedCacheSync("longhorn websocket", stopCh, wc.cacheSyncs...) {
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
