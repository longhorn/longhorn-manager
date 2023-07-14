package controller

import (
	"fmt"
	"time"

	"github.com/sirupsen/logrus"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
)

type ObjectEndpointController struct {
	*baseController

	namespace string
	ds        *datastore.DataStore
	image     string
}

func NewObjectEndpointController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string,
	objectEndpointImage string,
) *ObjectEndpointController {
	oec := &ObjectEndpointController{
		baseController: newBaseController("object-endpoint", logger),
		namespace:      namespace,
		ds:             ds,
		image:          objectEndpointImage,
	}

	ds.ObjectEndpointInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    oec.enqueueObjectEndpoint,
			UpdateFunc: func(old, cur interface{}) { oec.enqueueObjectEndpoint(cur) },
			DeleteFunc: oec.enqueueObjectEndpoint,
		},
	)

	return oec
}

func (oec *ObjectEndpointController) Run(workers int, stopCh <-chan struct{}) {
	oec.logger.Info("Starting Longhorn Object Endpoint Controller")
	defer oec.logger.Info("Shut down Longhorn Object Endpoint Controller")
	defer oec.queue.ShutDown()

	for i := 0; i < workers; i++ {
		go wait.Until(oec.worker, time.Second, stopCh)
	}

	<-stopCh
}

func (oec *ObjectEndpointController) worker() {
	for oec.processNextWorkItem() {
	}
}

func (oec *ObjectEndpointController) processNextWorkItem() bool {
	key, quit := oec.queue.Get()
	if quit {
		return false
	}
	defer oec.queue.Done(key)

	err := oec.syncObjectEndpoint(key.(string))
	oec.handleError(err, key)

	return true
}

func (oec *ObjectEndpointController) enqueueObjectEndpoint(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Failed to get key for %v: %v", obj, err))
		return
	}
	oec.queue.Add(key)
}

func (oec *ObjectEndpointController) handleError(err error, key interface{}) {
	if err == nil {
		oec.queue.Forget(key)
		return
	}

	if oec.queue.NumRequeues(key) < maxRetries {
		oec.logger.WithError(err).Errorf("")
		oec.queue.AddRateLimited(key)
		return
	}

	utilruntime.HandleError(err)
	oec.logger.WithError(err).Errorf("")
	oec.queue.Forget(key)
}

func (oec *ObjectEndpointController) syncObjectEndpoint(key string) error {
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	endpoint, err := oec.ds.GetObjectEndpoint(name, namespace)
	if err != nil {
		return err
	}

	oec.logger.Info("Object Endpoint Controller syncing: %v", endpoint.Name)
	oec.logger.Info("Object Endpoint Controller should deploy: %v", oec.image)
	// TODO: Actually do something here

	return nil
}
