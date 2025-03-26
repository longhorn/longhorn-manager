package controller

import (
	"fmt"
	"reflect"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	KubernetesEndpointControllerName = "kubernetes-endpoint-controller"
)

type KubernetesEndpointController struct {
	*baseController

	namespace    string
	controllerID string

	kubeClient clientset.Interface
	ds         *datastore.DataStore
	cacheSyncs []cache.InformerSynced
}

func NewKubernetesEndpointController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) (*KubernetesEndpointController, error) {

	controller := &KubernetesEndpointController{
		baseController: newBaseController(KubernetesEndpointControllerName, logger),

		namespace:    namespace,
		controllerID: controllerID,

		kubeClient: kubeClient,
		ds:         ds,
	}

	var err error

	if _, err = ds.EndpointInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.enqueue,
		UpdateFunc: func(old, cur interface{}) { controller.enqueue(cur) },
	}, 0); err != nil {
		return nil, err
	}
	controller.cacheSyncs = append(controller.cacheSyncs, ds.EndpointInformer.HasSynced)

	if _, err = ds.PodInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.enqueuePodChange,
		UpdateFunc: func(old, cur interface{}) { controller.enqueuePodChange(cur) },
	}); err != nil {
		return nil, err
	}
	controller.cacheSyncs = append(controller.cacheSyncs, ds.PodInformer.HasSynced)

	return controller, nil
}

func (c *KubernetesEndpointController) enqueue(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.Add(key)
}

func (c *KubernetesEndpointController) enqueuePodChange(obj interface{}) {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		deletedState, ok := obj.(cache.DeletedFinalStateUnknown)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("received unexpected obj: %#v", obj))
			return
		}

		// use the last known state, to enqueue, dependent objects
		pod, ok = deletedState.Obj.(*corev1.Pod)
		if !ok {
			utilruntime.HandleError(fmt.Errorf("DeletedFinalStateUnknown contained invalid object: %#v", deletedState.Obj))
			return
		}
	}

	podMeta := pod.GetObjectMeta()
	ownerRefs := podMeta.GetOwnerReferences()
	for _, ownerRef := range ownerRefs {
		if ownerRef.Kind != types.LonghornKindShareManager {
			continue
		}

		c.logger.Tracef("Enqueueing Kubernetes Endpoint %v for pod %v", ownerRef.Name, pod.Name)
		c.queue.Add(c.namespace + "/" + ownerRef.Name)
		break
	}
}

func (c *KubernetesEndpointController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting Kubernetes Endpoint controller")
	defer c.logger.Info("Shut down Kubernetes Endpoint controller")

	if !cache.WaitForNamedCacheSync(c.name, stopCh, c.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (c *KubernetesEndpointController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *KubernetesEndpointController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.sync(key.(string))
	c.handleErr(err, key)

	return true
}

func (c *KubernetesEndpointController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	log := c.logger.WithField("endpoint", key)
	handleReconcileErrorLogging(log, err, "Failed to sync Kubernetes Endpoint")
	c.queue.AddRateLimited(key)
}

func (c *KubernetesEndpointController) sync(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync Kubernetes Endpoint %v", key)
	}()

	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if namespace != c.namespace {
		return nil
	}

	return c.reconcile(name)
}

func getLoggerForKubernetesEndpoint(logger logrus.FieldLogger, endpoint *corev1.Endpoints) *logrus.Entry {
	return logger.WithField("endpoint", endpoint.Name)
}

func (c *KubernetesEndpointController) reconcile(endpointName string) (err error) {
	endpoint, err := c.ds.GetKubernetesEndpointRO(endpointName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.WithError(err).Debug("Aborting Kubernetes Endpoint reconcile")
			return nil
		}
		return err
	}

	log := getLoggerForKubernetesEndpoint(c.logger, endpoint)

	if endpoint.OwnerReferences == nil {
		log.Debug("Aborting Kubernetes Endpoint reconcile due to missing owner references")
		return nil
	}

	if !endpoint.DeletionTimestamp.IsZero() {
		log.Debug("Aborting Kubernetes Endpoint reconcile due to Endpoint being marked for deletion")
		return nil
	}

	existingEndpoint := endpoint.DeepCopy()
	defer func() {
		if err != nil {
			return
		}

		if reflect.DeepEqual(existingEndpoint, endpoint) {
			return
		}

		_, err = c.ds.UpdateKubernetesEndpoint(endpoint)
		if err != nil {
			log.WithError(err).Debug("Requeue due to failed Kubernetes Endpoint update")
			c.enqueue(endpoint)
			err = nil // nolint: ineffassign
			return
		}
	}()

	for _, ownerRef := range endpoint.OwnerReferences {
		switch ownerRef.Kind {
		case types.LonghornKindShareManager:
			err := c.syncShareManager(endpoint)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *KubernetesEndpointController) syncShareManager(endpoint *corev1.Endpoints) (err error) {
	// Retrieve a list of share manager pod by the Endpoint
	shareManagerPods, err := c.ds.ListShareManagerPodsRO(endpoint.Name)
	if err != nil {
		return err
	}

	log := getLoggerForKubernetesEndpoint(c.logger, endpoint)

	// A map of desired share manager IP and its pod.
	desiredIPPod := make(map[string]*corev1.Pod)
	for _, pod := range shareManagerPods {
		storageIP := c.ds.GetStorageIPFromPod(pod)
		if _, exist := desiredIPPod[storageIP]; exist {
			log.Warnf("Found duplicated share manager pod storage IP %v", storageIP)
			continue
		}

		desiredIPPod[storageIP] = pod
	}

	// A map of existing IP in the Endpoint and its endpoint address.
	existingIPEndpointAddress := make(map[string]corev1.EndpointAddress)
	for _, subset := range endpoint.Subsets {
		for _, address := range subset.Addresses {
			existingIPEndpointAddress[address.IP] = address
		}
	}

	// Identify the address to delete, add and update.
	ipSubsetToDelete := c.identifyIPSubsetToDelete(endpoint, desiredIPPod)
	ipSubsetToAdd, addressToUpdate := c.identifyIPSubsetToAddOrUpdate(existingIPEndpointAddress, desiredIPPod, createDesiredSubsetForShareManager)

	// If no change, log and return.
	if len(ipSubsetToDelete) == 0 && len(ipSubsetToAdd) == 0 && len(addressToUpdate) == 0 {
		log.Trace("No need to reconcile Kubernetes Endpoint because there is no change to the subset")
		return nil
	}

	// Update the Endpoint subsets with the identified changes.
	updateEndpointSubsets(endpoint, ipSubsetToDelete, ipSubsetToAdd, addressToUpdate, log)

	return nil
}

func (c *KubernetesEndpointController) identifyIPSubsetToDelete(endpoint *corev1.Endpoints, desiredIPPod map[string]*corev1.Pod) map[string]bool {
	addressToDelete := make(map[string]bool)
	for _, subset := range endpoint.Subsets {
		for _, address := range subset.Addresses {
			// Check if the address is in the desired IP pod map.
			if _, exists := desiredIPPod[address.IP]; !exists {
				// If it does not exist, mark it for deletion.
				addressToDelete[address.IP] = true
			}
		}
	}
	return addressToDelete
}

func (c *KubernetesEndpointController) identifyIPSubsetToAddOrUpdate(existingIPEndpointAddress map[string]corev1.EndpointAddress, desiredIPPod map[string]*corev1.Pod, fnCreateDesiredSubset func(pod *corev1.Pod, storageIP string) corev1.EndpointSubset) (ipSubsetToAdd map[string]corev1.EndpointSubset, ipSubsetToUpdate map[string]corev1.EndpointSubset) {
	ipSubsetToAdd = make(map[string]corev1.EndpointSubset)
	ipSubsetToUpdate = make(map[string]corev1.EndpointSubset)

	for desiredIP, pod := range desiredIPPod {
		desiredSubset := fnCreateDesiredSubset(pod, desiredIP)

		// Check if the desired IP exists in the existing IP endpoint address.
		if endpointAddress, exists := existingIPEndpointAddress[desiredIP]; exists {
			// If it exists and the TargetRef is different, mark it for update.
			if !reflect.DeepEqual(endpointAddress.TargetRef, desiredSubset.Addresses[0].TargetRef) {
				ipSubsetToUpdate[desiredIP] = desiredSubset
			}
			continue
		}

		// If the desired IP does not exist, mark it to add.
		ipSubsetToAdd[desiredIP] = desiredSubset
	}

	return ipSubsetToAdd, ipSubsetToUpdate
}

func createDesiredSubsetForShareManager(pod *corev1.Pod, storageIP string) corev1.EndpointSubset {
	return corev1.EndpointSubset{
		Addresses: []corev1.EndpointAddress{
			{
				IP:       storageIP,
				NodeName: &pod.Spec.NodeName,
				TargetRef: &corev1.ObjectReference{
					Kind:      types.KubernetesKindPod,
					Name:      pod.Name,
					Namespace: pod.Namespace,
					UID:       pod.UID,
				},
			},
		},
		Ports: []corev1.EndpointPort{
			{
				Name:     "nfs",
				Port:     2049,
				Protocol: corev1.ProtocolTCP,
			},
		},
	}
}

func updateEndpointSubsets(endpoint *corev1.Endpoints, ipSubsetToDelete map[string]bool, ipSubsetToAdd, ipSubsetToUpdate map[string]corev1.EndpointSubset, log logrus.FieldLogger) {
	desireSubsets := []corev1.EndpointSubset{}

	// Add subsets to desiredSubsets that marked to add.
	for _, desireSubset := range ipSubsetToAdd {
		desireSubsets = append(desireSubsets, desireSubset)
	}

	// Iterate over existing subsets in the Endpoint.
	for _, subset := range endpoint.Subsets {
		// Iterate over the addresses in the subset.
		for _, address := range subset.Addresses {
			// Check if the address is marked for deletion.
			// If it is, log and skip to add it to desiredSubsets.
			if ipSubsetToDelete[address.IP] {
				log.Debugf("Deleting Endpoint subset %v", address)
				continue
			}

			// Check if the address is marked for update.
			// If it is, log and add it to desiredSubsets.
			if desireSubset, exist := ipSubsetToUpdate[address.IP]; exist {
				log.Debugf("Updating Endpoint subset %v", address)
				desireSubsets = append(desireSubsets, desireSubset)
				continue
			}
		}
	}

	// Update the Endpoint subsets with the identified changes.
	endpoint.Subsets = desireSubsets
}
