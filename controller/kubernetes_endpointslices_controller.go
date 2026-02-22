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
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientset "k8s.io/client-go/kubernetes"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
)

const (
	KubernetesEndpointSlicesControllerName = "kubernetes-endpointslices-controller"
)

type KubernetesEndpointSlicesController struct {
	*baseController

	namespace    string
	controllerID string

	kubeClient clientset.Interface
	ds         *datastore.DataStore
	cacheSyncs []cache.InformerSynced
}

func NewKubernetesEndpointSlicesController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) (*KubernetesEndpointSlicesController, error) {

	controller := &KubernetesEndpointSlicesController{
		baseController: newBaseController(KubernetesEndpointSlicesControllerName, logger),

		namespace:    namespace,
		controllerID: controllerID,

		kubeClient: kubeClient,
		ds:         ds,
	}

	var err error

	if _, err = ds.EndpointSlicesInformer.AddEventHandlerWithResyncPeriod(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.enqueue,
		UpdateFunc: func(old, cur interface{}) { controller.enqueue(cur) },
	}, 0); err != nil {
		return nil, err
	}
	controller.cacheSyncs = append(controller.cacheSyncs, ds.EndpointSlicesInformer.HasSynced)

	if _, err = ds.PodInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    controller.enqueuePodChange,
		UpdateFunc: func(old, cur interface{}) { controller.enqueuePodChange(cur) },
	}); err != nil {
		return nil, err
	}
	controller.cacheSyncs = append(controller.cacheSyncs, ds.PodInformer.HasSynced)

	return controller, nil
}

func (c *KubernetesEndpointSlicesController) enqueue(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	c.queue.Add(key)
}

func (c *KubernetesEndpointSlicesController) enqueuePodChange(obj interface{}) {
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

		c.logger.Tracef("Enqueueing Kubernetes EndpointSlices %v for pod %v", ownerRef.Name, pod.Name)
		c.queue.Add(c.namespace + "/" + ownerRef.Name)
		break
	}
}

func (c *KubernetesEndpointSlicesController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()

	c.logger.Info("Starting Kubernetes EndpointSlices controller")
	defer c.logger.Info("Shut down Kubernetes EndpointSlices controller")

	if !cache.WaitForNamedCacheSync(c.name, stopCh, c.cacheSyncs...) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (c *KubernetesEndpointSlicesController) worker() {
	for c.processNextWorkItem() {
	}
}

func (c *KubernetesEndpointSlicesController) processNextWorkItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(key)

	err := c.sync(key.(string))
	c.handleErr(err, key)

	return true
}

func (c *KubernetesEndpointSlicesController) handleErr(err error, key interface{}) {
	if err == nil {
		c.queue.Forget(key)
		return
	}

	log := c.logger.WithField("endpointslices", key)
	handleReconcileErrorLogging(log, err, "Failed to sync Kubernetes EndpointSlices")
	c.queue.AddRateLimited(key)
}

func (c *KubernetesEndpointSlicesController) sync(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to sync Kubernetes EndpointSlices %v", key)
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

func getLoggerForKubernetesEndpointSlices(logger logrus.FieldLogger, endpointSlices *discoveryv1.EndpointSlice) *logrus.Entry {
	return logger.WithField("endpointslices", endpointSlices.Name)
}

func (c *KubernetesEndpointSlicesController) reconcile(endpointSliceName string) (err error) {
	endpointSlice, err := c.ds.GetKubernetesEndpointSlices(c.namespace, endpointSliceName)
	if err != nil {
		if apierrors.IsNotFound(err) {
			logrus.WithError(err).Debugf("Aborting Kubernetes EndpointSlices reconcile, %s:%s not found", c.namespace, endpointSliceName)
			return nil
		}
		return err
	}

	log := getLoggerForKubernetesEndpointSlices(c.logger, endpointSlice)

	if len(endpointSlice.OwnerReferences) == 0 {
		log.Debug("Aborting Kubernetes EndpointSlices reconcile due to missing owner references")
		return nil
	}

	if !endpointSlice.DeletionTimestamp.IsZero() {
		log.Debug("Aborting Kubernetes EndpointSlices reconcile due to EndpointSlices being marked for deletion")
		return nil
	}

	existingEndpointSlice := endpointSlice.DeepCopy()
	defer func() {
		if err != nil {
			return
		}

		if reflect.DeepEqual(existingEndpointSlice, endpointSlice) {
			return
		}

		_, err = c.ds.UpdateKubernetesEndpointSlices(endpointSlice)
		if err != nil {
			log.WithError(err).Debug("Requeue due to failed Kubernetes EndpointSlices update")
			c.enqueue(endpointSlice)
			err = nil // nolint: ineffassign
			return
		}
	}()

	for _, ownerRef := range endpointSlice.OwnerReferences {
		if ownerRef.Kind == types.LonghornKindShareManager {
			err := c.syncShareManager(endpointSlice)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (c *KubernetesEndpointSlicesController) syncShareManager(endpointSlice *discoveryv1.EndpointSlice) (err error) {
	if name, ok := endpointSlice.Labels[types.KubernetesServiceName]; !ok || name != endpointSlice.Name {
		endpointSlice.Labels[types.KubernetesServiceName] = endpointSlice.Name
	}

	shareManagerPods, err := c.ds.ListShareManagerPodsRO(endpointSlice.Name)
	if err != nil {
		return err
	}

	log := getLoggerForKubernetesEndpointSlices(c.logger, endpointSlice)

	desiredIPPod := make(map[string]*corev1.Pod)
	for _, pod := range shareManagerPods {
		storageIP := c.ds.GetStorageIPFromPod(pod)
		if _, exist := desiredIPPod[storageIP]; exist {
			log.Warnf("Found duplicated share manager pod storage IP %v", storageIP)
			continue
		}

		if storageIP == "" {
			log.Debugf("Skip share manager pod %v without storage IP", pod.Name)
			continue
		}

		desiredIPPod[storageIP] = pod
	}

	// A map of existing IP in the Endpoint and its endpoint address.
	existingIPEndpointAddress := make(map[string]discoveryv1.Endpoint)
	for _, endpoint := range endpointSlice.Endpoints {
		for _, address := range endpoint.Addresses {
			existingIPEndpointAddress[address] = endpoint
		}
	}

	// Identify the address to delete, add and update.
	ipAddressToDelete := c.identifyIPEndpointToDelete(endpointSlice, desiredIPPod)
	endpointToAdd, endpointToUpdate := c.identifyIPEndpointToAddOrUpdate(existingIPEndpointAddress, desiredIPPod, createDesiredEndpointForShareManager)

	// If no change, log and return.
	if len(ipAddressToDelete) == 0 && len(endpointToAdd) == 0 && len(endpointToUpdate) == 0 {
		log.Trace("No need to reconcile Kubernetes EndpointSlices because there is no change to the subset")
		return nil
	}

	// Update the Endpoint with the identified changes.
	updateEndpointSlices(endpointSlice, ipAddressToDelete, endpointToAdd, endpointToUpdate, log)

	return nil
}

func (c *KubernetesEndpointSlicesController) identifyIPEndpointToDelete(endpointSlices *discoveryv1.EndpointSlice, desiredIPPod map[string]*corev1.Pod) map[string]bool {
	addressToDelete := make(map[string]bool)
	for _, endpoint := range endpointSlices.Endpoints {
		for _, address := range endpoint.Addresses {
			// Check if the address is in the desired IP pod map.
			if _, exists := desiredIPPod[address]; !exists {
				// If it does not exist, mark it for deletion.
				addressToDelete[address] = true
			}
		}
	}
	return addressToDelete
}

func (c *KubernetesEndpointSlicesController) identifyIPEndpointToAddOrUpdate(existingIPEndpointAddress map[string]discoveryv1.Endpoint, desiredIPPod map[string]*corev1.Pod, fnCreateDesiredEndpointSlices func(pod *corev1.Pod, storageIP string) discoveryv1.EndpointSlice) (endpointToAdd map[string]discoveryv1.EndpointSlice, endpointToUpdate map[string]discoveryv1.EndpointSlice) {
	endpointToAdd = make(map[string]discoveryv1.EndpointSlice)
	endpointToUpdate = make(map[string]discoveryv1.EndpointSlice)

	for desiredIP, pod := range desiredIPPod {
		desiredEndpointSlices := fnCreateDesiredEndpointSlices(pod, desiredIP)
		if len(desiredEndpointSlices.Endpoints) == 0 {
			c.logger.Error("BUG: identifyIPEndpointToAddOrUpdate: failed to create desired EndpointSlices because there is no endpoint")
			continue
		}

		desiredEndpoint := desiredEndpointSlices.Endpoints[0]

		// Check if the desired IP exists in the existing IP endpoint address.
		if endpoint, exists := existingIPEndpointAddress[desiredIP]; exists {
			// If it exists and the TargetRef is different, mark it for update.
			if !reflect.DeepEqual(endpoint.TargetRef, desiredEndpoint.TargetRef) {
				endpointToUpdate[desiredIP] = desiredEndpointSlices
			}
			continue
		}

		// If the desired IP does not exist, mark it to add.
		endpointToAdd[desiredIP] = desiredEndpointSlices
	}

	return endpointToAdd, endpointToUpdate
}

func createDesiredEndpointForShareManager(pod *corev1.Pod, storageIP string) discoveryv1.EndpointSlice {
	name := "nfs"
	ports := int32(2049)
	protocol := corev1.ProtocolTCP

	return discoveryv1.EndpointSlice{
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{storageIP},
				NodeName:  &pod.Spec.NodeName,
				TargetRef: &corev1.ObjectReference{
					Kind:      types.KubernetesKindPod,
					Name:      pod.Name,
					Namespace: pod.Namespace,
					UID:       pod.UID,
				},
			},
		},
		Ports: []discoveryv1.EndpointPort{
			{
				Name:     &name,
				Port:     &ports,
				Protocol: &protocol,
			},
		},
	}
}

func updateEndpointSlices(endpointSlices *discoveryv1.EndpointSlice, ipAddressToDelete map[string]bool, endpointSlicesToAdd, endpointSlicesToUpdate map[string]discoveryv1.EndpointSlice, log logrus.FieldLogger) {
	desireEndpoints := []discoveryv1.Endpoint{}
	desirePorts := []discoveryv1.EndpointPort{}

	// Add endpoint to desiredEndpoint that marked to add.
	for _, desireEndpointSlices := range endpointSlicesToAdd {
		desireEndpoints = append(desireEndpoints, desireEndpointSlices.Endpoints...)
		desirePorts = append(desirePorts, desireEndpointSlices.Ports...)
	}

	for _, endpoint := range endpointSlices.Endpoints {
		for _, address := range endpoint.Addresses {
			if ipAddressToDelete[address] {
				log.Debugf("Deleting Endpoint address %v", address)
				continue
			}

			if updated, exists := endpointSlicesToUpdate[address]; exists {
				log.Debugf("Updating Endpoint address %v", address)
				desireEndpoints = append(desireEndpoints, updated.Endpoints...)
				desirePorts = append(desirePorts, updated.Ports...)
				continue
			}
		}
	}

	// Update the Endpoint with the identified changes.
	endpointSlices.Endpoints = deduplicateEndpoints(desireEndpoints)
	endpointSlices.Ports = deduplicatePorts(desirePorts)
}

// deduplicateEndpoints removes duplicate endpoints based on the first address.
func deduplicateEndpoints(endpoints []discoveryv1.Endpoint) []discoveryv1.Endpoint {
	endpointMap := map[string]discoveryv1.Endpoint{}
	for _, ep := range endpoints {
		if len(ep.Addresses) == 0 {
			continue
		}
		endpointMap[ep.Addresses[0]] = ep
	}

	dedupEndpoint := make([]discoveryv1.Endpoint, 0, len(endpointMap))
	for _, ep := range endpointMap {
		dedupEndpoint = append(dedupEndpoint, ep)
	}
	return dedupEndpoint
}

// deduplicatePorts removes duplicate ports based on protocol/port/name combination.
func deduplicatePorts(ports []discoveryv1.EndpointPort) []discoveryv1.EndpointPort {
	portMap := map[string]discoveryv1.EndpointPort{}
	for _, port := range ports {
		if port.Port == nil || port.Protocol == nil || port.Name == nil {
			continue
		}
		key := fmt.Sprintf("%s/%d/%s", string(*port.Protocol), *port.Port, *port.Name)
		portMap[key] = port
	}

	dedupPort := make([]discoveryv1.EndpointPort, 0, len(portMap))
	for _, port := range portMap {
		dedupPort = append(dedupPort, port)
	}
	return dedupPort
}
