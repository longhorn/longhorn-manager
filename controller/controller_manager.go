package controller

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/util/workqueue"

	corev1 "k8s.io/api/core/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/longhorn/longhorn-manager/util/client"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

var (
	Workers              = 5
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group
)

// StartControllers initiates all Longhorn component controllers and monitors to manage the creating, updating, and deletion of Longhorn resources
func StartControllers(logger logrus.FieldLogger, clients *client.Clients,
	controllerID, serviceAccount, managerImage, backingImageManagerImage, shareManagerImage,
	kubeconfigPath, version string, proxyConnCounter util.Counter) (*WebsocketController, error) {
	namespace := clients.Namespace
	kubeClient := clients.Clients.K8s
	metricsClient := clients.MetricsClient
	ds := clients.Datastore
	scheme := clients.Scheme
	stopCh := clients.StopCh

	// Longhorn controllers
	replicaController, err := NewReplicaController(logger, ds, scheme, kubeClient, namespace, controllerID)
	if err != nil {
		return nil, err
	}
	engineController, err := NewEngineController(logger, ds, scheme, kubeClient, &engineapi.EngineCollection{}, namespace, controllerID, proxyConnCounter)
	if err != nil {
		return nil, err
	}
	volumeController, err := NewVolumeController(logger, ds, scheme, kubeClient, namespace, controllerID, shareManagerImage, proxyConnCounter)
	if err != nil {
		return nil, err
	}
	engineImageController, err := NewEngineImageController(logger, ds, scheme, kubeClient, namespace, controllerID, serviceAccount)
	if err != nil {
		return nil, err
	}
	nodeController, err := NewNodeController(logger, ds, scheme, kubeClient, namespace, controllerID)
	if err != nil {
		return nil, err
	}
	websocketController, err := NewWebsocketController(logger, ds)
	if err != nil {
		return nil, err
	}
	settingController, err := NewSettingController(logger, ds, scheme, kubeClient, metricsClient, namespace, controllerID, version)
	if err != nil {
		return nil, err
	}
	backupTargetController, err := NewBackupTargetController(logger, ds, scheme, kubeClient, controllerID, namespace, proxyConnCounter)
	if err != nil {
		return nil, err
	}
	backupVolumeController, err := NewBackupVolumeController(logger, ds, scheme, kubeClient, controllerID, namespace, proxyConnCounter)
	if err != nil {
		return nil, err
	}
	backupController, err := NewBackupController(logger, ds, scheme, kubeClient, controllerID, namespace, proxyConnCounter)
	if err != nil {
		return nil, err
	}
	backupBackingImageController, err := NewBackupBackingImageController(logger, ds, scheme, kubeClient, controllerID, namespace, proxyConnCounter)
	if err != nil {
		return nil, err
	}
	instanceManagerController, err := NewInstanceManagerController(logger, ds, scheme, kubeClient, namespace, controllerID, serviceAccount)
	if err != nil {
		return nil, err
	}
	shareManagerController, err := NewShareManagerController(logger, ds, scheme, kubeClient, namespace, controllerID, serviceAccount)
	if err != nil {
		return nil, err
	}
	backingImageController, err := NewBackingImageController(logger, ds, scheme, kubeClient, namespace, controllerID, serviceAccount, backingImageManagerImage)
	if err != nil {
		return nil, err
	}
	backingImageManagerController, err := NewBackingImageManagerController(logger, ds, scheme, kubeClient, namespace, controllerID, serviceAccount, backingImageManagerImage)
	if err != nil {
		return nil, err
	}
	backingImageDataSourceController, err := NewBackingImageDataSourceController(logger, ds, scheme, kubeClient, namespace, controllerID, serviceAccount, backingImageManagerImage, proxyConnCounter)
	if err != nil {
		return nil, err
	}
	recurringJobController, err := NewRecurringJobController(logger, ds, scheme, kubeClient, namespace, controllerID, serviceAccount, managerImage)
	if err != nil {
		return nil, err
	}
	orphanController, err := NewOrphanController(logger, ds, scheme, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}
	snapshotController, err := NewSnapshotController(logger, ds, scheme, kubeClient, namespace, controllerID, &engineapi.EngineCollection{}, proxyConnCounter)
	if err != nil {
		return nil, err
	}
	supportBundleController, err := NewSupportBundleController(logger, ds, scheme, kubeClient, controllerID, namespace, serviceAccount)
	if err != nil {
		return nil, err
	}
	systemBackupController, err := NewSystemBackupController(logger, ds, scheme, kubeClient, namespace, controllerID, managerImage)
	if err != nil {
		return nil, err
	}
	systemRestoreController, err := NewSystemRestoreController(logger, ds, scheme, kubeClient, namespace, controllerID)
	if err != nil {
		return nil, err
	}
	volumeAttachmentController, err := NewLonghornVolumeAttachmentController(logger, ds, scheme, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}
	volumeRestoreController, err := NewVolumeRestoreController(logger, ds, scheme, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}
	volumeRebuildingController, err := NewVolumeRebuildingController(logger, ds, scheme, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}
	volumeEvictionController, err := NewVolumeEvictionController(logger, ds, scheme, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}
	volumeCloneController, err := NewVolumeCloneController(logger, ds, scheme, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}
	volumeExpansionController, err := NewVolumeExpansionController(logger, ds, scheme, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}

	dataEngineUpgradeManagerController, err := NewDataEngineUpgradeManagerController(logger, ds, scheme, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}

	nodeDataEngineUpgradeController, err := NewNodeDataEngineUpgradeController(logger, ds, scheme, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}

	// Kubernetes controllers
	kubernetesPVController, err := NewKubernetesPVController(logger, ds, scheme, kubeClient, controllerID)
	if err != nil {
		return nil, err
	}
	kubernetesNodeController, err := NewKubernetesNodeController(logger, ds, scheme, kubeClient, controllerID)
	if err != nil {
		return nil, err
	}
	kubernetesPodController, err := NewKubernetesPodController(logger, ds, scheme, kubeClient, controllerID)
	if err != nil {
		return nil, err
	}
	kubernetesConfigMapController, err := NewKubernetesConfigMapController(logger, ds, scheme, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}
	kubernetesSecretController, err := NewKubernetesSecretController(logger, ds, scheme, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}
	kubernetesPDBController, err := NewKubernetesPDBController(logger, ds, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}
	kubernetesEndpointController, err := NewKubernetesEndpointController(logger, ds, kubeClient, controllerID, namespace)
	if err != nil {
		return nil, err
	}

	// Start goroutines for Longhorn controllers
	go replicaController.Run(Workers, stopCh)
	go engineController.Run(Workers, stopCh)
	go volumeController.Run(Workers, stopCh)
	go engineImageController.Run(Workers, stopCh)
	go nodeController.Run(Workers, stopCh)
	go websocketController.Run(stopCh)
	go settingController.Run(stopCh)
	go instanceManagerController.Run(Workers, stopCh)
	go shareManagerController.Run(Workers, stopCh)
	go backingImageController.Run(Workers, stopCh)
	go backingImageManagerController.Run(Workers, stopCh)
	go backingImageDataSourceController.Run(Workers, stopCh)
	go backupTargetController.Run(Workers, stopCh)
	go backupVolumeController.Run(Workers, stopCh)
	go backupController.Run(Workers, stopCh)
	go backupBackingImageController.Run(Workers, stopCh)
	go recurringJobController.Run(Workers, stopCh)
	go orphanController.Run(Workers, stopCh)
	go snapshotController.Run(Workers, stopCh)
	go supportBundleController.Run(Workers, stopCh)
	go systemBackupController.Run(Workers, stopCh)
	go systemRestoreController.Run(Workers, stopCh)
	go volumeAttachmentController.Run(Workers, stopCh)
	go volumeRestoreController.Run(Workers, stopCh)
	go volumeRebuildingController.Run(Workers, stopCh)
	go volumeEvictionController.Run(Workers, stopCh)
	go volumeCloneController.Run(Workers, stopCh)
	go volumeExpansionController.Run(Workers, stopCh)
	// The two controllers do not support upgrade in parallel
	go dataEngineUpgradeManagerController.Run(1, stopCh)
	go nodeDataEngineUpgradeController.Run(1, stopCh)

	// Start goroutines for Kubernetes controllers
	go kubernetesPVController.Run(Workers, stopCh)
	go kubernetesNodeController.Run(Workers, stopCh)
	go kubernetesPodController.Run(Workers, stopCh)
	go kubernetesConfigMapController.Run(Workers, stopCh)
	go kubernetesSecretController.Run(Workers, stopCh)
	go kubernetesPDBController.Run(Workers, stopCh)
	go kubernetesEndpointController.Run(Workers, stopCh)

	return websocketController, nil
}

func ParseResourceRequirement(val string) (*corev1.ResourceRequirements, error) {
	quantity, err := resource.ParseQuantity(val)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse value %v to a quantity", val)
	}
	if quantity.IsZero() {
		return nil, nil
	}
	return &corev1.ResourceRequirements{
		Requests: corev1.ResourceList{
			corev1.ResourceCPU: quantity,
		},
	}, nil
}

// GetInstanceManagerCPURequirement returns the instance manager CPU requirement
func GetInstanceManagerCPURequirement(ds *datastore.DataStore, imName string) (*corev1.ResourceRequirements, error) {
	im, err := ds.GetInstanceManager(imName)
	if err != nil {
		return nil, err
	}

	lhNode, err := ds.GetNode(im.Spec.NodeID)
	if err != nil {
		return nil, err
	}
	kubeNode, err := ds.GetKubernetesNodeRO(im.Spec.NodeID)
	if err != nil {
		return nil, err
	}

	cpuRequest := 0
	switch im.Spec.DataEngine {
	case longhorn.DataEngineTypeV1:
		cpuRequest = lhNode.Spec.InstanceManagerCPURequest
		if cpuRequest == 0 {
			guaranteedCPUSetting, err := ds.GetSettingWithAutoFillingRO(types.SettingNameGuaranteedInstanceManagerCPU)
			if err != nil {
				return nil, err
			}
			guaranteedCPUPercentage, err := strconv.ParseFloat(guaranteedCPUSetting.Value, 64)
			if err != nil {
				return nil, err
			}
			allocatableMilliCPU := float64(kubeNode.Status.Allocatable.Cpu().MilliValue())
			cpuRequest = int(math.Round(allocatableMilliCPU * guaranteedCPUPercentage / 100.0))
		}
	case longhorn.DataEngineTypeV2:
		// TODO: Support CPU request per node for v2 volumes
		guaranteedCPUSetting, err := ds.GetSettingWithAutoFillingRO(types.SettingNameV2DataEngineGuaranteedInstanceManagerCPU)
		if err != nil {
			return nil, err
		}
		guaranteedCPURequest, err := strconv.ParseFloat(guaranteedCPUSetting.Value, 64)
		if err != nil {
			return nil, err
		}
		cpuRequest = int(guaranteedCPURequest)
	default:
		return nil, fmt.Errorf("unknown data engine %v", im.Spec.DataEngine)
	}

	return ParseResourceRequirement(fmt.Sprintf("%dm", cpuRequest))
}

func isControllerResponsibleFor(controllerID string, ds *datastore.DataStore, name, preferredOwnerID, currentOwnerID string) bool {
	// we use this approach so that if there is an issue with the data store
	// we don't accidentally transfer ownership
	isOwnerUnavailable := func(node string) bool {
		isUnavailable, err := ds.IsNodeDownOrDeletedOrMissingManager(node)
		if node != "" && err != nil {
			logrus.Errorf("Error while checking IsNodeDownOrDeletedOrMissingManager for object %v, node %v: %v", name, node, err)
		}
		return node == "" || isUnavailable
	}

	isPreferredOwner := controllerID == preferredOwnerID
	continueToBeOwner := currentOwnerID == controllerID && isOwnerUnavailable(preferredOwnerID)
	requiresNewOwner := isOwnerUnavailable(currentOwnerID) && isOwnerUnavailable(preferredOwnerID)
	return isPreferredOwner || continueToBeOwner || requiresNewOwner
}

// EnhancedDefaultControllerRateLimiter is an enhanced version of workqueue.DefaultControllerRateLimiter()
// See https://github.com/longhorn/longhorn/issues/1058 for details
func EnhancedDefaultControllerRateLimiter() workqueue.TypedRateLimiter[any] {
	return workqueue.NewTypedMaxOfRateLimiter[any](
		workqueue.NewTypedItemExponentialFailureRateLimiter[any](5*time.Millisecond, 1000*time.Second),
		// 100 qps, 1000 bucket size
		&workqueue.TypedBucketRateLimiter[any]{Limiter: rate.NewLimiter(rate.Limit(100), 1000)},
	)
}

// IsSameGuaranteedCPURequirement returns true if the resource requirement a is equal to the resource requirement b
func IsSameGuaranteedCPURequirement(a, b *corev1.ResourceRequirements) bool {
	var aQ, bQ resource.Quantity
	if a != nil && a.Requests != nil {
		aQ = a.Requests[corev1.ResourceCPU]
	}
	if b != nil && b.Requests != nil {
		bQ = b.Requests[corev1.ResourceCPU]
	}
	return (&aQ).Cmp(bQ) == 0
}
