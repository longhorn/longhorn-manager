package controller

import (
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/time/rate"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
)

var (
	Workers              = 5
	longhornFinalizerKey = longhorn.SchemeGroupVersion.Group
)

func StartControllers(stopCh chan struct{}, controllerID, serviceAccount, managerImage, kubeconfigPath, version string) (*datastore.DataStore, *WebsocketController, error) {
	namespace := os.Getenv(types.EnvPodNamespace)
	if namespace == "" {
		logrus.Warnf("Cannot detect pod namespace, environment variable %v is missing, "+
			"using default namespace", types.EnvPodNamespace)
		namespace = corev1.NamespaceDefault
	}

	config, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to get client config")
	}

	kubeClient, err := clientset.NewForConfig(config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to get k8s client")
	}

	lhClient, err := lhclientset.NewForConfig(config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "unable to get clientset")
	}

	scheme := runtime.NewScheme()
	if err := longhorn.SchemeBuilder.AddToScheme(scheme); err != nil {
		return nil, nil, errors.Wrap(err, "unable to create scheme")
	}

	kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, time.Second*30)
	lhInformerFactory := lhinformers.NewSharedInformerFactory(lhClient, time.Second*30)

	replicaInformer := lhInformerFactory.Longhorn().V1beta1().Replicas()
	engineInformer := lhInformerFactory.Longhorn().V1beta1().Engines()
	volumeInformer := lhInformerFactory.Longhorn().V1beta1().Volumes()
	engineImageInformer := lhInformerFactory.Longhorn().V1beta1().EngineImages()
	nodeInformer := lhInformerFactory.Longhorn().V1beta1().Nodes()
	settingInformer := lhInformerFactory.Longhorn().V1beta1().Settings()
	imInformer := lhInformerFactory.Longhorn().V1beta1().InstanceManagers()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	kubeNodeInformer := kubeInformerFactory.Core().V1().Nodes()
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1().DaemonSets()
	deploymentInformer := kubeInformerFactory.Apps().V1().Deployments()
	volumeAttachmentInformer := kubeInformerFactory.Storage().V1beta1().VolumeAttachments()

	ds := datastore.NewDataStore(
		volumeInformer, engineInformer, replicaInformer,
		engineImageInformer, nodeInformer, settingInformer, imInformer,
		lhClient,
		podInformer, cronJobInformer, daemonSetInformer, deploymentInformer,
		persistentVolumeInformer, persistentVolumeClaimInformer, kubeNodeInformer,
		kubeClient, namespace)
	rc := NewReplicaController(ds, scheme,
		replicaInformer, imInformer,
		kubeClient, namespace, controllerID)
	ec := NewEngineController(ds, scheme,
		engineInformer, imInformer,
		kubeClient, &engineapi.EngineCollection{}, namespace, controllerID)
	vc := NewVolumeController(ds, scheme,
		volumeInformer, engineInformer, replicaInformer,
		kubeClient, namespace, controllerID,
		serviceAccount, managerImage)
	ic := NewEngineImageController(ds, scheme,
		engineImageInformer, volumeInformer, daemonSetInformer,
		kubeClient, namespace, controllerID, serviceAccount)
	nc := NewNodeController(ds, scheme,
		nodeInformer, settingInformer, podInformer, replicaInformer, kubeNodeInformer,
		kubeClient, namespace, controllerID)
	ws := NewWebsocketController(volumeInformer, engineInformer, replicaInformer,
		settingInformer, engineImageInformer, nodeInformer)
	sc := NewSettingController(ds, scheme,
		settingInformer,
		kubeClient, version)
	imc := NewInstanceManagerController(ds, scheme, imInformer, podInformer, kubeClient, namespace, controllerID, serviceAccount)

	kpvc := NewKubernetesPVController(ds, scheme, volumeInformer, persistentVolumeInformer,
		persistentVolumeClaimInformer, podInformer, volumeAttachmentInformer, kubeClient, controllerID)
	knc := NewKubernetesNodeController(ds, scheme, nodeInformer, settingInformer, kubeNodeInformer,
		kubeClient, controllerID)

	go kubeInformerFactory.Start(stopCh)
	go lhInformerFactory.Start(stopCh)
	if !ds.Sync(stopCh) {
		return nil, nil, fmt.Errorf("datastore cache sync up failed")
	}
	go rc.Run(Workers, stopCh)
	go ec.Run(Workers, stopCh)
	go vc.Run(Workers, stopCh)
	go ic.Run(Workers, stopCh)
	go nc.Run(Workers, stopCh)
	go ws.Run(stopCh)
	go sc.Run(stopCh)
	go imc.Run(Workers, stopCh)

	go kpvc.Run(Workers, stopCh)
	go knc.Run(Workers, stopCh)

	return ds, ws, nil
}

func GetGuaranteedResourceRequirement(ds *datastore.DataStore) (*corev1.ResourceRequirements, error) {
	guaranteedCPU, err := ds.GetSetting(types.SettingNameGuaranteedEngineCPU)
	if err != nil {
		return nil, err
	}
	quantity, err := resource.ParseQuantity(guaranteedCPU.Value)
	if err != nil {
		return nil, err
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

func isControllerResponsibleFor(controllerID string, ds *datastore.DataStore, name, preferredOwnerID, currentOwnerID string) bool {
	var err error
	responsible := false

	ownerDown := false
	if currentOwnerID != "" {
		ownerDown, err = ds.IsNodeDownOrDeleted(currentOwnerID)
		if err != nil {
			logrus.Warnf("Error while checking if object %v owner is down or deleted: %v", name, err)
		}
	}

	if controllerID == preferredOwnerID {
		responsible = true
	} else if currentOwnerID == "" {
		responsible = true
	} else { // currentOwnerID != ""
		if ownerDown {
			responsible = true
		}
	}
	return responsible
}

// EnhancedDefaultControllerRateLimiter is an enhanced version of workqueue.DefaultControllerRateLimiter()
// See https://github.com/longhorn/longhorn/issues/1058 for details
func EnhancedDefaultControllerRateLimiter() workqueue.RateLimiter {
	return workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
		// 100 qps, 1000 bucket size
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(100), 1000)},
	)
}

func IsSameGuaranteedCPURequirement(a, b *corev1.ResourceRequirements) bool {
	if (a == nil) != (b == nil) {
		return false
	}
	if a == nil { // means b == nil
		return true
	}
	if (a.Requests == nil) != (b.Requests == nil) {
		return false
	}
	if a.Requests == nil { // means b.Requests == nil
		return true
	}
	requestA := a.Requests[corev1.ResourceCPU]
	return (&requestA).Cmp(b.Requests[corev1.ResourceCPU]) == 0
}
