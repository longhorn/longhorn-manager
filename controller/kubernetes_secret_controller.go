package controller

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	clientset "k8s.io/client-go/kubernetes"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

type KubernetesSecretController struct {
	*baseController

	// use as the OwnerID of the controller
	namespace    string
	controllerID string

	kubeClient    clientset.Interface
	eventRecorder record.EventRecorder

	ds *datastore.DataStore

	secretSynced cache.InformerSynced
}

func NewKubernetesSecretController(
	logger logrus.FieldLogger,
	ds *datastore.DataStore,
	scheme *runtime.Scheme,
	secretInformer coreinformers.SecretInformer,
	kubeClient clientset.Interface,
	controllerID string,
	namespace string) *KubernetesSecretController {
	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartLogging(logrus.Infof)
	// TODO: remove the wrapper when every clients have moved to use the clientset.
	eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{
		Interface: v1core.New(kubeClient.CoreV1().RESTClient()).Events(""),
	})

	ks := &KubernetesSecretController{
		baseController: newBaseController("longhorn-kubernetes-secret-controller", logger),

		namespace:    namespace,
		controllerID: controllerID,

		ds: ds,

		kubeClient:    kubeClient,
		eventRecorder: eventBroadcaster.NewRecorder(scheme, v1.EventSource{Component: "longhorn-kubernetes-secret-controller"}),

		secretSynced: secretInformer.Informer().HasSynced,
	}

	secretInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ks.enqueueSecretChange,
		UpdateFunc: func(old, cur interface{}) { ks.enqueueSecretChange(cur) },
		DeleteFunc: ks.enqueueSecretChange,
	})

	return ks
}

func (ks *KubernetesSecretController) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer ks.queue.ShutDown()

	ks.logger.Infof("Start")
	defer ks.logger.Infof("Shutting down")

	if !cache.WaitForNamedCacheSync(ks.name, stopCh, ks.secretSynced) {
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(ks.worker, time.Second, stopCh)
	}
	<-stopCh
}

func (ks *KubernetesSecretController) worker() {
	for ks.processNextWorkItem() {
	}
}

func (ks *KubernetesSecretController) processNextWorkItem() bool {
	key, quit := ks.queue.Get()
	if quit {
		return false
	}
	defer ks.queue.Done(key)
	err := ks.syncHandler(key.(string))
	ks.handleErr(err, key)
	return true
}

func (ks *KubernetesSecretController) handleErr(err error, key interface{}) {
	if err == nil {
		ks.queue.Forget(key)
		return
	}

	if ks.queue.NumRequeues(key) < maxRetries {
		ks.logger.WithError(err).Warnf("Error syncing Secret %v", key)
		ks.queue.AddRateLimited(key)
		return
	}

	ks.logger.WithError(err).Warnf("Dropping Secret %v out of the queue", key)
	ks.queue.Forget(key)
	utilruntime.HandleError(err)
}

func (ks *KubernetesSecretController) syncHandler(key string) (err error) {
	defer func() {
		err = errors.Wrapf(err, "%v: fail to sync %v", ks.name, key)
	}()

	namespace, secretName, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}

	if err := ks.reconcileSecret(namespace, secretName); err != nil {
		return err
	}
	return nil
}

func (ks *KubernetesSecretController) reconcileSecret(namespace, secretName string) error {
	if namespace != ks.namespace {
		// Not ours, skip it
		return nil
	}

	backupTarget, err := ks.ds.GetSettingValueExisted(types.SettingNameBackupTarget)
	if err != nil {
		// The backup target does not exist, skip it
		return nil
	}
	backupType, err := util.CheckBackupType(backupTarget)
	if err != nil {
		// Invalid backup target, skip it
		return nil
	}
	if backupType != types.BackupStoreTypeS3 {
		// We only focus on backup target S3, skip it
		return nil
	}

	sn, err := ks.ds.GetSettingValueExisted(types.SettingNameBackupTargetCredentialSecret)
	if err != nil {
		// The backup target credential secret does not exist, skip it
		return nil
	}
	if sn != secretName {
		// Not ours, skip it
		return nil
	}

	secret, err := ks.ds.GetSecretRO(namespace, secretName)
	if err != nil {
		return err
	}

	// Annotates AWS IAM role arn to the manager as well as the replica instance managers
	awsIAMRoleArn := string(secret.Data[types.AWSIAMRoleArn])
	return ks.annotateAWSIAMRoleArn(awsIAMRoleArn)
}

func (ks *KubernetesSecretController) enqueueSecretChange(obj interface{}) {
	key, err := controller.KeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %#v: %v", obj, err))
		return
	}

	ks.queue.Add(key)
}

// annotateAWSIAMRoleArn ensures that the running pods of the manager as well as the replica instance managers.
// have the correct AWS IAM role arn assigned to them based on the passed `awsIAMRoleArn`
func (ks *KubernetesSecretController) annotateAWSIAMRoleArn(awsIAMRoleArn string) error {
	managerPods, err := ks.ds.ListManagerPods()
	if err != nil {
		return err
	}

	imPods, err := ks.ds.ListInstanceManagerPodsBy(ks.controllerID, "", types.InstanceManagerTypeReplica)
	if err != nil {
		return err
	}

	pods := append(managerPods, imPods...)
	for _, pod := range pods {
		if pod.Spec.NodeName != ks.controllerID {
			continue
		}

		val, exist := pod.Annotations[types.AWSIAMRoleAnnotation]
		updateAnnotation := awsIAMRoleArn != "" && awsIAMRoleArn != val
		deleteAnnotation := awsIAMRoleArn == "" && exist
		if updateAnnotation {
			if pod.Annotations == nil {
				pod.Annotations = make(map[string]string)
			}
			pod.Annotations[types.AWSIAMRoleAnnotation] = awsIAMRoleArn
		} else if deleteAnnotation {
			delete(pod.Annotations, types.AWSIAMRoleAnnotation)
		} else {
			continue
		}

		if _, err = ks.kubeClient.CoreV1().Pods(pod.Namespace).Update(pod); err != nil {
			return err
		}

		ks.logger.Infof("AWS IAM role for pod %v/%v updated", pod.Namespace, pod.Name)
	}

	return nil
}
