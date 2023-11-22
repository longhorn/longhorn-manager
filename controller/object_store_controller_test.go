package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	k8score "k8s.io/client-go/testing"
	"k8s.io/kubernetes/pkg/controller"
)

const (
	TestObjectStoreName       = "test-object-store"
	TestObjectStoreSecretName = "test-secret"
	TestObjectStoreImage      = "quay.io/s3gw/s3gw:latest"
	TestObjectStoreUIImage    = "quay.io/s3gw/s3gw-ui:latest"

	TestObjectStoreControllerID = "test-objecte-store-controller"
)

var (
	TestObjectStoreSize = resource.MustParse("10Gi")
)

type fixture struct {
	test *testing.T

	kubeClient       *k8sfake.Clientset
	lhClient         *lhfake.Clientset
	extensionsClient *apiextensionsfake.Clientset

	objectStoreLister    []*longhorn.ObjectStore
	longhornVolumeLister []*longhorn.Volume
	pvcLister            []*corev1.PersistentVolumeClaim
	secretLister         []*corev1.Secret
	serviceLister        []*corev1.Service
	ingressLister        []*networkingv1.Ingress
	deploymentLister     []*appsv1.Deployment

	kubeActions       []k8score.Action
	lhActions         []k8score.Action
	extensionsActions []k8score.Action

	kubeObjects       []runtime.Object
	lhObjects         []runtime.Object
	extensionsObjects []runtime.Object
}

func newFixture(t *testing.T) *fixture {
	return &fixture{
		test:              t,
		kubeObjects:       []runtime.Object{},
		lhObjects:         []runtime.Object{},
		extensionsObjects: []runtime.Object{},
	}
}

func osTestNewObjectStore(secret *corev1.Secret) *longhorn.ObjectStore {
	return &longhorn.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestObjectStoreName,
			Namespace: TestNamespace,
		},
		Spec: longhorn.ObjectStoreSpec{
			Size:             TestObjectStoreSize,
			VolumeParameters: longhorn.ObjectStoreVolumeParameterSpec{},
			Credentials: corev1.SecretReference{
				Name:      secret.Name,
				Namespace: secret.Namespace,
			},
			Image:       TestObjectStoreImage,
			UIImage:     TestObjectStoreUIImage,
			TargetState: "running",
		},
	}
}

func osTestNewSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestObjectStoreSecretName,
			Namespace: TestNamespace,
		},
		StringData: map[string]string{
			"RGW_DEFAULT_USER_ACCESS_KEY": "foobar",
			"RGW_DEFAULT_USER_SECRET_KEY": "barfoo",
		},
	}
}

func osTestNewPersistentVolumeClaim() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestObjectStoreName,
			Namespace: TestNamespace,
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes: []corev1.PersistentVolumeAccessMode{
				corev1.ReadWriteOnce,
			},
			Resources: corev1.ResourceRequirements{
				Requests: map[corev1.ResourceName]resource.Quantity{
					corev1.ResourceStorage: TestObjectStoreSize,
				},
			},
			StorageClassName: func() *string { s := ""; return &s }(),
			VolumeName:       TestObjectStoreName,
		},
	}
}

func osTestNewLonghornVolume() *longhorn.Volume {
	return &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestObjectStoreName,
			Namespace: TestNamespace,
		},
		Spec: longhorn.VolumeSpec{
			Size:       func() int64 { s, _ := TestObjectStoreSize.AsInt64(); return s }(),
			Frontend:   longhorn.VolumeFrontendBlockDev,
			AccessMode: longhorn.AccessModeReadWriteOnce,
		},
	}
}

func osTestNewService() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestObjectStoreName,
			Namespace: TestNamespace,
		},
		Spec: corev1.ServiceSpec{},
	}
}

func osTestNewDeployment() *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestObjectStoreName,
			Namespace: TestNamespace,
		},
		Spec: appsv1.DeploymentSpec{},
	}
}

func (f *fixture) newObjectStoreController(ctx *context.Context) *ObjectStoreController {
	f.kubeClient = k8sfake.NewSimpleClientset()
	f.lhClient = lhfake.NewSimpleClientset()
	f.extensionsClient = apiextensionsfake.NewSimpleClientset()

	informerFactories := util.NewInformerFactories(
		TestNamespace,
		f.kubeClient,
		f.lhClient,
		controller.NoResyncPeriodFunc())

	logger := logrus.StandardLogger()
	logrus.SetLevel(logrus.DebugLevel)

	ds := datastore.NewDataStore(TestNamespace, f.lhClient, f.kubeClient, f.extensionsClient, informerFactories)

	c := NewObjectStoreController(
		logger,
		ds,
		scheme.Scheme,
		f.kubeClient,
		TestObjectStoreControllerID,
		TestNamespace,
	)

	for index := range c.cacheSyncs {
		c.cacheSyncs[index] = alwaysReady
	}

	for _, o := range f.objectStoreLister {
		f.lhClient.
			LonghornV1beta2().
			ObjectStores(TestNamespace).
			Create(context.TODO(), o, metav1.CreateOptions{})
		informerFactories.LhInformerFactory.
			Longhorn().
			V1beta2().
			ObjectStores().
			Informer().
			GetIndexer().
			Add(o)
	}

	for _, v := range f.longhornVolumeLister {
		f.lhClient.
			LonghornV1beta2().
			Volumes(TestNamespace).
			Create(context.TODO(), v, metav1.CreateOptions{})
		informerFactories.LhInformerFactory.
			Longhorn().
			V1beta2().
			Volumes().
			Informer().
			GetIndexer().
			Add(v)
	}

	for _, p := range f.pvcLister {
		f.kubeClient.
			CoreV1().
			PersistentVolumeClaims(TestNamespace).
			Create(context.TODO(), p, metav1.CreateOptions{})
		informerFactories.KubeNamespaceFilteredInformerFactory.
			Core().
			V1().
			PersistentVolumeClaims().
			Informer().
			GetIndexer().
			Add(p)
	}

	for _, s := range f.secretLister {
		f.kubeClient.
			CoreV1().
			Secrets(TestNamespace).
			Create(context.TODO(), s, metav1.CreateOptions{})
		informerFactories.KubeNamespaceFilteredInformerFactory.
			Core().
			V1().
			Secrets().
			Informer().
			GetIndexer().
			Add(s)
	}

	for _, s := range f.serviceLister {
		f.kubeClient.
			CoreV1().
			Services(TestNamespace).
			Create(context.TODO(), s, metav1.CreateOptions{})
		informerFactories.KubeNamespaceFilteredInformerFactory.
			Core().
			V1().
			Services().
			Informer().
			GetIndexer().
			Add(s)
	}

	for _, i := range f.ingressLister {
		f.kubeClient.
			NetworkingV1().
			Ingresses(TestNamespace).
			Create(context.TODO(), i, metav1.CreateOptions{})
		informerFactories.KubeNamespaceFilteredInformerFactory.
			Networking().
			V1().
			Ingresses().
			Informer().
			GetIndexer().
			Add(i)
	}

	for _, d := range f.deploymentLister {
		f.kubeClient.
			AppsV1().
			Deployments(TestNamespace).
			Create(context.TODO(), d, metav1.CreateOptions{})
		informerFactories.KubeNamespaceFilteredInformerFactory.
			Apps().
			V1().
			Deployments().
			Informer().
			GetIndexer().
			Add(d)
	}

	return c
}

func (f *fixture) runObjectStoreController(ctx *context.Context, key string) error {
	c := f.newObjectStoreController(ctx)
	err := c.reconcile(key)
	return err
}

func (f *fixture) runExpectSuccess(ctx *context.Context, key string) {
	err := f.runObjectStoreController(ctx, key)
	if err != nil {
		f.test.Errorf("%v", err)
	}
}

func (f *fixture) runExpectFailure(ctx *context.Context, key string) {
	err := f.runObjectStoreController(ctx, key)
	if err == nil {
		f.test.Errorf("%v", err)
	}
}

// TestSyncNonexistentObjectStore tests that the object endpoint controller
// gracefully handles the case where the object endpoint doss not exist
func TestSyncNonexistentObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	f.runExpectSuccess(&ctx, getMetaKey(TestNamespace, TestObjectStoreName))
}

// TestSyncNewObjectStore tests the case where a new object endpoint is
// created that dossn't have any status property at all
func TestSyncNewObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	secret := osTestNewSecret()
	store := osTestNewObjectStore(secret)

	f.lhObjects = append(f.lhObjects, store)
	f.objectStoreLister = append(f.objectStoreLister, store)

	f.runExpectSuccess(&ctx, getMetaKey(TestNamespace, TestObjectStoreName))

	check, _ := f.lhClient.
		LonghornV1beta2().
		ObjectStores(TestNamespace).
		Get(context.TODO(), TestObjectStoreName, metav1.GetOptions{})

	if check.Spec.Image != TestObjectStoreImage {
		f.test.Errorf("%v != %v", (*store).Spec.Image, TestObjectStoreImage)
	}

	if check.Spec.UIImage != TestObjectStoreUIImage {
		f.test.Errorf("%v != %v", (*store).Spec.UIImage, TestObjectStoreUIImage)
	}

	if check.Spec.TargetState != longhorn.ObjectStoreStateRunning {
		f.test.Errorf("%v != %v",
			(*store).Spec.TargetState,
			longhorn.ObjectStoreStateRunning)
	}

	if check.Status.State != longhorn.ObjectStoreStateStarting {
		f.test.Errorf("%v != %v",
			(*store).Status.State,
			longhorn.ObjectStoreStateStarting)
	}
}

// TestSyncUnkonwObjectStore tests the default case of a new object endpoint
// where the status is already filled out by the kubeapi, but still contains the
// default value of "Unknown"
func TestSyncUnkonwObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	secret := osTestNewSecret()
	store := osTestNewObjectStore(secret)
	(*store).Status = longhorn.ObjectStoreStatus{
		State:     longhorn.ObjectStoreStateUnknown,
		Endpoints: []string{},
	}

	f.lhObjects = append(f.lhObjects, store)
	f.objectStoreLister = append(f.objectStoreLister, store)

	f.runExpectSuccess(&ctx, getMetaKey(TestNamespace, TestObjectStoreName))

	check, _ := f.lhClient.
		LonghornV1beta2().
		ObjectStores(TestNamespace).
		Get(context.TODO(), TestObjectStoreName, metav1.GetOptions{})

	if check.Spec.Image != TestObjectStoreImage {
		f.test.Errorf("%v != %v", (*store).Spec.Image, TestObjectStoreImage)
	}

	if check.Spec.UIImage != TestObjectStoreUIImage {
		f.test.Errorf("%v != %v", (*store).Spec.UIImage, TestObjectStoreUIImage)
	}

	if check.Spec.TargetState != longhorn.ObjectStoreStateRunning {
		f.test.Errorf("%v != %v",
			(*store).Spec.TargetState,
			longhorn.ObjectStoreStateRunning)
	}

	if check.Status.State != longhorn.ObjectStoreStateStarting {
		f.test.Errorf("%v != %v",
			(*store).Status.State,
			longhorn.ObjectStoreStateStarting)
	}
}

// TestSyncStartingObjectStore  tests the case where the object endpoint has
// already been seen by the controller and the resources should have been
// deployed
func TestSyncStartingObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	secret := osTestNewSecret()
	store := osTestNewObjectStore(secret)
	(*store).Status = longhorn.ObjectStoreStatus{
		State:     longhorn.ObjectStoreStateStarting,
		Endpoints: []string{},
	}
	pvc := osTestNewPersistentVolumeClaim()
	vol := osTestNewLonghornVolume()
	deployment := osTestNewDeployment()

	f.lhObjects = append(f.lhObjects, store)
	f.kubeObjects = append(f.kubeObjects, pvc)
	f.lhObjects = append(f.lhObjects, vol)
	f.kubeObjects = append(f.kubeObjects, deployment)
	f.objectStoreLister = append(f.objectStoreLister, store)
	f.pvcLister = append(f.pvcLister, pvc)
	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
	f.deploymentLister = append(f.deploymentLister, deployment)

	f.runExpectSuccess(&ctx, getMetaKey(TestNamespace, TestObjectStoreName))
}

// TestSyncRunningObjectStore tests the case where the object endpoint is
// already fully functional and the controller only needs to monitor it
func TestSyncRunningObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	secret := osTestNewSecret()
	store := osTestNewObjectStore(secret)
	(*store).Status = longhorn.ObjectStoreStatus{
		State: longhorn.ObjectStoreStateRunning,
		Endpoints: []string{
			fmt.Sprintf("%s.%s.svc", TestObjectStoreName, TestNamespace),
		},
	}
	pvc := osTestNewPersistentVolumeClaim()
	vol := osTestNewLonghornVolume()
	service := osTestNewService()
	deployment := osTestNewDeployment()

	f.lhObjects = append(f.lhObjects, store)
	f.kubeObjects = append(f.kubeObjects, pvc)
	f.lhObjects = append(f.lhObjects, vol)
	f.kubeObjects = append(f.kubeObjects, secret)
	f.kubeObjects = append(f.kubeObjects, service)
	f.kubeObjects = append(f.kubeObjects, deployment)
	f.objectStoreLister = append(f.objectStoreLister, store)
	f.pvcLister = append(f.pvcLister, pvc)
	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
	f.secretLister = append(f.secretLister, secret)
	f.serviceLister = append(f.serviceLister, service)
	f.deploymentLister = append(f.deploymentLister, deployment)

	f.runExpectSuccess(&ctx, getMetaKey(TestNamespace, TestObjectStoreName))
}

// TestSyncStoppingObjectStore
func TestSyncStoppingObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	secret := osTestNewSecret()
	store := osTestNewObjectStore(secret)
	(*store).Status = longhorn.ObjectStoreStatus{
		State: longhorn.ObjectStoreStateStopping,
		Endpoints: []string{
			fmt.Sprintf("%s.%s.svc", TestObjectStoreName, TestNamespace),
		},
	}
	pvc := osTestNewPersistentVolumeClaim()
	vol := osTestNewLonghornVolume()
	service := osTestNewService()
	deployment := osTestNewDeployment()
	(*deployment).Spec.Replicas = func() *int32 { a := int32(1); return &a }()

	f.lhObjects = append(f.lhObjects, store)
	f.kubeObjects = append(f.kubeObjects, pvc)
	f.lhObjects = append(f.lhObjects, vol)
	f.kubeObjects = append(f.kubeObjects, secret)
	f.kubeObjects = append(f.kubeObjects, service)
	f.kubeObjects = append(f.kubeObjects, deployment)
	f.objectStoreLister = append(f.objectStoreLister, store)
	f.pvcLister = append(f.pvcLister, pvc)
	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
	f.secretLister = append(f.secretLister, secret)
	f.serviceLister = append(f.serviceLister, service)
	f.deploymentLister = append(f.deploymentLister, deployment)

	f.runExpectSuccess(&ctx, getMetaKey(TestNamespace, TestObjectStoreName))
}

// TestSyncStoppedObjectStore
func TestSyncStoppedObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	secret := osTestNewSecret()
	store := osTestNewObjectStore(secret)
	(*store).Status = longhorn.ObjectStoreStatus{
		State: longhorn.ObjectStoreStateStopped,
		Endpoints: []string{
			fmt.Sprintf("%s.%s.svc", TestObjectStoreName, TestNamespace),
		},
	}
	pvc := osTestNewPersistentVolumeClaim()
	vol := osTestNewLonghornVolume()
	service := osTestNewService()
	deployment := osTestNewDeployment()
	(*deployment).Spec.Replicas = func() *int32 { a := int32(0); return &a }()

	f.lhObjects = append(f.lhObjects, store)
	f.kubeObjects = append(f.kubeObjects, pvc)
	f.lhObjects = append(f.lhObjects, vol)
	f.kubeObjects = append(f.kubeObjects, secret)
	f.kubeObjects = append(f.kubeObjects, service)
	f.kubeObjects = append(f.kubeObjects, deployment)
	f.objectStoreLister = append(f.objectStoreLister, store)
	f.pvcLister = append(f.pvcLister, pvc)
	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
	f.secretLister = append(f.secretLister, secret)
	f.serviceLister = append(f.serviceLister, service)
	f.deploymentLister = append(f.deploymentLister, deployment)

	f.runExpectSuccess(&ctx, getMetaKey(TestNamespace, TestObjectStoreName))
}

// TestSyncTerminatingObjectStore tests that the object endpoint has been marked
// for suspension and the controller needs to wait for the deployment to scale
// down
func TestSyncTerminatingObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	secret := osTestNewSecret()
	store := osTestNewObjectStore(secret)
	(*store).Status = longhorn.ObjectStoreStatus{
		State: longhorn.ObjectStoreStateStopping,
		Endpoints: []string{
			fmt.Sprintf("%s.%s.svc", TestObjectStoreName, TestNamespace),
		},
	}
	pvc := osTestNewPersistentVolumeClaim()
	vol := osTestNewLonghornVolume()
	service := osTestNewService()
	deployment := osTestNewDeployment()

	f.lhObjects = append(f.lhObjects, store)
	f.kubeObjects = append(f.kubeObjects, pvc)
	f.lhObjects = append(f.lhObjects, vol)
	f.kubeObjects = append(f.kubeObjects, secret)
	f.kubeObjects = append(f.kubeObjects, service)
	f.kubeObjects = append(f.kubeObjects, deployment)
	f.objectStoreLister = append(f.objectStoreLister, store)
	f.pvcLister = append(f.pvcLister, pvc)
	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
	f.secretLister = append(f.secretLister, secret)
	f.serviceLister = append(f.serviceLister, service)
	f.deploymentLister = append(f.deploymentLister, deployment)

	f.runExpectSuccess(&ctx, getMetaKey(TestNamespace, TestObjectStoreName))
}

// TestSyncErrorObjectStore tests the case where the objecte endpoint is in
// error state
func TestSyncErrorObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	secret := osTestNewSecret()
	store := osTestNewObjectStore(secret)
	(*store).Status = longhorn.ObjectStoreStatus{
		State:     longhorn.ObjectStoreStateError,
		Endpoints: []string{},
	}
	pvc := osTestNewPersistentVolumeClaim()
	vol := osTestNewLonghornVolume()
	service := osTestNewService()
	deployment := osTestNewDeployment()

	f.lhObjects = append(f.lhObjects, store)
	f.kubeObjects = append(f.kubeObjects, pvc)
	f.lhObjects = append(f.lhObjects, vol)
	f.kubeObjects = append(f.kubeObjects, secret)
	f.kubeObjects = append(f.kubeObjects, service)
	f.kubeObjects = append(f.kubeObjects, deployment)
	f.objectStoreLister = append(f.objectStoreLister, store)
	f.pvcLister = append(f.pvcLister, pvc)
	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
	f.secretLister = append(f.secretLister, secret)
	f.serviceLister = append(f.serviceLister, service)
	f.deploymentLister = append(f.deploymentLister, deployment)

	f.runExpectSuccess(&ctx, getMetaKey(TestNamespace, TestObjectStoreName))
}

// --- Helper Functions ---

func getMetaKey(namespace, name string) string { return fmt.Sprintf("%v/%v", namespace, name) }
