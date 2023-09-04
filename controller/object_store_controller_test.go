package controller

import (
	"context"
	"fmt"
	"testing"

	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
	"github.com/sirupsen/logrus"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sinformers "k8s.io/client-go/informers"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	k8score "k8s.io/client-go/testing"
	"k8s.io/kubernetes/pkg/controller"
)

const (
	TestObjectStoreName    = "test-object-store"
	TestObjectStorePVName  = "pv-test-object-store"
	TestObjectStorePVCName = "pvc-test-object-store"
	TestObjectStoreImage   = "quay.io/s3gw/s3gw:latest"
	TestObjectStoreUIImage = "quay.io/s3gw/s3gw-ui:latest"

	TestObjectStoreControllerID = "test-objecte-store-controller"
)

var (
	TestObjectStoreSize = resource.MustParse("10Gi")
)

type fixture struct {
	test                 *testing.T
	kubeClient           *k8sfake.Clientset
	lhClient             *lhfake.Clientset
	objectStoreLister    []*longhorn.ObjectStore
	longhornVolumeLister []*longhorn.Volume
	pvcLister            []*corev1.PersistentVolumeClaim
	secretLister         []*corev1.Secret
	serviceLister        []*corev1.Service
	deploymentLister     []*appsv1.Deployment
	kubeActions          []k8score.Action
	lhActions            []k8score.Action
	kubeObjects          []runtime.Object
	lhObjects            []runtime.Object
}

func newFixture(t *testing.T) *fixture {
	return &fixture{
		test:        t,
		kubeObjects: []runtime.Object{},
		lhObjects:   []runtime.Object{},
	}
}

func osTestNewObjectStore() *longhorn.ObjectStore {
	return &longhorn.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name: TestObjectStoreName,
		},
		Spec: longhorn.ObjectStoreSpec{
			Storage: longhorn.ObjectStoreStorageSpec{
				Size: TestObjectStoreSize,
			},
			Credentials: longhorn.ObjectStoreCredentials{
				AccessKey: "foobar",
				SecretKey: "barfoo",
			},
		},
	}
}

func osTestNewPersistentVolumeClaim() *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestObjectStorePVCName,
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
			VolumeName:       TestObjectStorePVName,
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

func osTestNewSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestObjectStoreName,
			Namespace: TestNamespace,
		},
		StringData: map[string]string{},
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

func (f *fixture) newObjectStoreController(ctx *context.Context) (*ObjectStoreController, k8sinformers.SharedInformerFactory, lhinformers.SharedInformerFactory) {
	f.kubeClient = k8sfake.NewSimpleClientset()
	f.lhClient = lhfake.NewSimpleClientset()

	kubeInformerFactory := k8sinformers.NewSharedInformerFactory(
		f.kubeClient,
		controller.NoResyncPeriodFunc())
	lhInformerFactory := lhinformers.NewSharedInformerFactory(
		f.lhClient,
		controller.NoResyncPeriodFunc())
	extensionsClient := apiextensionsfake.NewSimpleClientset()

	logger := logrus.StandardLogger()
	logrus.SetLevel(logrus.DebugLevel)

	ds := datastore.NewDataStore(
		lhInformerFactory,
		f.lhClient,
		kubeInformerFactory,
		f.kubeClient,
		extensionsClient,
		TestNamespace)

	c := NewObjectStoreController(
		logger,
		ds,
		scheme.Scheme,
		f.kubeClient,
		TestObjectStoreControllerID,
		TestNamespace,
		TestObjectStoreImage,
		TestObjectStoreUIImage)

	for index := range c.cacheSyncs {
		c.cacheSyncs[index] = alwaysReady
	}

	for _, o := range f.objectStoreLister {
		lhInformerFactory.
			Longhorn().
			V1beta2().
			ObjectStores().
			Informer().
			GetIndexer().
			Add(o)
	}

	for _, v := range f.longhornVolumeLister {
		lhInformerFactory.
			Longhorn().
			V1beta2().
			Volumes().
			Informer().
			GetIndexer().
			Add(v)
	}

	for _, p := range f.pvcLister {
		kubeInformerFactory.
			Core().
			V1().
			PersistentVolumeClaims().
			Informer().
			GetIndexer().
			Add(p)
	}

	for _, s := range f.secretLister {
		kubeInformerFactory.
			Core().
			V1().
			Secrets().
			Informer().
			GetIndexer().
			Add(s)
	}

	for _, s := range f.serviceLister {
		kubeInformerFactory.
			Core().
			V1().
			Services().
			Informer().
			GetIndexer().
			Add(s)
	}

	for _, d := range f.deploymentLister {
		kubeInformerFactory.
			Apps().
			V1().
			Deployments().
			Informer().
			GetIndexer().
			Add(d)
	}

	return c, kubeInformerFactory, lhInformerFactory
}

func (f *fixture) runObjectStoreController(ctx *context.Context, key string) error {
	c, _, _ := f.newObjectStoreController(ctx)
	err := c.syncObjectStore(key)
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

	f.runExpectSuccess(&ctx, getMetaKey(TestObjectStoreName))
}

// TestSyncNewObjectStore tests the case where a new object endpoint is
// created that dossn't have any status property at all
func TestSyncNewObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	store := osTestNewObjectStore()
	vol := osTestNewLonghornVolume()

	f.lhObjects = append(f.lhObjects, store)
	f.lhObjects = append(f.lhObjects, vol)
	f.objectStoreLister = append(f.objectStoreLister, store)
	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)

	f.runExpectSuccess(&ctx, getMetaKey(TestObjectStoreName))
}

// TestSyncUnkonwObjectStore tests the default case of a new object endpoint
// where the status is already filled out by the kubeapi, but still contains the
// default value of "Unknown"
func TestSyncUnkonwObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	store := osTestNewObjectStore()
	(*store).Status = longhorn.ObjectStoreStatus{
		State:     longhorn.ObjectStoreStateUnknown,
		Endpoints: []string{},
	}
	vol := osTestNewLonghornVolume()

	f.lhObjects = append(f.lhObjects, store)
	f.lhObjects = append(f.lhObjects, vol)
	f.objectStoreLister = append(f.objectStoreLister, store)
	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)

	f.runExpectSuccess(&ctx, getMetaKey(TestObjectStoreName))
}

// TestSyncStartingObjectStore  tests the case where the object endpoint has
// already been seen by the controller and the resources should have been
// deployed
func TestSyncStartingObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	store := osTestNewObjectStore()
	(*store).Status = longhorn.ObjectStoreStatus{
		State:     longhorn.ObjectStoreStateStarting,
		Endpoints: []string{},
	}
	pvc := osTestNewPersistentVolumeClaim()
	vol := osTestNewLonghornVolume()
	deployment := osTestNewDeployment()
	// TODO: Create the other objects here too. This only succeeds because the
	// volume claim isn't in bound state, so the controller will return success
	// and wait

	f.lhObjects = append(f.lhObjects, store)
	f.kubeObjects = append(f.kubeObjects, pvc)
	f.lhObjects = append(f.lhObjects, vol)
	f.kubeObjects = append(f.kubeObjects, deployment)
	f.objectStoreLister = append(f.objectStoreLister, store)
	f.pvcLister = append(f.pvcLister, pvc)
	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
	f.deploymentLister = append(f.deploymentLister, deployment)

	f.runExpectSuccess(&ctx, getMetaKey(TestObjectStoreName))
}

// TestSyncRunningObjectStore tests the case where the object endpoint is
// already fully functional and the controller only needs to monitor it
func TestSyncRunningObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	store := osTestNewObjectStore()
	(*store).Status = longhorn.ObjectStoreStatus{
		State: longhorn.ObjectStoreStateRunning,
		Endpoints: []string{
			fmt.Sprintf("%s.%s.svc", TestObjectStoreName, TestNamespace),
		},
	}
	pvc := osTestNewPersistentVolumeClaim()
	vol := osTestNewLonghornVolume()
	secret := osTestNewSecret()
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

	f.runExpectSuccess(&ctx, getMetaKey(TestObjectStoreName))
}

// TestSyncStoppingObjectStore tests that the object endpoint has been marked
// for deletion and the controller needs to wait for the resources to be removed
// before it can remvos the finalizer and stop tracking the object endpoint
func TestSyncStoppingObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	store := osTestNewObjectStore()
	(*store).Status = longhorn.ObjectStoreStatus{
		State: longhorn.ObjectStoreStateStopping,
		Endpoints: []string{
			fmt.Sprintf("%s.%s.svc", TestObjectStoreName, TestNamespace),
		},
	}

	f.lhObjects = append(f.lhObjects, store)
	f.objectStoreLister = append(f.objectStoreLister, store)

	f.runExpectSuccess(&ctx, getMetaKey(TestObjectStoreName))
}

// TestSyncErrorObjectStore tests the case where the objecte endpoint is in
// error state
func TestSyncErrorObjectStore(t *testing.T) {
	f := newFixture(t)
	ctx := context.TODO()

	store := osTestNewObjectStore()
	(*store).Status = longhorn.ObjectStoreStatus{
		State:     longhorn.ObjectStoreStateStarting,
		Endpoints: []string{},
	}
	pvc := osTestNewPersistentVolumeClaim()
	vol := osTestNewLonghornVolume()
	deployment := osTestNewDeployment()
	// TODO: Create the other objects here too. This only succeeds because the
	// volume claim isn't in bound state, so the controller will return success
	// and wait

	f.lhObjects = append(f.lhObjects, store)
	f.kubeObjects = append(f.kubeObjects, pvc)
	f.lhObjects = append(f.lhObjects, vol)
	f.kubeObjects = append(f.kubeObjects, deployment)
	f.objectStoreLister = append(f.objectStoreLister, store)
	f.pvcLister = append(f.pvcLister, pvc)
	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
	f.deploymentLister = append(f.deploymentLister, deployment)

	f.runExpectSuccess(&ctx, getMetaKey(TestObjectStoreName))
}

// --- Helper Functions ---

func getMetaKey(name string) string { return name }
