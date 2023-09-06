package controller

//
//import (
//	"context"
//	"fmt"
//	"testing"
//
//	"github.com/longhorn/longhorn-manager/datastore"
//	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
//	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
//	lhinformers "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"
//	"github.com/sirupsen/logrus"
//	appsv1 "k8s.io/api/apps/v1"
//	corev1 "k8s.io/api/core/v1"
//	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
//	"k8s.io/apimachinery/pkg/api/resource"
//	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
//	"k8s.io/apimachinery/pkg/runtime"
//	k8sinformers "k8s.io/client-go/informers"
//	k8sfake "k8s.io/client-go/kubernetes/fake"
//	"k8s.io/client-go/kubernetes/scheme"
//	k8score "k8s.io/client-go/testing"
//	"k8s.io/kubernetes/pkg/controller"
//)
//
//const (
//	TestObjectEndpointName    = "test-object-endpoint"
//	TestObjectEndpointPVName  = "pv-test-object-endpoint"
//	TestObjectEndpointPVCName = "pvc-test-object-endpoint"
//	TestObjectEndpointImage   = "quay.io/s3gw/s3gw:latest"
//	TestObjectEndpointUIImage = "quay.io/s3gw/s3gw-ui:latest"
//
//	TestObjectEndpointControllerID = "test-objecte-endpoint-controller"
//)
//
//var (
//	TestObjectEndpointSize = resource.MustParse("10Gi")
//)
//
//type fixture struct {
//	test                 *testing.T
//	kubeClient           *k8sfake.Clientset
//	lhClient             *lhfake.Clientset
//	objectEndpointLister []*longhorn.ObjectEndpoint
//	longhornVolumeLister []*longhorn.Volume
//	pvcLister            []*corev1.PersistentVolumeClaim
//	secretLister         []*corev1.Secret
//	serviceLister        []*corev1.Service
//	deploymentLister     []*appsv1.Deployment
//	kubeActions          []k8score.Action
//	lhActions            []k8score.Action
//	kubeObjects          []runtime.Object
//	lhObjects            []runtime.Object
//}
//
//func newFixture(t *testing.T) *fixture {
//	return &fixture{
//		test:        t,
//		kubeObjects: []runtime.Object{},
//		lhObjects:   []runtime.Object{},
//	}
//}
//
//func oeTestNewObjectEndpoint() *longhorn.ObjectEndpoint {
//	return &longhorn.ObjectEndpoint{
//		ObjectMeta: metav1.ObjectMeta{
//			Name: TestObjectEndpointName,
//		},
//		Spec: longhorn.ObjectEndpointSpec{
//			Size:         TestObjectEndpointSize,
//			StorageClass: "longhorn",
//			Credentials: longhorn.ObjectEndpointCredentials{
//				AccessKey: "foobar",
//				SecretKey: "barfoo",
//			},
//		},
//	}
//}
//
//func oeTestNewPersistentVolumeClaim() *corev1.PersistentVolumeClaim {
//	return &corev1.PersistentVolumeClaim{
//		ObjectMeta: metav1.ObjectMeta{
//			Name:      TestObjectEndpointPVCName,
//			Namespace: TestNamespace,
//		},
//		Spec: corev1.PersistentVolumeClaimSpec{
//			AccessModes: []corev1.PersistentVolumeAccessMode{
//				corev1.ReadWriteOnce,
//			},
//			Resources: corev1.ResourceRequirements{
//				Requests: map[corev1.ResourceName]resource.Quantity{
//					corev1.ResourceStorage: TestObjectEndpointSize,
//				},
//			},
//			StorageClassName: func() *string { s := ""; return &s }(),
//			VolumeName:       TestObjectEndpointPVName,
//		},
//	}
//}
//
//func oeTestNewLonghornVolume() *longhorn.Volume {
//	return &longhorn.Volume{
//		ObjectMeta: metav1.ObjectMeta{
//			Name:      TestObjectEndpointName,
//			Namespace: TestNamespace,
//		},
//		Spec: longhorn.VolumeSpec{
//			Size:       func() int64 { s, _ := TestObjectEndpointSize.AsInt64(); return s }(),
//			Frontend:   longhorn.VolumeFrontendBlockDev,
//			AccessMode: longhorn.AccessModeReadWriteOnce,
//		},
//	}
//}
//
//func oeTestNewSecret() *corev1.Secret {
//	return &corev1.Secret{
//		ObjectMeta: metav1.ObjectMeta{
//			Name:      TestObjectEndpointName,
//			Namespace: TestNamespace,
//		},
//		StringData: map[string]string{},
//	}
//}
//
//func oeTestNewService() *corev1.Service {
//	return &corev1.Service{
//		ObjectMeta: metav1.ObjectMeta{
//			Name:      TestObjectEndpointName,
//			Namespace: TestNamespace,
//		},
//		Spec: corev1.ServiceSpec{},
//	}
//}
//
//func oeTestNewDeployment() *appsv1.Deployment {
//	return &appsv1.Deployment{
//		ObjectMeta: metav1.ObjectMeta{
//			Name:      TestObjectEndpointName,
//			Namespace: TestNamespace,
//		},
//		Spec: appsv1.DeploymentSpec{},
//	}
//}
//
//func (f *fixture) newObjectEndpointController(ctx *context.Context) (*ObjectEndpointController, k8sinformers.SharedInformerFactory, lhinformers.SharedInformerFactory) {
//	f.kubeClient = k8sfake.NewSimpleClientset()
//	f.lhClient = lhfake.NewSimpleClientset()
//
//	kubeInformerFactory := k8sinformers.NewSharedInformerFactory(
//		f.kubeClient,
//		controller.NoResyncPeriodFunc())
//	lhInformerFactory := lhinformers.NewSharedInformerFactory(
//		f.lhClient,
//		controller.NoResyncPeriodFunc())
//	extensionsClient := apiextensionsfake.NewSimpleClientset()
//
//	logger := logrus.StandardLogger()
//	logrus.SetLevel(logrus.DebugLevel)
//
//	ds := datastore.NewDataStore(
//		lhInformerFactory,
//		f.lhClient,
//		kubeInformerFactory,
//		f.kubeClient,
//		extensionsClient,
//		TestNamespace)
//
//	c := NewObjectEndpointController(
//		logger,
//		ds,
//		scheme.Scheme,
//		f.kubeClient,
//		TestObjectEndpointControllerID,
//		TestNamespace,
//		TestObjectEndpointImage,
//		TestObjectEndpointUIImage)
//
//	for index := range c.cacheSyncs {
//		c.cacheSyncs[index] = alwaysReady
//	}
//
//	for _, o := range f.objectEndpointLister {
//		lhInformerFactory.
//			Longhorn().
//			V1beta2().
//			ObjectEndpoints().
//			Informer().
//			GetIndexer().
//			Add(o)
//	}
//
//	for _, v := range f.longhornVolumeLister {
//		lhInformerFactory.
//			Longhorn().
//			V1beta2().
//			Volumes().
//			Informer().
//			GetIndexer().
//			Add(v)
//	}
//
//	for _, p := range f.pvcLister {
//		kubeInformerFactory.
//			Core().
//			V1().
//			PersistentVolumeClaims().
//			Informer().
//			GetIndexer().
//			Add(p)
//	}
//
//	for _, s := range f.secretLister {
//		kubeInformerFactory.
//			Core().
//			V1().
//			Secrets().
//			Informer().
//			GetIndexer().
//			Add(s)
//	}
//
//	for _, s := range f.serviceLister {
//		kubeInformerFactory.
//			Core().
//			V1().
//			Services().
//			Informer().
//			GetIndexer().
//			Add(s)
//	}
//
//	for _, d := range f.deploymentLister {
//		kubeInformerFactory.
//			Apps().
//			V1().
//			Deployments().
//			Informer().
//			GetIndexer().
//			Add(d)
//	}
//
//	return c, kubeInformerFactory, lhInformerFactory
//}
//
//func (f *fixture) runObjectEndpointController(ctx *context.Context, key string) error {
//	c, _, _ := f.newObjectEndpointController(ctx)
//	err := c.syncObjectEndpoint(key)
//	return err
//}
//
//func (f *fixture) runExpectSuccess(ctx *context.Context, key string) {
//	err := f.runObjectEndpointController(ctx, key)
//	if err != nil {
//		f.test.Errorf("%v", err)
//	}
//}
//
//func (f *fixture) runExpectFailure(ctx *context.Context, key string) {
//	err := f.runObjectEndpointController(ctx, key)
//	if err == nil {
//		f.test.Errorf("%v", err)
//	}
//}
//
//// TestSyncNonexistentObjectEndpoint tests that the object endpoint controller
//// gracefully handles the case where the object endpoint does not exist
//func TestSyncNonexistentObjectEndpoint(t *testing.T) {
//	f := newFixture(t)
//	ctx := context.TODO()
//
//	f.runExpectSuccess(&ctx, getMetaKey(TestObjectEndpointName))
//}
//
//// TestSyncNewObjectEndpoint tests the case where a new object endpoint is
//// created that doesn't have any status property at all
//func TestSyncNewObjectEndpoint(t *testing.T) {
//	f := newFixture(t)
//	ctx := context.TODO()
//
//	endpoint := oeTestNewObjectEndpoint()
//	vol := oeTestNewLonghornVolume()
//
//	f.lhObjects = append(f.lhObjects, endpoint)
//	f.lhObjects = append(f.lhObjects, vol)
//	f.objectEndpointLister = append(f.objectEndpointLister, endpoint)
//	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
//
//	f.runExpectSuccess(&ctx, getMetaKey(TestObjectEndpointName))
//}
//
//// TestSyncUnkonwObjectEndpoint tests the default case of a new object endpoint
//// where the status is already filled out by the kubeapi, but still contains the
//// default value of "Unknown"
//func TestSyncUnkonwObjectEndpoint(t *testing.T) {
//	f := newFixture(t)
//	ctx := context.TODO()
//
//	endpoint := oeTestNewObjectEndpoint()
//	(*endpoint).Status = longhorn.ObjectEndpointStatus{
//		State:    longhorn.ObjectEndpointStateUnknown,
//		Endpoint: "",
//	}
//	vol := oeTestNewLonghornVolume()
//
//	f.lhObjects = append(f.lhObjects, endpoint)
//	f.lhObjects = append(f.lhObjects, vol)
//	f.objectEndpointLister = append(f.objectEndpointLister, endpoint)
//	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
//
//	f.runExpectSuccess(&ctx, getMetaKey(TestObjectEndpointName))
//}
//
//// TestSyncStartingObjectEndpoint  tests the case where the object endpoint has
//// already been seen by the controller and the resources should have been
//// deployed
//func TestSyncStartingObjectEndpoint(t *testing.T) {
//	f := newFixture(t)
//	ctx := context.TODO()
//
//	endpoint := oeTestNewObjectEndpoint()
//	(*endpoint).Status = longhorn.ObjectEndpointStatus{
//		State:    longhorn.ObjectEndpointStateStarting,
//		Endpoint: "",
//	}
//	pvc := oeTestNewPersistentVolumeClaim()
//	vol := oeTestNewLonghornVolume()
//	deployment := oeTestNewDeployment()
//	// TODO: Create the other objects here too. This only succeeds because the
//	// volume claim isn't in bound state, so the controller will return success
//	// and wait
//
//	f.lhObjects = append(f.lhObjects, endpoint)
//	f.kubeObjects = append(f.kubeObjects, pvc)
//	f.lhObjects = append(f.lhObjects, vol)
//	f.kubeObjects = append(f.kubeObjects, deployment)
//	f.objectEndpointLister = append(f.objectEndpointLister, endpoint)
//	f.pvcLister = append(f.pvcLister, pvc)
//	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
//	f.deploymentLister = append(f.deploymentLister, deployment)
//
//	f.runExpectSuccess(&ctx, getMetaKey(TestObjectEndpointName))
//}
//
//// TestSyncRunningObjectEndpoint tests the case where the object endpoint is
//// already fully functional and the controller only needs to monitor it
//func TestSyncRunningObjectEndpoint(t *testing.T) {
//	f := newFixture(t)
//	ctx := context.TODO()
//
//	endpoint := oeTestNewObjectEndpoint()
//	(*endpoint).Status = longhorn.ObjectEndpointStatus{
//		State:    longhorn.ObjectEndpointStateRunning,
//		Endpoint: fmt.Sprintf("%s.%s.svc", TestObjectEndpointName, TestNamespace),
//	}
//	pvc := oeTestNewPersistentVolumeClaim()
//	vol := oeTestNewLonghornVolume()
//	secret := oeTestNewSecret()
//	service := oeTestNewService()
//	deployment := oeTestNewDeployment()
//
//	f.lhObjects = append(f.lhObjects, endpoint)
//	f.kubeObjects = append(f.kubeObjects, pvc)
//	f.lhObjects = append(f.lhObjects, vol)
//	f.kubeObjects = append(f.kubeObjects, secret)
//	f.kubeObjects = append(f.kubeObjects, service)
//	f.kubeObjects = append(f.kubeObjects, deployment)
//	f.objectEndpointLister = append(f.objectEndpointLister, endpoint)
//	f.pvcLister = append(f.pvcLister, pvc)
//	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
//	f.secretLister = append(f.secretLister, secret)
//	f.serviceLister = append(f.serviceLister, service)
//	f.deploymentLister = append(f.deploymentLister, deployment)
//
//	f.runExpectSuccess(&ctx, getMetaKey(TestObjectEndpointName))
//}
//
//// TestSyncStoppingObjectEndpoint tests that the object endpoint has been marked
//// for deletion and the controller needs to wait for the resources to be removed
//// before it can remvoe the finalizer and stop tracking the object endpoint
//func TestSyncStoppingObjectEndpoint(t *testing.T) {
//	f := newFixture(t)
//	ctx := context.TODO()
//
//	endpoint := oeTestNewObjectEndpoint()
//	(*endpoint).Status = longhorn.ObjectEndpointStatus{
//		State:    longhorn.ObjectEndpointStateStopping,
//		Endpoint: fmt.Sprintf("%s.%s.svc", TestObjectEndpointName, TestNamespace),
//	}
//
//	f.lhObjects = append(f.lhObjects, endpoint)
//	f.objectEndpointLister = append(f.objectEndpointLister, endpoint)
//
//	f.runExpectSuccess(&ctx, getMetaKey(TestObjectEndpointName))
//}
//
//// TestSyncErrorObjectEndpoint tests the case where the objecte endpoint is in
//// error state
//func TestSyncErrorObjectEndpoint(t *testing.T) {
//	f := newFixture(t)
//	ctx := context.TODO()
//
//	endpoint := oeTestNewObjectEndpoint()
//	(*endpoint).Status = longhorn.ObjectEndpointStatus{
//		State:    longhorn.ObjectEndpointStateStarting,
//		Endpoint: "",
//	}
//	pvc := oeTestNewPersistentVolumeClaim()
//	vol := oeTestNewLonghornVolume()
//	deployment := oeTestNewDeployment()
//	// TODO: Create the other objects here too. This only succeeds because the
//	// volume claim isn't in bound state, so the controller will return success
//	// and wait
//
//	f.lhObjects = append(f.lhObjects, endpoint)
//	f.kubeObjects = append(f.kubeObjects, pvc)
//	f.lhObjects = append(f.lhObjects, vol)
//	f.kubeObjects = append(f.kubeObjects, deployment)
//	f.objectEndpointLister = append(f.objectEndpointLister, endpoint)
//	f.pvcLister = append(f.pvcLister, pvc)
//	f.longhornVolumeLister = append(f.longhornVolumeLister, vol)
//	f.deploymentLister = append(f.deploymentLister, deployment)
//
//	f.runExpectSuccess(&ctx, getMetaKey(TestObjectEndpointName))
//}
//
//// --- Helper Functions ---
//
//func getMetaKey(name string) string { return name }
