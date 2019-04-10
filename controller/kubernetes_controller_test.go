package controller

import (
	"fmt"

	"github.com/rancher/longhorn-manager/datastore"
	"github.com/rancher/longhorn-manager/types"
	apiv1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1beta1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	longhorn "github.com/rancher/longhorn-manager/k8s/pkg/apis/longhorn/v1alpha1"
	lhfake "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/rancher/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

const (
	TestWorkloadName  = "test-statefulset"
	TestWorkloadKind  = "StatefulSet"
	TestStatusDeleted = "Deleted"
)

var (
	storageClassName = "longhorn"
	pvcVolumeMode    = apiv1.PersistentVolumeFilesystem
)

type KubernetesTestCase struct {
	volume *longhorn.Volume
	pv     *apiv1.PersistentVolume
	pvc    *apiv1.PersistentVolumeClaim
	pod    *apiv1.Pod

	expectVolume *longhorn.Volume
}

type DisasterRecoveryTestCase struct {
	volume *longhorn.Volume
	pv     *apiv1.PersistentVolume
	pvc    *apiv1.PersistentVolumeClaim
	pod    *apiv1.Pod
	node   *longhorn.Node
	va     *storagev1.VolumeAttachment

	vaShouldExist bool
}

func generateKubernetesTestCaseTemplate() *KubernetesTestCase {
	volume := newVolume(TestVolumeName, 2)
	pv := newPV()
	pvc := newPVC()
	pod := newPodWithPVC()

	return &KubernetesTestCase{
		volume: volume,
		pv:     pv,
		pvc:    pvc,
		pod:    pod,

		expectVolume: nil,
	}
}

func generateDisasterRecoveryTestCaseTemplate() *DisasterRecoveryTestCase {
	volume := newVolume(TestVolumeName, 2)
	pv := newPV()
	pvc := newPVC()
	pod := newPodWithPVC()
	va := newVA(TestVAName, TestNode1, TestPVName)

	return &DisasterRecoveryTestCase{
		volume: volume,
		pv:     pv,
		pvc:    pvc,
		pod:    pod,
		node:   nil,
		va:     va,
	}
}

func (tc *KubernetesTestCase) copyCurrentToExpect() {
	tc.expectVolume = tc.volume.DeepCopy()
}

func newPV() *apiv1.PersistentVolume {
	return &apiv1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: TestPVName,
		},
		Spec: apiv1.PersistentVolumeSpec{
			Capacity: apiv1.ResourceList{
				apiv1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
			},
			VolumeMode: &pvcVolumeMode,
			PersistentVolumeSource: apiv1.PersistentVolumeSource{
				CSI: &apiv1.CSIPersistentVolumeSource{
					Driver: "io.rancher.longhorn",
					FSType: "ext4",
					VolumeAttributes: map[string]string{
						"numberOfReplicas":    "3",
						"staleReplicaTimeout": "30",
					},
					VolumeHandle: TestVolumeName,
				},
			},
			ClaimRef: &apiv1.ObjectReference{
				Name:      TestPVCName,
				Namespace: TestNamespace,
			},
		},
		Status: apiv1.PersistentVolumeStatus{
			Phase: apiv1.VolumeBound,
		},
	}
}

func newPVC() *apiv1.PersistentVolumeClaim {
	return &apiv1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: TestPVCName,
		},
		Spec: apiv1.PersistentVolumeClaimSpec{
			AccessModes: []apiv1.PersistentVolumeAccessMode{
				apiv1.ReadWriteOnce,
			},
			Resources: apiv1.ResourceRequirements{
				Requests: apiv1.ResourceList{
					apiv1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
				},
			},
			VolumeName: TestPVName,
		},
	}
}

func newPodWithPVC() *apiv1.Pod {
	return &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      TestPodName,
			Namespace: TestNamespace,
			OwnerReferences: []metav1.OwnerReference{
				metav1.OwnerReference{
					Kind: TestWorkloadKind,
					Name: TestWorkloadName,
				},
			},
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				apiv1.Container{
					Name:            TestPodName,
					Image:           "nginx:stable-alpine",
					ImagePullPolicy: apiv1.PullIfNotPresent,
					VolumeMounts: []apiv1.VolumeMount{
						apiv1.VolumeMount{
							Name:      "vol",
							MountPath: "/data",
						},
					},
					Ports: []apiv1.ContainerPort{
						apiv1.ContainerPort{
							ContainerPort: 80,
						},
					},
				},
			},
			Volumes: []apiv1.Volume{
				apiv1.Volume{
					Name: "vol",
					VolumeSource: apiv1.VolumeSource{
						PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
							ClaimName: TestPVCName,
						},
					},
				},
			},
		},
	}
}

func newVA(vaName, nodeName, pvName string) *storagev1.VolumeAttachment {
	return &storagev1.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:              vaName,
			CreationTimestamp: metav1.Now(),
		},
		Spec: storagev1.VolumeAttachmentSpec{
			Attacher: "io.rancher.longhorn",
			NodeName: nodeName,
			Source: storagev1.VolumeAttachmentSource{
				PersistentVolumeName: &pvName,
			},
		},
		Status: storagev1.VolumeAttachmentStatus{
			Attached: true,
		},
	}
}

func newTestKubernetesController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset) *KubernetesController {

	volumeInformer := lhInformerFactory.Longhorn().V1alpha1().Volumes()
	engineInformer := lhInformerFactory.Longhorn().V1alpha1().Engines()
	replicaInformer := lhInformerFactory.Longhorn().V1alpha1().Replicas()
	engineImageInformer := lhInformerFactory.Longhorn().V1alpha1().EngineImages()
	nodeInformer := lhInformerFactory.Longhorn().V1alpha1().Nodes()
	settingInformer := lhInformerFactory.Longhorn().V1alpha1().Settings()

	podInformer := kubeInformerFactory.Core().V1().Pods()
	persistentVolumeInformer := kubeInformerFactory.Core().V1().PersistentVolumes()
	persistentVolumeClaimInformer := kubeInformerFactory.Core().V1().PersistentVolumeClaims()
	cronJobInformer := kubeInformerFactory.Batch().V1beta1().CronJobs()
	daemonSetInformer := kubeInformerFactory.Apps().V1beta2().DaemonSets()
	volumeAttachmentInformer := kubeInformerFactory.Storage().V1beta1().VolumeAttachments()

	ds := datastore.NewDataStore(
		volumeInformer, engineInformer, replicaInformer,
		engineImageInformer, nodeInformer, settingInformer,
		lhClient,
		podInformer, cronJobInformer, daemonSetInformer,
		persistentVolumeInformer, persistentVolumeClaimInformer,
		kubeClient, TestNamespace)

	kc := NewKubernetesController(ds, scheme.Scheme, volumeInformer, persistentVolumeInformer,
		persistentVolumeClaimInformer, podInformer, volumeAttachmentInformer, kubeClient)

	fakeRecorder := record.NewFakeRecorder(100)
	kc.eventRecorder = fakeRecorder

	kc.pvStoreSynced = alwaysReady
	kc.pvcStoreSynced = alwaysReady
	kc.pStoreSynced = alwaysReady

	kc.nowHandler = getTestNow

	return kc
}

func (s *TestSuite) TestSyncKubernetesStatus(c *C) {
	deleteTime := metav1.Now()
	var tc *KubernetesTestCase
	testCases := map[string]*KubernetesTestCase{}

	// pod + pvc + pv + workload set
	tc = generateKubernetesTestCaseTemplate()
	tc.copyCurrentToExpect()
	tc.pv.Status.Phase = apiv1.VolumeBound
	tc.pod.Status.Phase = apiv1.PodRunning
	tc.expectVolume.Status.KubernetesStatus = types.KubernetesStatus{
		PVName:       TestPVName,
		PVStatus:     string(apiv1.VolumeBound),
		Namespace:    TestNamespace,
		PVCName:      TestPVCName,
		PodName:      TestPodName,
		PodStatus:    string(apiv1.PodRunning),
		WorkloadName: TestWorkloadName,
		WorkloadType: TestWorkloadKind,
	}
	testCases["all set"] = tc

	// volume unset
	tc = generateKubernetesTestCaseTemplate()
	tc.copyCurrentToExpect()
	tc.volume = nil
	tc.pv.Status.Phase = apiv1.VolumeBound
	tc.pod.Status.Phase = apiv1.PodRunning
	tc.expectVolume.Status.KubernetesStatus = types.KubernetesStatus{}
	testCases["volume unset"] = tc

	// pv unset
	tc = generateKubernetesTestCaseTemplate()
	tc.copyCurrentToExpect()
	tc.pv = nil
	tc.pod.Status.Phase = apiv1.PodRunning
	tc.expectVolume.Status.KubernetesStatus = types.KubernetesStatus{}
	testCases["pv unset"] = tc

	// pvc unset
	tc = generateKubernetesTestCaseTemplate()
	tc.copyCurrentToExpect()
	tc.pv.Status.Phase = apiv1.VolumeAvailable
	tc.pv.Spec.ClaimRef = nil
	tc.pvc = nil
	tc.expectVolume.Status.KubernetesStatus = types.KubernetesStatus{
		PVName:   TestPVName,
		PVStatus: string(apiv1.VolumeAvailable),
	}
	testCases["pvc unset"] = tc

	// pod unset
	tc = generateKubernetesTestCaseTemplate()
	tc.copyCurrentToExpect()
	tc.pv.Status.Phase = apiv1.VolumeBound
	tc.pod = nil
	tc.expectVolume.Status.KubernetesStatus = types.KubernetesStatus{
		PVName:    TestPVName,
		PVStatus:  string(apiv1.VolumeBound),
		Namespace: TestNamespace,
		PVCName:   TestPVCName,
	}
	testCases["pod unset"] = tc

	// workload unset
	tc = generateKubernetesTestCaseTemplate()
	tc.copyCurrentToExpect()
	tc.pv.Status.Phase = apiv1.VolumeBound
	tc.pod.Status.Phase = apiv1.PodRunning
	tc.pod.ObjectMeta.OwnerReferences = nil
	tc.expectVolume.Status.KubernetesStatus = types.KubernetesStatus{
		PVName:    TestPVName,
		PVStatus:  string(apiv1.VolumeBound),
		Namespace: TestNamespace,
		PVCName:   TestPVCName,
		PodName:   TestPodName,
		PodStatus: string(apiv1.PodRunning),
	}
	testCases["workload unset"] = tc

	// pod phase updated: running -> failed
	tc = generateKubernetesTestCaseTemplate()
	tc.pv.Status.Phase = apiv1.VolumeBound
	tc.pod.Status.Phase = apiv1.PodFailed
	tc.volume.Status.KubernetesStatus = types.KubernetesStatus{
		PVName:       TestPVName,
		PVStatus:     string(apiv1.VolumeBound),
		Namespace:    TestNamespace,
		PVCName:      TestPVCName,
		PodName:      TestPodName,
		PodStatus:    string(apiv1.PodRunning),
		WorkloadName: TestWorkloadName,
		WorkloadType: TestWorkloadKind,
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.KubernetesStatus.PodStatus = string(apiv1.PodFailed)
	testCases["pod phase updated to 'failed'"] = tc

	// pod deleted
	tc = generateKubernetesTestCaseTemplate()
	tc.pv.Status.Phase = apiv1.VolumeBound
	tc.pod.Status.Phase = apiv1.PodRunning
	tc.pod.DeletionTimestamp = &deleteTime
	tc.volume.Status.KubernetesStatus = types.KubernetesStatus{
		PVName:       TestPVName,
		PVStatus:     string(apiv1.VolumeBound),
		Namespace:    TestNamespace,
		PVCName:      TestPVCName,
		PodName:      TestPodName,
		PodStatus:    string(apiv1.PodRunning),
		WorkloadName: TestWorkloadName,
		WorkloadType: TestWorkloadKind,
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.KubernetesStatus.LastPodRefAt = getTestNow()
	testCases["pod deleted"] = tc

	// pv phase updated: bound -> failed
	tc = generateKubernetesTestCaseTemplate()
	tc.pv.Status.Phase = apiv1.VolumeFailed
	tc.pod.Status.Phase = apiv1.PodRunning
	tc.volume.Status.KubernetesStatus = types.KubernetesStatus{
		PVName:       TestPVName,
		PVStatus:     string(apiv1.VolumeBound),
		Namespace:    TestNamespace,
		PVCName:      TestPVCName,
		PodName:      TestPodName,
		PodStatus:    string(apiv1.PodRunning),
		WorkloadName: TestWorkloadName,
		WorkloadType: TestWorkloadKind,
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.KubernetesStatus.PVStatus = string(apiv1.VolumeFailed)
	tc.expectVolume.Status.KubernetesStatus.LastPVCRefAt = getTestNow()
	testCases["pv phase updated to 'failed'"] = tc

	// pv deleted
	tc = generateKubernetesTestCaseTemplate()
	tc.pv.Status.Phase = apiv1.VolumeBound
	tc.pv.DeletionTimestamp = &deleteTime
	tc.pod.Status.Phase = apiv1.PodRunning
	tc.volume.Status.KubernetesStatus = types.KubernetesStatus{
		Namespace:    TestNamespace,
		PVCName:      TestPVCName,
		PodName:      TestPodName,
		PodStatus:    string(apiv1.PodRunning),
		WorkloadName: TestWorkloadName,
		WorkloadType: TestWorkloadKind,
		LastPodRefAt: "",
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.KubernetesStatus.LastPVCRefAt = getTestNow()
	tc.expectVolume.Status.KubernetesStatus.LastPodRefAt = getTestNow()
	testCases["pv deleted"] = tc

	s.runKubernetesTestCases(c, testCases)
}

func (s *TestSuite) runKubernetesTestCases(c *C, testCases map[string]*KubernetesTestCase) {
	for name, tc := range testCases {
		var err error
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())
		vIndexer := lhInformerFactory.Longhorn().V1alpha1().Volumes().Informer().GetIndexer()

		pvIndexer := kubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()
		pvcIndexer := kubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
		pIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

		kc := newTestKubernetesController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient)

		// Need to create pv, pvc, pod and longhorn volume
		var v *longhorn.Volume
		if tc.volume != nil {
			v, err = lhClient.LonghornV1alpha1().Volumes(TestNamespace).Create(tc.volume)
			c.Assert(err, IsNil)
			err = vIndexer.Add(v)
			c.Assert(err, IsNil)
		}

		var pv *apiv1.PersistentVolume
		if tc.pv != nil {
			pv, err = kubeClient.CoreV1().PersistentVolumes().Create(tc.pv)
			c.Assert(err, IsNil)
			pvIndexer.Add(pv)
			c.Assert(err, IsNil)
			if pv.DeletionTimestamp != nil {
				kc.enqueuePVDeletion(pv)
			}
		}

		if tc.pvc != nil {
			pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Create(tc.pvc)
			c.Assert(err, IsNil)
			pvcIndexer.Add(pvc)
		}

		var p *apiv1.Pod
		if tc.pod != nil {
			p, err = kubeClient.CoreV1().Pods(TestNamespace).Create(tc.pod)
			c.Assert(err, IsNil)
			pIndexer.Add(p)
		}

		if pv != nil {
			err = kc.syncKubernetesStatus(getKey(pv, c))
			c.Assert(err, IsNil)
		}

		if v != nil {
			retV, err := lhClient.LonghornV1alpha1().Volumes(TestNamespace).Get(v.Name, metav1.GetOptions{})
			c.Assert(err, IsNil)
			c.Assert(retV.Spec, DeepEquals, tc.expectVolume.Spec)
			c.Assert(retV.Status.KubernetesStatus, DeepEquals, tc.expectVolume.Status.KubernetesStatus)
		}

	}
}

func (s *TestSuite) TestDisasterRecovery(c *C) {
	var tc *DisasterRecoveryTestCase
	testCases := map[string]*DisasterRecoveryTestCase{}

	tc = generateDisasterRecoveryTestCaseTemplate()
	tc.node = newNode(TestNode1, TestNamespace, true, types.ConditionStatusFalse, types.NodeConditionReasonKubernetesNodeDown)
	tc.pod.Status.Phase = apiv1.PodPending
	tc.pvc.Status.Phase = apiv1.ClaimBound
	tc.volume.Status.KubernetesStatus = types.KubernetesStatus{
		PVName:       TestPVName,
		PVStatus:     string(apiv1.VolumeBound),
		Namespace:    TestNamespace,
		PVCName:      TestPVCName,
		PodName:      TestPodName,
		PodStatus:    string(apiv1.PodRunning),
		WorkloadName: TestWorkloadName,
		WorkloadType: TestWorkloadKind,
		LastPodRefAt: getTestNow(),
	}
	tc.vaShouldExist = false
	testCases["va deleted when node is NotReady"] = tc

	tc = generateDisasterRecoveryTestCaseTemplate()
	tc.node = newNode(TestNode1, TestNamespace, true, types.ConditionStatusTrue, "")
	tc.pod.Status.Phase = apiv1.PodPending
	tc.pvc.Status.Phase = apiv1.ClaimBound
	tc.volume.Status.KubernetesStatus = types.KubernetesStatus{
		PVName:       TestPVName,
		PVStatus:     string(apiv1.VolumeBound),
		Namespace:    TestNamespace,
		PVCName:      TestPVCName,
		PodName:      TestPodName,
		PodStatus:    string(apiv1.PodRunning),
		WorkloadName: TestWorkloadName,
		WorkloadType: TestWorkloadKind,
		LastPodRefAt: getTestNow(),
	}
	tc.vaShouldExist = true
	testCases["va unchanged when node is Ready"] = tc

	s.runDisasterRecoveryTestCases(c, testCases)
}

func (s *TestSuite) runDisasterRecoveryTestCases(c *C, testCases map[string]*DisasterRecoveryTestCase) {
	for name, tc := range testCases {
		var err error
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		kubeInformerFactory := informers.NewSharedInformerFactory(kubeClient, controller.NoResyncPeriodFunc())

		lhClient := lhfake.NewSimpleClientset()
		lhInformerFactory := lhinformerfactory.NewSharedInformerFactory(lhClient, controller.NoResyncPeriodFunc())
		vIndexer := lhInformerFactory.Longhorn().V1alpha1().Volumes().Informer().GetIndexer()
		nodeIndexer := lhInformerFactory.Longhorn().V1alpha1().Nodes().Informer().GetIndexer()

		pvIndexer := kubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()
		pvcIndexer := kubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
		pIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
		vaIndexer := kubeInformerFactory.Storage().V1beta1().VolumeAttachments().Informer().GetIndexer()

		kc := newTestKubernetesController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient)

		if tc.node != nil {
			node, err := lhClient.LonghornV1alpha1().Nodes(TestNamespace).Create(tc.node)
			c.Assert(err, IsNil)
			nodeIndexer.Add(node)
		}

		var v *longhorn.Volume
		if tc.volume != nil {
			v, err = lhClient.LonghornV1alpha1().Volumes(TestNamespace).Create(tc.volume)
			c.Assert(err, IsNil)
			err = vIndexer.Add(v)
			c.Assert(err, IsNil)
		}

		var pv *apiv1.PersistentVolume
		if tc.pv != nil {
			pv, err = kubeClient.CoreV1().PersistentVolumes().Create(tc.pv)
			c.Assert(err, IsNil)
			pvIndexer.Add(pv)
			c.Assert(err, IsNil)
			if pv.DeletionTimestamp != nil {
				kc.enqueuePVDeletion(pv)
			}
		}

		if tc.pvc != nil {
			pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Create(tc.pvc)
			c.Assert(err, IsNil)
			pvcIndexer.Add(pvc)
		}

		if tc.va != nil {
			va, err := kubeClient.StorageV1beta1().VolumeAttachments().Create(tc.va)
			c.Assert(err, IsNil)
			vaIndexer.Add(va)
		}

		if tc.pod != nil {
			p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(tc.pod)
			c.Assert(err, IsNil)
			pIndexer.Add(p)
		}

		if pv != nil {
			err = kc.syncKubernetesStatus(getKey(pv, c))
			c.Assert(err, IsNil)
		}

		va, err := kubeClient.StorageV1beta1().VolumeAttachments().Get(TestVAName, metav1.GetOptions{})
		if tc.vaShouldExist {
			c.Assert(err, IsNil)
			c.Assert(va, DeepEquals, tc.va)
		} else {
			if err != nil {
				c.Assert(datastore.ErrorIsNotFound(err), Equals, true)
			} else {
				c.Assert(va.DeletionTimestamp, NotNil)
			}
		}
	}
}
