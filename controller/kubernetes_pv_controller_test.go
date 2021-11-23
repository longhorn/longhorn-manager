package controller

import (
	"context"
	"fmt"
	"sort"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	apiv1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	lhinformerfactory "github.com/longhorn/longhorn-manager/k8s/pkg/client/informers/externalversions"

	. "gopkg.in/check.v1"
)

const (
	TestWorkloadName = "test-statefulset"
	TestWorkloadKind = "StatefulSet"
)

type KubernetesTestCase struct {
	volume *longhorn.Volume
	pv     *apiv1.PersistentVolume
	pvc    *apiv1.PersistentVolumeClaim
	pods   []*apiv1.Pod

	expectVolume *longhorn.Volume
}

func generateKubernetesTestCaseTemplate() *KubernetesTestCase {
	volume := newVolume(TestVolumeName, 2)
	pv := newPV()
	pvc := newPVC()
	pods := []*apiv1.Pod{newPodWithPVC(TestPod1)}

	return &KubernetesTestCase{
		volume: volume,
		pv:     pv,
		pvc:    pvc,
		pods:   pods,

		expectVolume: nil,
	}
}

func (tc *KubernetesTestCase) copyCurrentToExpect() {
	tc.expectVolume = tc.volume.DeepCopy()
}

func newPV() *apiv1.PersistentVolume {
	pvcFilesystemMode := apiv1.PersistentVolumeFilesystem
	return &apiv1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: TestPVName,
		},
		Spec: apiv1.PersistentVolumeSpec{
			Capacity: apiv1.ResourceList{
				apiv1.ResourceStorage: *resource.NewQuantity(1, resource.BinarySI),
			},
			VolumeMode: &pvcFilesystemMode,
			PersistentVolumeSource: apiv1.PersistentVolumeSource{
				CSI: &apiv1.CSIPersistentVolumeSource{
					Driver: types.LonghornDriverName,
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

func newPodWithPVC(podName string) *apiv1.Pod {
	return &apiv1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podName,
			Namespace: TestNamespace,
			OwnerReferences: []metav1.OwnerReference{
				{
					Kind: TestWorkloadKind,
					Name: TestWorkloadName,
				},
			},
		},
		Spec: apiv1.PodSpec{
			Containers: []apiv1.Container{
				{
					Name:            podName,
					Image:           "nginx:stable-alpine",
					ImagePullPolicy: apiv1.PullIfNotPresent,
					VolumeMounts: []apiv1.VolumeMount{
						{
							Name:      "vol",
							MountPath: "/data",
						},
					},
					Ports: []apiv1.ContainerPort{
						{
							ContainerPort: 80,
						},
					},
				},
			},
			Volumes: []apiv1.Volume{
				{
					Name: "vol",
					VolumeSource: apiv1.VolumeSource{
						PersistentVolumeClaim: &apiv1.PersistentVolumeClaimVolumeSource{
							ClaimName: TestPVCName,
						},
					},
				},
			},
		},
		Status: apiv1.PodStatus{
			Phase: apiv1.PodRunning,
		},
	}
}

func newTestKubernetesPVController(lhInformerFactory lhinformerfactory.SharedInformerFactory, kubeInformerFactory informers.SharedInformerFactory,
	lhClient *lhfake.Clientset, kubeClient *fake.Clientset) *KubernetesPVController {
	ds := datastore.NewDataStore(lhInformerFactory, lhClient, kubeInformerFactory, kubeClient, TestNamespace)

	logger := logrus.StandardLogger()
	kc := NewKubernetesPVController(logger, ds, scheme.Scheme, kubeClient, TestNode1)

	fakeRecorder := record.NewFakeRecorder(100)
	kc.eventRecorder = fakeRecorder
	for index := range kc.cacheSyncs {
		kc.cacheSyncs[index] = alwaysReady
	}
	kc.nowHandler = getTestNow

	return kc
}

func (s *TestSuite) TestSyncKubernetesStatus(c *C) {
	deleteTime := metav1.Now()
	var workloads []longhorn.WorkloadStatus
	var tc *KubernetesTestCase
	testCases := map[string]*KubernetesTestCase{}

	// pod + pvc + pv + workload set
	tc = generateKubernetesTestCaseTemplate()
	tc.copyCurrentToExpect()
	tc.pv.Status.Phase = apiv1.VolumeBound
	tc.pods = append(tc.pods, newPodWithPVC(TestPod2))
	workloads = []longhorn.WorkloadStatus{}
	for _, p := range tc.pods {
		ws := longhorn.WorkloadStatus{
			PodName:      p.Name,
			PodStatus:    string(p.Status.Phase),
			WorkloadName: TestWorkloadName,
			WorkloadType: TestWorkloadKind,
		}
		workloads = append(workloads, ws)
	}
	tc.expectVolume.Status.KubernetesStatus = longhorn.KubernetesStatus{
		PVName:          TestPVName,
		PVStatus:        string(apiv1.VolumeBound),
		Namespace:       TestNamespace,
		PVCName:         TestPVCName,
		WorkloadsStatus: workloads,
	}
	testCases["all set"] = tc

	// volume unset
	tc = generateKubernetesTestCaseTemplate()
	tc.copyCurrentToExpect()
	tc.volume = nil
	tc.pv.Status.Phase = apiv1.VolumeBound
	workloads = []longhorn.WorkloadStatus{}
	for _, p := range tc.pods {
		ws := longhorn.WorkloadStatus{
			PodName:      p.Name,
			PodStatus:    string(p.Status.Phase),
			WorkloadName: TestWorkloadName,
			WorkloadType: TestWorkloadKind,
		}
		workloads = append(workloads, ws)
	}
	tc.expectVolume.Status.KubernetesStatus = longhorn.KubernetesStatus{}
	testCases["volume unset"] = tc

	// pv unset
	tc = generateKubernetesTestCaseTemplate()
	tc.copyCurrentToExpect()
	tc.pv = nil
	workloads = []longhorn.WorkloadStatus{}
	for _, p := range tc.pods {
		ws := longhorn.WorkloadStatus{
			PodName:      p.Name,
			PodStatus:    string(p.Status.Phase),
			WorkloadName: TestWorkloadName,
			WorkloadType: TestWorkloadKind,
		}
		workloads = append(workloads, ws)
	}
	tc.expectVolume.Status.KubernetesStatus = longhorn.KubernetesStatus{}
	testCases["pv unset"] = tc

	// pvc unset
	tc = generateKubernetesTestCaseTemplate()
	tc.copyCurrentToExpect()
	tc.pv.Status.Phase = apiv1.VolumeAvailable
	tc.pv.Spec.ClaimRef = nil
	tc.pvc = nil
	tc.expectVolume.Status.KubernetesStatus = longhorn.KubernetesStatus{
		PVName:   TestPVName,
		PVStatus: string(apiv1.VolumeAvailable),
	}
	testCases["pvc unset"] = tc

	// pod unset
	tc = generateKubernetesTestCaseTemplate()
	tc.copyCurrentToExpect()
	tc.pv.Status.Phase = apiv1.VolumeBound
	tc.pods = nil
	tc.expectVolume.Status.KubernetesStatus = longhorn.KubernetesStatus{
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
	tc.pods = append(tc.pods, newPodWithPVC(TestPod2))
	workloads = []longhorn.WorkloadStatus{}
	for _, p := range tc.pods {
		p.ObjectMeta.OwnerReferences = nil
		ws := longhorn.WorkloadStatus{
			PodName:   p.Name,
			PodStatus: string(p.Status.Phase),
		}
		workloads = append(workloads, ws)
	}
	tc.expectVolume.Status.KubernetesStatus = longhorn.KubernetesStatus{
		PVName:          TestPVName,
		PVStatus:        string(apiv1.VolumeBound),
		Namespace:       TestNamespace,
		PVCName:         TestPVCName,
		WorkloadsStatus: workloads,
	}
	testCases["workload unset"] = tc

	// pod phase updated: running -> failed
	tc = generateKubernetesTestCaseTemplate()
	tc.pv.Status.Phase = apiv1.VolumeBound
	workloads = []longhorn.WorkloadStatus{}
	for _, p := range tc.pods {
		ws := longhorn.WorkloadStatus{
			PodName:      p.Name,
			PodStatus:    string(p.Status.Phase),
			WorkloadName: TestWorkloadName,
			WorkloadType: TestWorkloadKind,
		}
		workloads = append(workloads, ws)
	}
	tc.volume.Status.KubernetesStatus = longhorn.KubernetesStatus{
		PVName:          TestPVName,
		PVStatus:        string(apiv1.VolumeBound),
		Namespace:       TestNamespace,
		PVCName:         TestPVCName,
		WorkloadsStatus: workloads,
	}
	tc.copyCurrentToExpect()
	workloads = []longhorn.WorkloadStatus{}
	for _, p := range tc.pods {
		p.Status.Phase = apiv1.PodFailed
		ws := longhorn.WorkloadStatus{
			PodName:      p.Name,
			PodStatus:    string(p.Status.Phase),
			WorkloadName: TestWorkloadName,
			WorkloadType: TestWorkloadKind,
		}
		workloads = append(workloads, ws)
	}
	tc.expectVolume.Status.KubernetesStatus.WorkloadsStatus = workloads
	testCases["pod phase updated to 'failed'"] = tc

	// pod deletion requested (retain workload status)
	tc = generateKubernetesTestCaseTemplate()
	tc.pv.Status.Phase = apiv1.VolumeBound
	workloads = []longhorn.WorkloadStatus{}
	for _, p := range tc.pods {
		p.DeletionTimestamp = &deleteTime
		ws := longhorn.WorkloadStatus{
			PodName:      p.Name,
			PodStatus:    string(p.Status.Phase),
			WorkloadName: TestWorkloadName,
			WorkloadType: TestWorkloadKind,
		}
		workloads = append(workloads, ws)
	}
	tc.volume.Status.KubernetesStatus = longhorn.KubernetesStatus{
		PVName:          TestPVName,
		PVStatus:        string(apiv1.VolumeBound),
		Namespace:       TestNamespace,
		PVCName:         TestPVCName,
		WorkloadsStatus: workloads,
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.KubernetesStatus.LastPodRefAt = ""
	testCases["pod deletion requested"] = tc

	// pod deleted (set podLastRefAt)
	tc = generateKubernetesTestCaseTemplate()
	tc.pv.Status.Phase = apiv1.VolumeBound
	workloads = []longhorn.WorkloadStatus{}
	for _, p := range tc.pods {
		p.DeletionTimestamp = &deleteTime
		ws := longhorn.WorkloadStatus{
			PodName:      p.Name,
			PodStatus:    string(p.Status.Phase),
			WorkloadName: TestWorkloadName,
			WorkloadType: TestWorkloadKind,
		}
		workloads = append(workloads, ws)
	}
	tc.volume.Status.KubernetesStatus = longhorn.KubernetesStatus{
		PVName:          TestPVName,
		PVStatus:        string(apiv1.VolumeBound),
		Namespace:       TestNamespace,
		PVCName:         TestPVCName,
		WorkloadsStatus: workloads,
	}
	tc.copyCurrentToExpect()
	tc.pods = nil // pod has been deleted
	tc.expectVolume.Status.KubernetesStatus.LastPodRefAt = getTestNow()
	testCases["pod deletion done"] = tc

	// pv phase updated: bound -> failed
	tc = generateKubernetesTestCaseTemplate()
	tc.pv.Status.Phase = apiv1.VolumeFailed
	workloads = []longhorn.WorkloadStatus{}
	for _, p := range tc.pods {
		ws := longhorn.WorkloadStatus{
			PodName:      p.Name,
			PodStatus:    string(p.Status.Phase),
			WorkloadName: TestWorkloadName,
			WorkloadType: TestWorkloadKind,
		}
		workloads = append(workloads, ws)
	}
	tc.volume.Status.KubernetesStatus = longhorn.KubernetesStatus{
		PVName:          TestPVName,
		PVStatus:        string(apiv1.VolumeBound),
		Namespace:       TestNamespace,
		PVCName:         TestPVCName,
		WorkloadsStatus: workloads,
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.KubernetesStatus.PVStatus = string(apiv1.VolumeFailed)
	tc.expectVolume.Status.KubernetesStatus.LastPVCRefAt = getTestNow()
	tc.expectVolume.Status.KubernetesStatus.LastPodRefAt = getTestNow()
	testCases["pv phase updated to 'failed'"] = tc

	// pv deleted
	tc = generateKubernetesTestCaseTemplate()
	tc.pv.Status.Phase = apiv1.VolumeBound
	tc.pv.DeletionTimestamp = &deleteTime
	workloads = []longhorn.WorkloadStatus{}
	for _, p := range tc.pods {
		ws := longhorn.WorkloadStatus{
			PodName:      p.Name,
			PodStatus:    string(p.Status.Phase),
			WorkloadName: TestWorkloadName,
			WorkloadType: TestWorkloadKind,
		}
		workloads = append(workloads, ws)
	}
	tc.volume.Status.KubernetesStatus = longhorn.KubernetesStatus{
		Namespace:       TestNamespace,
		PVCName:         TestPVCName,
		WorkloadsStatus: workloads,
		LastPodRefAt:    "",
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.KubernetesStatus.LastPVCRefAt = getTestNow()
	tc.expectVolume.Status.KubernetesStatus.LastPodRefAt = getTestNow()
	testCases["pv deleted"] = tc

	// unknown PV - no CSI
	tc = generateKubernetesTestCaseTemplate()
	tc.pv.Spec.CSI = nil
	tc.pvc = nil
	tc.pods = nil
	tc.copyCurrentToExpect()
	testCases["unknown pv - no CSI"] = tc

	// unknown PV - wrong CSI driver
	tc = generateKubernetesTestCaseTemplate()
	tc.pv.Spec.CSI.Driver = "random_csi_driver"
	tc.pvc = nil
	tc.pods = nil
	tc.copyCurrentToExpect()
	testCases["unknown pv - wrong CSI driver"] = tc

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
		vIndexer := lhInformerFactory.Longhorn().V1beta1().Volumes().Informer().GetIndexer()

		pvIndexer := kubeInformerFactory.Core().V1().PersistentVolumes().Informer().GetIndexer()
		pvcIndexer := kubeInformerFactory.Core().V1().PersistentVolumeClaims().Informer().GetIndexer()
		pIndexer := kubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()

		kc := newTestKubernetesPVController(lhInformerFactory, kubeInformerFactory, lhClient, kubeClient)

		// Need to create pv, pvc, pod and longhorn volume
		var v *longhorn.Volume
		if tc.volume != nil {
			v, err = lhClient.LonghornV1beta1().Volumes(TestNamespace).Create(context.TODO(), tc.volume, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = vIndexer.Add(v)
			c.Assert(err, IsNil)
		}

		var pv *apiv1.PersistentVolume
		if tc.pv != nil {
			pv, err = kubeClient.CoreV1().PersistentVolumes().Create(context.TODO(), tc.pv, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = pvIndexer.Add(pv)
			c.Assert(err, IsNil)
			if pv.DeletionTimestamp != nil {
				kc.enqueuePVDeletion(pv)
			}
		}

		if tc.pvc != nil {
			pvc, err := kubeClient.CoreV1().PersistentVolumeClaims(TestNamespace).Create(context.TODO(), tc.pvc, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = pvcIndexer.Add(pvc)
			c.Assert(err, IsNil)

		}

		if len(tc.pods) != 0 {
			for _, p := range tc.pods {
				p, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), p, metav1.CreateOptions{})
				c.Assert(err, IsNil)
				err = pIndexer.Add(p)
				c.Assert(err, IsNil)
			}
		}

		if pv != nil {
			err = kc.syncKubernetesStatus(getKey(pv, c))
			c.Assert(err, IsNil)
		}

		if v != nil {
			retV, err := lhClient.LonghornV1beta1().Volumes(TestNamespace).Get(context.TODO(), v.Name, metav1.GetOptions{})
			c.Assert(err, IsNil)
			c.Assert(retV.Spec, DeepEquals, tc.expectVolume.Spec)
			sort.Slice(retV.Status.KubernetesStatus.WorkloadsStatus, func(i, j int) bool {
				return retV.Status.KubernetesStatus.WorkloadsStatus[i].PodName < retV.Status.KubernetesStatus.WorkloadsStatus[j].PodName
			})
			sort.Slice(tc.expectVolume.Status.KubernetesStatus.WorkloadsStatus, func(i, j int) bool {
				return tc.expectVolume.Status.KubernetesStatus.WorkloadsStatus[i].PodName < tc.expectVolume.Status.KubernetesStatus.WorkloadsStatus[j].PodName
			})
			c.Assert(retV.Status.KubernetesStatus, DeepEquals, tc.expectVolume.Status.KubernetesStatus)
		}

	}
}
