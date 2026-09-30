package controller

import (
	"context"
	"fmt"
	"slices"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/util/flowcontrol"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func (s *TestSuite) TestSyncBackingImageDataSourceFileEverTransferred(c *C) {
	bids := &longhorn.BackingImageDataSource{}
	syncBackingImageDataSourceFileEverTransferred(bids)
	c.Assert(bids.Status.FileEverTransferred, Equals, false)

	bids.Spec.FileTransferred = true
	syncBackingImageDataSourceFileEverTransferred(bids)
	c.Assert(bids.Status.FileEverTransferred, Equals, true)

	bids.Spec.FileTransferred = false
	syncBackingImageDataSourceFileEverTransferred(bids)
	c.Assert(bids.Status.FileEverTransferred, Equals, true)
}

func (s *TestSuite) TestIsCreatedBackingImageDataSourcePodLost(c *C) {
	kubeClient := fake.NewSimpleClientset()
	bc := &BackingImageDataSourceController{
		namespace:   TestNamespace,
		kubeClient:  kubeClient,
		createdPods: map[string]struct{}{},
	}
	bids := &longhorn.BackingImageDataSource{ObjectMeta: metav1.ObjectMeta{Name: "bids"}}
	podName := types.GetBackingImageDataSourcePodName(bids.Name)

	lost, err := bc.isCreatedPodLost(bids, podName)
	c.Assert(err, IsNil)
	c.Assert(lost, Equals, false, Commentf("a pod that was never created is an initial prepare"))

	bc.recordCreatedPod(bids.Name)
	_, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(),
		&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: podName, Namespace: TestNamespace}}, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	lost, err = bc.isCreatedPodLost(bids, podName)
	c.Assert(err, IsNil)
	c.Assert(lost, Equals, false, Commentf("informer cache lag must not count as a loss"))

	err = kubeClient.CoreV1().Pods(TestNamespace).Delete(context.TODO(), podName, metav1.DeleteOptions{})
	c.Assert(err, IsNil)
	lost, err = bc.isCreatedPodLost(bids, podName)
	c.Assert(err, IsNil)
	c.Assert(lost, Equals, true)

	bids.Status.CurrentState = longhorn.BackingImageStatePending
	lost, err = bc.isCreatedPodLost(bids, podName)
	c.Assert(err, IsNil)
	c.Assert(lost, Equals, true)

	bids.Status.CurrentState = longhorn.BackingImageStateStarting
	lost, err = bc.isCreatedPodLost(bids, podName)
	c.Assert(err, IsNil)
	c.Assert(lost, Equals, false, Commentf("starting and later states are handled by the not-ready path"))

	bc.forgetCreatedPod(bids.Name)
	bids.Status.CurrentState = ""
	lost, err = bc.isCreatedPodLost(bids, podName)
	c.Assert(err, IsNil)
	c.Assert(lost, Equals, false)
}

func newBackingImageDataSourceBackoffTestController() *BackingImageDataSourceController {
	return &BackingImageDataSourceController{
		backoff:         flowcontrol.NewBackOff(backingImageDataSourceBackoffInit, backingImageDataSourceBackoffMax),
		createdPods:     map[string]struct{}{},
		chargedAttempts: map[string]struct{}{},
	}
}

func (s *TestSuite) TestChargeBackingImageDataSourceFailedAttemptOnce(c *C) {
	bc := newBackingImageDataSourceBackoffTestController()
	name := "bids"

	bc.chargeFailedAttempt(name)
	bc.chargeFailedAttempt(name)
	c.Assert(bc.backoff.Get(name), Equals, backingImageDataSourceBackoffInit)

	bc.startNewAttempt(name)
	bc.chargeFailedAttempt(name)
	c.Assert(bc.backoff.Get(name), Equals, 2*backingImageDataSourceBackoffInit)
}

func (s *TestSuite) TestBackingImageDataSourceMonitorChargesFailureBeforePublishing(c *C) {
	bc := newBackingImageDataSourceBackoffTestController()
	m := &BackingImageDataSourceMonitor{Name: "bids", chargeFailure: bc.chargeFailedAttempt}
	failedInfo := &engineapi.BackingImageDataSourceInfo{State: string(longhorn.BackingImageStateFailed)}

	bids := &longhorn.BackingImageDataSource{}
	bids.Status.CurrentState = longhorn.BackingImageStateInProgress
	m.applyFileInfo(bids, failedInfo)

	// No status was written yet; the controller must already see the retry window.
	c.Assert(bids.Status.CurrentState, Equals, longhorn.BackingImageStateFailed)
	c.Assert(bc.backoff.IsInBackOffSinceUpdate(m.Name, time.Now()), Equals, true)

	// A conflicting write leaves the stored state in-progress, so the next poll reapplies the failure.
	retry := &longhorn.BackingImageDataSource{}
	retry.Status.CurrentState = longhorn.BackingImageStateInProgress
	m.applyFileInfo(retry, failedInfo)
	c.Assert(bc.backoff.Get(m.Name), Equals, backingImageDataSourceBackoffInit)

	// The controller seeing the same failure must not advance it either.
	bc.markBackingImageDataSourceFailed(retry, "")
	c.Assert(bc.backoff.Get(m.Name), Equals, backingImageDataSourceBackoffInit)
}

func (s *TestSuite) TestForgetBackingImageDataSourceClearsBackoff(c *C) {
	bc := newBackingImageDataSourceBackoffTestController()
	name := "bids"

	bc.chargeFailedAttempt(name)
	c.Assert(bc.backoff.IsInBackOffSinceUpdate(name, time.Now()), Equals, true)

	bc.forgetBackingImageDataSource(name)
	c.Assert(bc.backoff.IsInBackOffSinceUpdate(name, time.Now()), Equals, false)
	c.Assert(bc.backoff.Get(name), Equals, time.Duration(0))
}

func (s *TestSuite) TestBackingImageDataSourceRestartConflictKeepsReplacementPod(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	bc, err := NewBackingImageDataSourceController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient,
		TestNamespace, TestNode1, "", "longhorn-backing-image-manager:latest", nil)
	c.Assert(err, IsNil)

	bi := &longhorn.BackingImage{
		ObjectMeta: metav1.ObjectMeta{Name: "bi", Namespace: TestNamespace},
		Status:     longhorn.BackingImageStatus{UUID: "bi-uuid"},
	}
	c.Assert(informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImages().Informer().GetIndexer().Add(bi), IsNil)

	bids := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{Name: bi.Name, Namespace: TestNamespace},
		Spec: longhorn.BackingImageDataSourceSpec{
			NodeID:     TestNode1,
			UUID:       bi.Status.UUID,
			DiskUUID:   TestDiskID1,
			DiskPath:   TestDefaultDataPath,
			SourceType: longhorn.BackingImageDataSourceTypeDownload,
			Parameters: map[string]string{longhorn.DataSourceTypeDownloadParameterURL: "http://example.com/image"},
		},
		Status: longhorn.BackingImageDataSourceStatus{
			OwnerID:      TestNode1,
			CurrentState: longhorn.BackingImageStateFailed,
			Message:      "download failed",
		},
	}
	bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	bidsIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackingImageDataSources().Informer().GetIndexer()
	c.Assert(bidsIndexer.Add(bids), IsNil)

	conflictStatus := true
	lhClient.PrependReactor("update", "backingimagedatasources", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		if !conflictStatus || action.GetSubresource() != "status" {
			return false, nil, nil
		}
		return true, nil, apierrors.NewConflict(longhorn.Resource("backingimagedatasources"), bids.Name, fmt.Errorf("status changed"))
	})
	podName := types.GetBackingImageDataSourcePodName(bids.Name)

	// The backoff window has expired, so the controller restarts the attempt. A conflicting
	// status reset must stop it before the replacement pod exists.
	err = bc.syncBackingImageDataSourcePod(bids.DeepCopy())
	c.Assert(apierrors.IsConflict(errors.Cause(err)), Equals, true)
	_, err = kubeClient.CoreV1().Pods(TestNamespace).Get(context.TODO(), podName, metav1.GetOptions{})
	c.Assert(apierrors.IsNotFound(err), Equals, true)

	conflictStatus = false
	err = bc.syncBackingImageDataSourcePod(bids.DeepCopy())
	c.Assert(err, IsNil)
	persisted, err := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), bids.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(persisted.Status.CurrentState, Equals, longhorn.BackingImageState(""))
	pod, err := kubeClient.CoreV1().Pods(TestNamespace).Get(context.TODO(), podName, metav1.GetOptions{})
	c.Assert(err, IsNil)

	// The next reconcile sees the persisted reset and the starting pod, and keeps the pod.
	c.Assert(informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer().Add(pod), IsNil)
	c.Assert(bidsIndexer.Update(persisted), IsNil)
	err = bc.syncBackingImageDataSourcePod(persisted.DeepCopy())
	c.Assert(err, IsNil)
	_, err = kubeClient.CoreV1().Pods(TestNamespace).Get(context.TODO(), podName, metav1.GetOptions{})
	c.Assert(err, IsNil)
}

func (s *TestSuite) TestBackingImageDataSourceExportConflictReusesSnapshot(c *C) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	lhInformers := informerFactories.LhInformerFactory.Longhorn().V1beta2()

	bc, err := NewBackingImageDataSourceController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient,
		TestNamespace, TestNode1, "", "longhorn-backing-image-manager:latest", nil)
	c.Assert(err, IsNil)
	var createdSnapshots []string
	bc.createExportSnapshot = func(v *longhorn.Volume, e *longhorn.Engine, snapshotName string, labels map[string]string) (string, error) {
		createdSnapshots = append(createdSnapshots, snapshotName)
		return snapshotName, nil
	}

	v := newVolume(TestVolumeName, 1)
	v.Namespace = TestNamespace
	v.Status.State = longhorn.VolumeStateAttached
	v.Status.OwnerID = TestNode1
	c.Assert(lhInformers.Volumes().Informer().GetIndexer().Add(v), IsNil)

	e := newEngineForVolume(v)
	r := newReplicaForVolume(v, e, TestNode1, TestDiskID1)
	r.Namespace = TestNamespace
	r.Status.CurrentState = longhorn.InstanceStateRunning
	r.Status.StorageIP = "10.0.0.1"
	r.Status.Port = 10000
	e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{r.Name: longhorn.ReplicaModeRW}
	// The engine has not reported the new snapshot yet.
	e.Status.CurrentReplicaAddressMap = map[string]string{r.Name: "10.0.0.1:10000"}
	c.Assert(lhInformers.Engines().Informer().GetIndexer().Add(e), IsNil)
	c.Assert(lhInformers.Replicas().Informer().GetIndexer().Add(r), IsNil)

	va := newVolumeAttachment(types.GetLHVolumeAttachmentNameFromVolumeName(v.Name))
	va.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{}
	va, err = lhClient.LonghornV1beta2().VolumeAttachments(TestNamespace).Create(context.TODO(), va, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(lhInformers.VolumeAttachments().Informer().GetIndexer().Add(va), IsNil)

	bi := &longhorn.BackingImage{
		ObjectMeta: metav1.ObjectMeta{Name: "bi", Namespace: TestNamespace},
		Status:     longhorn.BackingImageStatus{UUID: "bi-uuid"},
	}
	c.Assert(lhInformers.BackingImages().Informer().GetIndexer().Add(bi), IsNil)

	bids := &longhorn.BackingImageDataSource{
		ObjectMeta: metav1.ObjectMeta{Name: bi.Name, Namespace: TestNamespace},
		Spec: longhorn.BackingImageDataSourceSpec{
			NodeID:     TestNode1,
			UUID:       bi.Status.UUID,
			DiskUUID:   TestDiskID1,
			DiskPath:   TestDefaultDataPath,
			SourceType: longhorn.BackingImageDataSourceTypeExportFromVolume,
			Parameters: map[string]string{longhorn.DataSourceTypeExportFromVolumeParameterVolumeName: v.Name},
		},
		Status: longhorn.BackingImageDataSourceStatus{OwnerID: TestNode1},
	}
	bids, err = lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Create(context.TODO(), bids, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	c.Assert(lhInformers.BackingImageDataSources().Informer().GetIndexer().Add(bids), IsNil)

	conflictStatus := true
	lhClient.PrependReactor("update", "backingimagedatasources", func(action k8stesting.Action) (bool, k8sruntime.Object, error) {
		if !conflictStatus || action.GetSubresource() != "status" {
			return false, nil, nil
		}
		return true, nil, apierrors.NewConflict(longhorn.Resource("backingimagedatasources"), bids.Name, fmt.Errorf("status changed"))
	})

	for i := 0; i < 3; i++ {
		err = bc.syncBackingImageDataSourcePod(bids.DeepCopy())
		c.Assert(apierrors.IsConflict(errors.Cause(err)), Equals, true)
	}
	c.Assert(createdSnapshots, HasLen, 1)

	conflictStatus = false
	err = bc.syncBackingImageDataSourcePod(bids.DeepCopy())
	c.Assert(err, IsNil)
	c.Assert(createdSnapshots, HasLen, 1)

	persisted, err := lhClient.LonghornV1beta2().BackingImageDataSources(TestNamespace).Get(context.TODO(), bids.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(persisted.Status.RunningParameters[longhorn.DataSourceTypeExportFromVolumeParameterSnapshotName], Equals, createdSnapshots[0])
	pod, err := kubeClient.CoreV1().Pods(TestNamespace).Get(context.TODO(), types.GetBackingImageDataSourcePodName(bids.Name), metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(slices.Contains(pod.Spec.Containers[0].Command,
		fmt.Sprintf("%s=%s", longhorn.DataSourceTypeExportFromVolumeParameterSnapshotName, createdSnapshots[0])), Equals, true)

	// The next retry can observe the persisted name with no pending entry and with the
	// engine status still not listing the snapshot. That must not create another one.
	bc.clearPendingExportSnapshot(bids.Name)
	err = bc.syncBackingImageDataSourcePod(persisted.DeepCopy())
	c.Assert(err, IsNil)
	c.Assert(createdSnapshots, HasLen, 1)
}
