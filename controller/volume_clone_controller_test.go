package controller

import (
	"context"
	"time"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

const (
	testCloneSrcVolumeName = "clone-src"
	testCloneDstVolumeName = "clone-dst"
)

type cloneRetryTestCase struct {
	cloneState           longhorn.VolumeCloneState
	attemptCount         int
	nextAllowedAttemptAt time.Duration
	expected             bool
	expectedWakeUp       bool
}

// cloneRetryTestCases are shared by the clone retry tests. "expected" means the source volume
// should be attached for the clone right now. "expectedWakeUp" means the source volume should be
// synced again when the retry backoff ends.
func cloneRetryTestCases() map[string]cloneRetryTestCase {
	return map[string]cloneRetryTestCase{
		"clone copying data": {
			cloneState:           longhorn.VolumeCloneStateInitiated,
			attemptCount:         1,
			nextAllowedAttemptAt: time.Hour,
			expected:             true,
		},
		"failed clone with the retry due": {
			cloneState:           longhorn.VolumeCloneStateFailed,
			attemptCount:         1,
			nextAllowedAttemptAt: -time.Minute,
			expected:             true,
		},
		"failed clone waiting for the retry backoff": {
			cloneState:           longhorn.VolumeCloneStateFailed,
			attemptCount:         1,
			nextAllowedAttemptAt: time.Hour,
			expected:             false,
			expectedWakeUp:       true,
		},
		"failed clone with no retry left": {
			cloneState:           longhorn.VolumeCloneStateFailed,
			attemptCount:         maxCloneRetry,
			nextAllowedAttemptAt: -time.Minute,
			expected:             false,
		},
	}
}

// newTestCloneVolumes returns a detached v2 source volume and a full-copy clone of it.
func newTestCloneVolumes(c *C, tc cloneRetryTestCase) (src, dst *longhorn.Volume) {
	src = newVolume(testCloneSrcVolumeName, 1)
	src.Namespace = TestNamespace
	src.Spec.DataEngine = longhorn.DataEngineTypeV2
	src.Status.State = longhorn.VolumeStateDetached

	dataSource, err := types.NewVolumeDataSource(longhorn.VolumeDataSourceTypeVolume, map[string]string{types.VolumeNameKey: src.Name})
	c.Assert(err, IsNil)

	dst = newVolume(testCloneDstVolumeName, 1)
	dst.Namespace = TestNamespace
	dst.Labels = types.GetCloneSourceVolumeLabel(src.Name)
	dst.Spec.DataEngine = longhorn.DataEngineTypeV2
	dst.Spec.DataSource = dataSource
	dst.Spec.CloneMode = longhorn.CloneModeFullCopy
	dst.Status.State = longhorn.VolumeStateDetached
	dst.Status.CloneStatus = longhorn.VolumeCloneStatus{
		SourceVolume:         src.Name,
		State:                tc.cloneState,
		AttemptCount:         tc.attemptCount,
		NextAllowedAttemptAt: util.TimestampAfterDuration(tc.nextAllowedAttemptAt),
	}
	return src, dst
}

func newTestVolumeCloneController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*VolumeCloneController, error) {
	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	vcc, err := NewVolumeCloneController(logrus.StandardLogger(), ds, scheme.Scheme, kubeClient, controllerID, TestNamespace)
	if err != nil {
		return nil, err
	}
	vcc.eventRecorder = record.NewFakeRecorder(100)
	for index := range vcc.cacheSyncs {
		vcc.cacheSyncs[index] = alwaysReady
	}
	return vcc, nil
}

// TestVolumeCloneControllerSourceTicketForCloneRetry checks that the source of a failed clone gets
// a clone attachment ticket again once the retry is due. Without it the source stays detached and
// the retry never starts. Ref: https://github.com/longhorn/longhorn/issues/14530
//
// While the retry waits for its backoff, the source must be synced again when the backoff ends.
// This controller does not resync volumes, so nothing else may wake an idle, detached source.
func (s *TestSuite) TestVolumeCloneControllerSourceTicketForCloneRetry(c *C) {
	ticketID := longhorn.GetAttachmentTicketID(longhorn.AttacherTypeVolumeCloneController, testCloneDstVolumeName)

	for name, tc := range cloneRetryTestCases() {
		c.Logf("testing %v", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())
		lhInformers := informerFactories.LhInformerFactory.Longhorn().V1beta2()

		vcc, err := newTestVolumeCloneController(lhClient, kubeClient, extensionsClient, informerFactories, TestNode1)
		c.Assert(err, IsNil)
		queue := &delayRecordingQueue{
			TypedRateLimitingInterface: vcc.queue,
			delayed:                    map[any]time.Duration{},
		}
		vcc.queue = queue

		node := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
		c.Assert(lhInformers.Nodes().Informer().GetIndexer().Add(node), IsNil)
		im := newInstanceManager("instance-manager-"+TestNode1, longhorn.InstanceManagerStateRunning,
			TestNode1, TestNode1, randomIP(), nil, nil, nil, longhorn.DataEngineTypeV2, TestInstanceManagerImage, false)
		c.Assert(lhInformers.InstanceManagers().Informer().GetIndexer().Add(im), IsNil)

		src, dst := newTestCloneVolumes(c, tc)
		c.Assert(lhInformers.Volumes().Informer().GetIndexer().Add(src), IsNil)
		c.Assert(lhInformers.Volumes().Informer().GetIndexer().Add(dst), IsNil)

		srcVA := newVolumeAttachment(src.Name)
		srcVA.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{}
		srcVA, err = lhClient.LonghornV1beta2().VolumeAttachments(TestNamespace).Create(context.TODO(), srcVA, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		c.Assert(lhInformers.VolumeAttachments().Informer().GetIndexer().Add(srcVA), IsNil)

		c.Assert(vcc.reconcile(src.Name), IsNil)

		srcVA, err = lhClient.LonghornV1beta2().VolumeAttachments(TestNamespace).Get(context.TODO(), src.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		ticket, exists := srcVA.Spec.AttachmentTickets[ticketID]
		c.Assert(exists, Equals, tc.expected)
		if tc.expected {
			c.Assert(ticket.NodeID, Equals, TestNode1)
		}

		if !tc.expectedWakeUp {
			c.Assert(queue.delayed, HasLen, 0)
			queue.ShutDown()
			continue
		}

		// The source, not the clone, is synced again at about the end of the backoff.
		srcKey := TestNamespace + "/" + src.Name
		c.Assert(queue.delayed, HasLen, 1)
		delay, scheduled := queue.delayed[srcKey]
		c.Assert(scheduled, Equals, true)
		c.Assert(delay > tc.nextAllowedAttemptAt-time.Minute && delay <= tc.nextAllowedAttemptAt, Equals, true,
			Commentf("delay %v", delay))

		// End the backoff by moving the retry time into the past instead of waiting. Then handle the
		// wake-up the way the worker does. Nothing else has changed, so the wake-up alone must bring
		// the ticket back.
		dueDst := dst.DeepCopy()
		dueDst.Status.CloneStatus.NextAllowedAttemptAt = util.TimestampAfterDuration(-time.Minute)
		c.Assert(lhInformers.Volumes().Informer().GetIndexer().Update(dueDst), IsNil)
		c.Assert(vcc.syncHandler(srcKey), IsNil)

		srcVA, err = lhClient.LonghornV1beta2().VolumeAttachments(TestNamespace).Get(context.TODO(), src.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		ticket, exists = srcVA.Spec.AttachmentTickets[ticketID]
		c.Assert(exists, Equals, true)
		c.Assert(ticket.NodeID, Equals, TestNode1)
		queue.ShutDown()
	}
}

// TestEnqueueVolumeChangeForCloneRetry checks that a failed clone whose retry is due is synced as
// soon as its source volume becomes attached, the same as a clone that is copying data.
func (s *TestSuite) TestEnqueueVolumeChangeForCloneRetry(c *C) {
	for name, tc := range cloneRetryTestCases() {
		c.Logf("testing %v", name)

		kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
		lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
		extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck
		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		vc, err := newTestVolumeController(lhClient, kubeClient, extensionsClient, informerFactories, TestOwnerID1)
		c.Assert(err, IsNil)

		src, dst := newTestCloneVolumes(c, tc)
		c.Assert(informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer().Add(dst), IsNil)

		attachedSrc := src.DeepCopy()
		attachedSrc.Status.State = longhorn.VolumeStateAttached
		vc.enqueueVolumeChange(src, attachedSrc)

		dstEnqueued := false
		for vc.queue.Len() > 0 {
			key, _ := vc.queue.Get()
			if key == TestNamespace+"/"+dst.Name {
				dstEnqueued = true
			}
			vc.queue.Done(key)
		}
		c.Assert(dstEnqueued, Equals, tc.expected)
	}
}
