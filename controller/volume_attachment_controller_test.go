package controller

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

type volumeAttachmentTestCase struct {
	volAttachment   *longhorn.VolumeAttachment
	vol             *longhorn.Volume
	nodes           []*longhorn.Node
	engines         []*longhorn.Engine
	engineFrontends []*longhorn.EngineFrontend

	expectedVolAttachment *longhorn.VolumeAttachment
	expectedVol           *longhorn.Volume
}

func (tc *volumeAttachmentTestCase) copyCurrentToExpect() {
	tc.expectedVolAttachment = tc.volAttachment.DeepCopy()
	tc.expectedVol = tc.vol.DeepCopy()
}

func (s *TestSuite) TestVolumeAttachmentLifeCycle(c *C) {
	var tc *volumeAttachmentTestCase
	testCases := map[string]*volumeAttachmentTestCase{}

	///////////////////////////////////////////////////////////////////
	tc = generateVolumeAttachmentTestCaseTemplate(TestVolumeName)
	tc.volAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"attachment-01": &longhorn.AttachmentTicket{
			ID:         "attachment-01",
			Type:       longhorn.AttacherTypeCSIAttacher,
			NodeID:     TestNode1,
			Parameters: map[string]string{},
			Generation: 0,
		},
	}
	tc.vol.Status.OwnerID = TestNode1
	tc.vol.Status.State = longhorn.VolumeStateDetached
	tc.copyCurrentToExpect()
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"attachment-01": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-01",
			Satisfied: false,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusFalse, "", ""),
			Generation: 0,
		},
	}
	tc.expectedVol.Spec.NodeID = TestNode1
	testCases["test case 1: attach: basic"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	tc = generateVolumeAttachmentTestCaseTemplate(TestVolumeName)
	tc.volAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"attachment-01": &longhorn.AttachmentTicket{
			ID:         "attachment-01",
			Type:       longhorn.AttacherTypeSnapshotController,
			NodeID:     TestNode1,
			Parameters: map[string]string{},
			Generation: 0,
		},
		"attachment-02": &longhorn.AttachmentTicket{
			ID:         "attachment-02",
			Type:       longhorn.AttacherTypeCSIAttacher,
			NodeID:     TestNode2,
			Parameters: map[string]string{},
			Generation: 0,
		},
	}
	tc.vol.Status.OwnerID = TestNode1
	tc.vol.Status.State = longhorn.VolumeStateDetached
	tc.copyCurrentToExpect()
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"attachment-01": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-01",
			Satisfied: false,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusFalse, "", ""),
			Generation: 0,
		},
		"attachment-02": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-02",
			Satisfied: false,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusFalse, "", ""),
			Generation: 0,
		},
	}
	tc.expectedVol.Spec.NodeID = TestNode2
	testCases["test case 2: attach: multiple attachments"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	tc = generateVolumeAttachmentTestCaseTemplate(TestVolumeName)
	tc.volAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"attachment-01": &longhorn.AttachmentTicket{
			ID:         "attachment-01",
			Type:       longhorn.AttacherTypeCSIAttacher,
			NodeID:     TestNode1,
			Parameters: map[string]string{},
			Generation: 0,
		},
		"attachment-02": &longhorn.AttachmentTicket{
			ID:         "attachment-02",
			Type:       longhorn.AttacherTypeCSIAttacher,
			NodeID:     TestNode2,
			Parameters: map[string]string{},
			Generation: 0,
		},
	}
	tc.vol.Status.OwnerID = TestNode1
	tc.vol.Status.State = longhorn.VolumeStateDetached
	tc.copyCurrentToExpect()
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"attachment-01": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-01",
			Satisfied: false,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusFalse, "", ""),
			Generation: 0,
		},
		"attachment-02": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-02",
			Satisfied: false,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusFalse, "", ""),
			Generation: 0,
		},
	}
	// AD ticket is selected by priority then name.
	// Since tickets has same priority, we pick ticker with shorter name, attachment-01
	tc.expectedVol.Spec.NodeID = TestNode1
	testCases["test case 3: attach: multiple attachments with same priority level"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	tc = generateVolumeAttachmentTestCaseTemplate(TestVolumeName)
	tc.volAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"attachment-01": &longhorn.AttachmentTicket{
			ID:         "attachment-01",
			Type:       longhorn.AttacherTypeCSIAttacher,
			NodeID:     TestNode1,
			Parameters: map[string]string{},
			Generation: 0,
		},
	}
	tc.vol.Status.OwnerID = TestNode1
	tc.vol.Spec.NodeID = TestNode1
	tc.vol.Status.CurrentNodeID = TestNode1
	tc.vol.Status.State = longhorn.VolumeStateAttached
	tc.copyCurrentToExpect()
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"attachment-01": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-01",
			Satisfied: true,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusTrue, "", ""),
			Generation: 0,
		},
	}
	testCases["test case 4: attach: successfully attached case"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	tc = generateVolumeAttachmentTestCaseTemplate(TestVolumeName)
	tc.volAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"attachment-01": &longhorn.AttachmentTicket{
			ID:     "attachment-01",
			Type:   longhorn.AttacherTypeVolumeRestoreController,
			NodeID: TestNode1,
			Parameters: map[string]string{
				"disableFrontend": "true",
			},
			Generation: 0,
		},
		"attachment-02": &longhorn.AttachmentTicket{
			ID:         "attachment-02",
			Type:       longhorn.AttacherTypeCSIAttacher,
			NodeID:     TestNode1,
			Parameters: map[string]string{},
			Generation: 0,
		},
	}
	tc.vol.Status.OwnerID = TestNode1
	tc.vol.Spec.NodeID = TestNode1
	tc.vol.Status.CurrentNodeID = TestNode1
	tc.vol.Status.State = longhorn.VolumeStateAttached
	tc.copyCurrentToExpect()
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"attachment-01": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-01",
			Satisfied: false,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusFalse,
				longhorn.AttachmentStatusConditionReasonAttachedWithIncompatibleParameters,
				fmt.Sprintf("volume %v has already attached to node %v with incompatible parameters", tc.vol.Name, tc.vol.Status.CurrentNodeID)),
			Generation: 0,
		},
		"attachment-02": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-02",
			Satisfied: true,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusTrue, "", ""),
			Generation: 0,
		},
	}
	testCases["test case 5: attach: fail to attach because the volume is already attached with incompatible parameters"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	tc = generateVolumeAttachmentTestCaseTemplate(TestVolumeName)
	tc.volAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{}
	tc.volAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"attachment-01": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-01",
			Satisfied: true,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusTrue, "", ""),
			Generation: 0,
		},
	}
	tc.vol.Status.OwnerID = TestNode1
	tc.vol.Spec.NodeID = TestNode1
	tc.vol.Status.CurrentNodeID = TestNode1
	tc.vol.Status.State = longhorn.VolumeStateAttached
	tc.copyCurrentToExpect()
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{}
	tc.expectedVol.Spec.NodeID = ""
	testCases["test case 6: detach: basic"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	tc = generateVolumeAttachmentTestCaseTemplate(TestVolumeName)
	tc.volAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"attachment-01": &longhorn.AttachmentTicket{
			ID:         "attachment-01",
			Type:       longhorn.AttacherTypeCSIAttacher,
			NodeID:     TestNode1,
			Parameters: map[string]string{},
			Generation: 0,
		},
	}
	tc.volAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"attachment-01": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-01",
			Satisfied: true,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusTrue, "", ""),
			Generation: 0,
		},
		"attachment-02": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-02",
			Satisfied: true,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusTrue, "", ""),
			Generation: 0,
		},
	}
	tc.vol.Status.OwnerID = TestNode1
	tc.vol.Spec.NodeID = TestNode1
	tc.vol.Status.CurrentNodeID = TestNode1
	tc.vol.Status.State = longhorn.VolumeStateAttached
	tc.copyCurrentToExpect()
	delete(tc.expectedVolAttachment.Status.AttachmentTicketStatuses, "attachment-02")
	testCases["test case 7: detach: detach while there are still other attachments requesting the same node"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	tc = generateVolumeAttachmentTestCaseTemplate(TestVolumeName)
	tc.volAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"attachment-01": &longhorn.AttachmentTicket{
			ID:         "attachment-01",
			Type:       longhorn.AttacherTypeCSIAttacher,
			NodeID:     TestNode1,
			Parameters: map[string]string{},
			Generation: 0,
		},
	}
	tc.vol.Status.OwnerID = TestNode1
	tc.vol.Spec.NodeID = TestNode1
	tc.vol.Spec.DisableFrontend = true
	tc.vol.Status.CurrentNodeID = TestNode1
	tc.vol.Status.State = longhorn.VolumeStateAttached
	tc.copyCurrentToExpect()
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"attachment-01": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-01",
			Satisfied: true,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusTrue, "", ""),
			Generation: 0,
		},
	}
	tc.expectedVol.Spec.NodeID = ""
	tc.expectedVol.Spec.DisableFrontend = false
	testCases["test case 8: detach: the current attachment requesting the same node but with incompatible parameters"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	tc = generateVolumeAttachmentTestCaseTemplate(TestVolumeName)
	tc.volAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"attachment-01": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-01",
			Satisfied: true,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusTrue, "", ""),
			Generation: 0,
		},
	}
	tc.volAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"attachment-01": &longhorn.AttachmentTicket{
			ID:         "attachment-01",
			Type:       longhorn.AttacherTypeCSIAttacher,
			NodeID:     TestNode2,
			Parameters: map[string]string{},
			Generation: 1,
		},
	}
	tc.vol.Status.OwnerID = TestNode1
	tc.vol.Spec.NodeID = TestNode1
	tc.vol.Spec.DisableFrontend = false
	tc.vol.Status.CurrentNodeID = TestNode1
	tc.vol.Status.State = longhorn.VolumeStateAttached
	tc.copyCurrentToExpect()
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"attachment-01": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-01",
			Satisfied: false,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusFalse, "",
				fmt.Sprintf("the volume is currently attached to different node %v ", TestNode1)),
			Generation: 1,
		},
	}
	tc.expectedVol.Spec.NodeID = ""
	tc.expectedVol.Spec.DisableFrontend = false
	testCases["test case 9: test ticket's generation: attachment ticket change its node ID"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	tc = generateVolumeAttachmentTestCaseTemplate(TestVolumeName)
	tc.volAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"attachment-01": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-01",
			Satisfied: true,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusTrue, "", ""),
			Generation: 0,
		},
	}
	tc.volAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"attachment-01": &longhorn.AttachmentTicket{
			ID:         "attachment-01",
			Type:       longhorn.AttacherTypeSnapshotController,
			NodeID:     TestNode1,
			Parameters: map[string]string{},
			Generation: 0,
		},
		"attachment-02": &longhorn.AttachmentTicket{
			ID:         "attachment-02",
			Type:       longhorn.AttacherTypeCSIAttacher,
			NodeID:     TestNode2,
			Parameters: map[string]string{},
			Generation: 0,
		},
	}
	tc.vol.Status.OwnerID = TestNode1
	tc.vol.Spec.NodeID = TestNode1
	tc.vol.Spec.DisableFrontend = false
	tc.vol.Status.CurrentNodeID = TestNode1
	tc.vol.Status.State = longhorn.VolumeStateAttached
	tc.copyCurrentToExpect()
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"attachment-01": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-01",
			Satisfied: true,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusTrue, "", ""),
			Generation: 0,
		},
		"attachment-02": &longhorn.AttachmentTicketStatus{
			ID:        "attachment-02",
			Satisfied: false,
			Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
				longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusFalse, "",
				fmt.Sprintf("the volume is currently attached to different node %v ", TestNode1)),
			Generation: 0,
		},
	}
	tc.expectedVol.Spec.NodeID = ""
	testCases["test case 10: ticket with higher priority interrupts ticket with lower priority"] = tc
	///////////////////////////////////////////////////////////////////

	for name, tc := range testCases {
		//uncomment this block to test individual test case
		//if name != "test case 10: ticket with higher priority interrupts ticket with lower priority" {
		//	continue
		//}
		fmt.Printf("testing %v\n", name)
		s.runVolumeAttachmentTestCase(c, tc)
	}

}

func (s *TestSuite) TestIsVolumeAvailableOnNodeV2RequiresReadyEngineFrontend(c *C) {
	datastore.SkipListerCheck = true

	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)

	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	logger := logrus.StandardLogger()

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	engineIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()
	engineFrontendIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineFrontends().Informer().GetIndexer()

	vac, err := NewLonghornVolumeAttachmentController(logger, ds, scheme.Scheme, kubeClient, TestOwnerID1, TestNamespace)
	c.Assert(err, IsNil)

	for index := range vac.cacheSyncs {
		vac.cacheSyncs[index] = alwaysReady
	}

	v := newVolume(TestVolumeName, 1)
	v.Spec.DataEngine = longhorn.DataEngineTypeV2

	createdVolume, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), v, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = volumeIndexer.Add(createdVolume)
	c.Assert(err, IsNil)

	e := newEngineForVolume(v)
	e.Spec.DataEngine = longhorn.DataEngineTypeV2
	e.Spec.NodeID = TestNode2
	e.Spec.DesireState = longhorn.InstanceStateRunning
	e.Status.CurrentState = longhorn.InstanceStateRunning
	e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{
		"replica-1": longhorn.ReplicaModeRW,
	}

	createdEngine, err := lhClient.LonghornV1beta2().Engines(TestNamespace).Create(context.TODO(), e, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = engineIndexer.Add(createdEngine)
	c.Assert(err, IsNil)

	c.Assert(vac.isVolumeAvailableOnNode(v.Name, TestNode1), Equals, false)
	c.Assert(vac.isVolumeAvailableOnNode(v.Name, TestNode2), Equals, false)

	ef := newEngineFrontendForVolume(v, e.Name, TestNode1, "")
	ef.Spec.DesireState = longhorn.InstanceStateRunning
	createdEngineFrontend, err := lhClient.LonghornV1beta2().EngineFrontends(TestNamespace).Create(context.TODO(), ef, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = engineFrontendIndexer.Add(createdEngineFrontend)
	c.Assert(err, IsNil)

	c.Assert(vac.isVolumeAvailableOnNode(v.Name, TestNode1), Equals, false)
	c.Assert(vac.isVolumeAvailableOnNode(v.Name, TestNode2), Equals, false)

	createdEngineFrontend.Status.CurrentState = longhorn.InstanceStateRunning
	createdEngineFrontend.Status.Endpoint = "/dev/longhorn/" + v.Name
	err = engineFrontendIndexer.Update(createdEngineFrontend)
	c.Assert(err, IsNil)

	c.Assert(vac.isVolumeAvailableOnNode(v.Name, TestNode1), Equals, true)
	c.Assert(vac.isVolumeAvailableOnNode(v.Name, TestNode2), Equals, false)

	createdEngineFrontend.Status.Endpoint = ""
	createdEngineFrontend.Spec.DisableFrontend = true
	err = engineFrontendIndexer.Update(createdEngineFrontend)
	c.Assert(err, IsNil)

	c.Assert(vac.isVolumeAvailableOnNode(v.Name, TestNode1), Equals, true)

	createdEngineFrontend.Spec.DisableFrontend = false
	createdEngineFrontend.Spec.Frontend = longhorn.VolumeFrontendEmpty
	err = engineFrontendIndexer.Update(createdEngineFrontend)
	c.Assert(err, IsNil)

	c.Assert(vac.isVolumeAvailableOnNode(v.Name, TestNode1), Equals, true)
}

func (s *TestSuite) runVolumeAttachmentTestCase(c *C, tc *volumeAttachmentTestCase) {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)

	ds := datastore.NewDataStoreForGlobal(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	logger := logrus.StandardLogger()

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	volumeAttachmentIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().VolumeAttachments().Informer().GetIndexer()
	nodeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()

	vac, err := NewLonghornVolumeAttachmentController(logger, ds, scheme.Scheme, kubeClient, TestOwnerID1, TestNamespace)
	c.Assert(err, IsNil)

	fakeRecorder := record.NewFakeRecorder(100)
	vac.eventRecorder = fakeRecorder
	for index := range vac.cacheSyncs {
		vac.cacheSyncs[index] = alwaysReady
	}

	// Seed the data.
	// Need to put it into both fakeclientset and Indexer because
	// the fake client doesn't work well with informers.
	// See details at https://github.com/kubernetes/kubernetes/issues/95372
	vol, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), tc.vol, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = volumeIndexer.Add(vol)
	c.Assert(err, IsNil)

	volAttachment, err := lhClient.LonghornV1beta2().VolumeAttachments(TestNamespace).Create(context.TODO(), tc.volAttachment, metav1.CreateOptions{})
	c.Assert(err, IsNil)
	err = volumeAttachmentIndexer.Add(volAttachment)
	c.Assert(err, IsNil)

	for _, n := range tc.nodes {
		createdNode, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), n, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = nodeIndexer.Add(createdNode)
		c.Assert(err, IsNil)
	}

	engineIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()
	for _, e := range tc.engines {
		createdEngine, err := lhClient.LonghornV1beta2().Engines(TestNamespace).Create(context.TODO(), e, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = engineIndexer.Add(createdEngine)
		c.Assert(err, IsNil)
	}

	engineFrontendIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineFrontends().Informer().GetIndexer()
	for _, ef := range tc.engineFrontends {
		createdEngineFrontend, err := lhClient.LonghornV1beta2().EngineFrontends(TestNamespace).Create(context.TODO(), ef, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = engineFrontendIndexer.Add(createdEngineFrontend)
		c.Assert(err, IsNil)
	}

	////////////////////////////////////
	// main test func
	err = vac.syncHandler(getKey(volAttachment, c))
	c.Assert(err, IsNil)
	///////////////////////////////////

	retVol, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Get(context.TODO(), tc.vol.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	c.Assert(retVol.Spec, DeepEquals, tc.expectedVol.Spec)

	retVolAttachment, err := lhClient.LonghornV1beta2().VolumeAttachments(TestNamespace).Get(context.TODO(), tc.volAttachment.Name, metav1.GetOptions{})
	c.Assert(err, IsNil)
	// mask timestamps
	for _, ticketStatus := range retVolAttachment.Status.AttachmentTicketStatuses {
		for ctype, condition := range ticketStatus.Conditions {
			condition.LastTransitionTime = ""
			ticketStatus.Conditions[ctype] = condition
		}
	}
	c.Assert(retVolAttachment.Status, DeepEquals, tc.expectedVolAttachment.Status)

}

func (s *TestSuite) TestVolumeMigrationStartNodeReadiness(c *C) {
	testCases := map[string]*volumeAttachmentTestCase{}

	// shared builder: migratable vol attached to TestNode1, CSI tickets for both nodes
	makeMigrationTC := func() *volumeAttachmentTestCase {
		tc := generateVolumeAttachmentTestCaseTemplate(TestVolumeName)
		tc.vol.Spec.Migratable = true
		tc.vol.Spec.AccessMode = longhorn.AccessModeReadWriteMany
		tc.vol.Spec.NodeID = TestNode1
		tc.vol.Status.State = longhorn.VolumeStateAttached
		tc.vol.Status.CurrentNodeID = TestNode1
		tc.volAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
			"csi-node1": {
				ID:         "csi-node1",
				Type:       longhorn.AttacherTypeCSIAttacher,
				NodeID:     TestNode1,
				Parameters: map[string]string{},
			},
			"csi-node2": {
				ID:         "csi-node2",
				Type:       longhorn.AttacherTypeCSIAttacher,
				NodeID:     TestNode2,
				Parameters: map[string]string{},
			},
		}
		return tc
	}

	// shared expected ticket statuses: csi-node1 satisfied, csi-node2 not (migration not
	// confirmed by VolumeController yet, so CurrentMigrationNodeID is still "")
	expectedTicketStatuses := func() map[string]*longhorn.AttachmentTicketStatus {
		return map[string]*longhorn.AttachmentTicketStatus{
			"csi-node1": {
				ID:        "csi-node1",
				Satisfied: true,
				Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
					longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusTrue, "", ""),
			},
			"csi-node2": {
				ID:        "csi-node2",
				Satisfied: false,
				Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
					longhorn.AttachmentStatusConditionTypeSatisfied, longhorn.ConditionStatusFalse, "",
					fmt.Sprintf("the volume is currently attached to different node %v ", TestNode1)),
			},
		}
	}

	///////////////////////////////////////////////////////////////////
	// Case A: target node not found in etcd -> IsNodeDownOrDeletedOrMissingManager=true -> blocked
	tc := makeMigrationTC()
	tc.nodes = []*longhorn.Node{
		newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, ""),
		// TestNode2 intentionally absent
	}
	tc.copyCurrentToExpect()
	tc.expectedVol.Spec.MigrationNodeID = ""
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = expectedTicketStatuses()
	testCases["migration blocked: target node absent"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	// Case B: target node Ready=False, Reason=ManagerPodMissing -> blocked
	tc = makeMigrationTC()
	tc.nodes = []*longhorn.Node{
		newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, ""),
		newNode(TestNode2, TestNamespace, false, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonManagerPodMissing)),
	}
	tc.copyCurrentToExpect()
	tc.expectedVol.Spec.MigrationNodeID = ""
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = expectedTicketStatuses()
	testCases["migration blocked: target node ManagerPodMissing"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	// Case C: target node Ready=False, Reason=KubernetesNodeNotReady -> blocked
	tc = makeMigrationTC()
	tc.nodes = []*longhorn.Node{
		newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, ""),
		newNode(TestNode2, TestNamespace, false, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonKubernetesNodeNotReady)),
	}
	tc.copyCurrentToExpect()
	tc.expectedVol.Spec.MigrationNodeID = ""
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = expectedTicketStatuses()
	testCases["migration blocked: target node KubernetesNodeNotReady"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	// Case D: target node Ready=False, Reason=ManagerPodDown (not in IsNodeDownOrDeletedOrMissingManager).
	// IsNodeDownOrDeletedOrMissingManager returns false, so only the Ready=True gate blocks this.
	// This test will FAIL before the Ready=True gate is added and PASS after.
	tc = makeMigrationTC()
	tc.nodes = []*longhorn.Node{
		newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, ""),
		newNode(TestNode2, TestNamespace, false, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonManagerPodDown)),
	}
	tc.copyCurrentToExpect()
	tc.expectedVol.Spec.MigrationNodeID = ""
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = expectedTicketStatuses()
	testCases["migration blocked: target node Ready=False transitional (ManagerPodDown)"] = tc
	///////////////////////////////////////////////////////////////////

	///////////////////////////////////////////////////////////////////
	// Case E: target node Ready=True -> migration proceeds
	tc = makeMigrationTC()
	tc.nodes = []*longhorn.Node{
		newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, ""),
		newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, ""),
	}
	tc.copyCurrentToExpect()
	tc.expectedVol.Spec.MigrationNodeID = TestNode2
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = expectedTicketStatuses()
	testCases["migration proceeds: target node Ready=True"] = tc
	///////////////////////////////////////////////////////////////////

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)
		s.runVolumeAttachmentTestCase(c, tc)
	}
}

type migrationTargetState int

const (
	migrationTargetDeleting migrationTargetState = iota
	migrationTargetPreparing
	migrationTargetReady
)

const migratingTicketSatisfiedMsg = "The migrating attachment ticket is satisfied"

// newMigrationTestCase builds a migratable volume attached to TestNode1 with CSI tickets for TestNode1 and TestNode2.
// The active (source) engine on TestNode1 has three replicas in its spec; modes and purge status are set by the caller.
// If migrationStarted is true, the migration to TestNode2 has already started.
func newMigrationTestCase(dataEngine longhorn.DataEngineType, modes map[string]longhorn.ReplicaMode, purging, migrationStarted bool) *volumeAttachmentTestCase {
	tc := generateVolumeAttachmentTestCaseTemplate(TestVolumeName)
	tc.vol.Spec.DataEngine = dataEngine
	tc.vol.Spec.Migratable = true
	tc.vol.Spec.AccessMode = longhorn.AccessModeReadWriteMany
	tc.vol.Spec.NodeID = TestNode1
	tc.vol.Status.State = longhorn.VolumeStateAttached
	tc.vol.Status.CurrentNodeID = TestNode1
	if migrationStarted {
		tc.vol.Spec.MigrationNodeID = TestNode2
		tc.vol.Status.CurrentMigrationNodeID = TestNode2
	}
	tc.volAttachment.Spec.AttachmentTickets = map[string]*longhorn.AttachmentTicket{
		"csi-node1": {ID: "csi-node1", Type: longhorn.AttacherTypeCSIAttacher, NodeID: TestNode1, Parameters: map[string]string{}},
		"csi-node2": {ID: "csi-node2", Type: longhorn.AttacherTypeCSIAttacher, NodeID: TestNode2, Parameters: map[string]string{}},
	}
	tc.nodes = []*longhorn.Node{
		newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, ""),
		newNode(TestNode2, TestNamespace, true, longhorn.ConditionStatusTrue, ""),
	}

	source := newEngineForVolume(tc.vol)
	source.Spec.DataEngine = dataEngine
	source.Spec.NodeID = TestNode1
	source.Spec.DesireState = longhorn.InstanceStateRunning
	source.Status.CurrentState = longhorn.InstanceStateRunning
	source.Spec.ReplicaAddressMap = map[string]string{
		"replica-1": "10.0.0.1:10000",
		"replica-2": "10.0.0.2:10000",
		"replica-3": "10.0.0.3:10000",
	}
	source.Status.ReplicaModeMap = modes
	if purging {
		source.Status.PurgeStatus = map[string]*longhorn.PurgeStatus{"tcp://10.0.0.1:10000": {IsPurging: true}}
	}
	tc.engines = []*longhorn.Engine{source}
	return tc
}

func allRWReplicaModes() map[string]longhorn.ReplicaMode {
	return map[string]longhorn.ReplicaMode{
		"replica-1": longhorn.ReplicaModeRW,
		"replica-2": longhorn.ReplicaModeRW,
		"replica-3": longhorn.ReplicaModeRW,
	}
}

// pendingReplicaModes returns modes where replica-3 is in the engine spec but has no mode yet (the state in #14153).
func pendingReplicaModes() map[string]longhorn.ReplicaMode {
	return map[string]longhorn.ReplicaMode{
		"replica-1": longhorn.ReplicaModeRW,
		"replica-2": longhorn.ReplicaModeRW,
	}
}

// addMigrationTargetEngine appends a non-active migration engine, and for a ready v2 target the engine frontends.
func addMigrationTargetEngine(tc *volumeAttachmentTestCase, state migrationTargetState) {
	dataEngine := tc.vol.Spec.DataEngine
	migration := newEngineForVolume(tc.vol)
	migration.Spec.DataEngine = dataEngine
	migration.Spec.Active = false
	switch state {
	case migrationTargetDeleting:
		now := metav1.Now()
		migration.DeletionTimestamp = &now
		migration.Finalizers = []string{longhorn.SchemeGroupVersion.Group}
		migration.Spec.NodeID = TestNode2
	case migrationTargetPreparing:
		migration.Spec.NodeID = "" // just created, not started yet
	case migrationTargetReady:
		migration.Spec.NodeID = TestNode2
		migration.Spec.DesireState = longhorn.InstanceStateRunning
		migration.Status.CurrentState = longhorn.InstanceStateRunning
		migration.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{"replica-1-migration": longhorn.ReplicaModeRW}
	}
	tc.engines = append(tc.engines, migration)

	if types.IsDataEngineV2(dataEngine) && state == migrationTargetReady {
		source := tc.engines[0]
		sourceEF := newEngineFrontendForVolume(tc.vol, source.Name, TestNode1, "")
		sourceEF.Spec.DesireState = longhorn.InstanceStateRunning
		sourceEF.Status.CurrentState = longhorn.InstanceStateRunning
		sourceEF.Status.Endpoint = "/dev/longhorn/" + tc.vol.Name
		targetEF := newEngineFrontendForVolume(tc.vol, migration.Name, TestNode2, sourceEF.Name)
		targetEF.Spec.DesireState = longhorn.InstanceStateRunning
		targetEF.Status.CurrentState = longhorn.InstanceStateRunning
		targetEF.Status.Endpoint = "/dev/longhorn/" + tc.vol.Name
		tc.engineFrontends = append(tc.engineFrontends, sourceEF, targetEF)
	}
}

func migrationTicketStatus(id string, satisfied bool, reason, message string) *longhorn.AttachmentTicketStatus {
	conditionStatus := longhorn.ConditionStatusFalse
	if satisfied {
		conditionStatus = longhorn.ConditionStatusTrue
	}
	return &longhorn.AttachmentTicketStatus{
		ID:        id,
		Satisfied: satisfied,
		Conditions: types.SetConditionWithoutTimestamp([]longhorn.Condition{},
			longhorn.AttachmentStatusConditionTypeSatisfied, conditionStatus, reason, message),
	}
}

func sourceTicketSatisfied() *longhorn.AttachmentTicketStatus {
	return migrationTicketStatus("csi-node1", true, "", "")
}

func targetTicketWaitingForMigration() *longhorn.AttachmentTicketStatus {
	return migrationTicketStatus("csi-node2", false, "", fmt.Sprintf("waiting for volume to migrate to node %v", TestNode2))
}

// TestVolumeMigrationBlockedByActiveEngine verifies that a live migration does not start while the active engine has
// replicas pending or in rebuilding, or a snapshot purge in progress, and that the target ticket reports why.
// See https://github.com/longhorn/longhorn/issues/14153
func (s *TestSuite) TestVolumeMigrationBlockedByActiveEngine(c *C) {
	testCases := map[string]*volumeAttachmentTestCase{}

	rebuildingMsg := func(engineName string) string {
		return fmt.Sprintf("waiting to migrate the volume to node %v: replicas [replica-3] of engine %v are pending or in rebuilding", TestNode2, engineName)
	}

	// Replica in the engine spec without a mode -> deferred
	tc := newMigrationTestCase(longhorn.DataEngineTypeV1, pendingReplicaModes(), false, false)
	tc.copyCurrentToExpect()
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"csi-node1": sourceTicketSatisfied(),
		"csi-node2": migrationTicketStatus("csi-node2", false, longhorn.AttachmentStatusConditionReasonMigrationPending, rebuildingMsg(tc.engines[0].Name)),
	}
	testCases["deferred: replica in engine spec without mode"] = tc

	// Replica WO -> deferred
	modes := allRWReplicaModes()
	modes["replica-3"] = longhorn.ReplicaModeWO
	tc = newMigrationTestCase(longhorn.DataEngineTypeV1, modes, false, false)
	tc.copyCurrentToExpect()
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"csi-node1": sourceTicketSatisfied(),
		"csi-node2": migrationTicketStatus("csi-node2", false, longhorn.AttachmentStatusConditionReasonMigrationPending, rebuildingMsg(tc.engines[0].Name)),
	}
	testCases["deferred: replica WO"] = tc

	// All RW but a snapshot purge is in progress (e.g., right after a rebuild) -> deferred
	tc = newMigrationTestCase(longhorn.DataEngineTypeV1, allRWReplicaModes(), true, false)
	tc.copyCurrentToExpect()
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"csi-node1": sourceTicketSatisfied(),
		"csi-node2": migrationTicketStatus("csi-node2", false, longhorn.AttachmentStatusConditionReasonMigrationPending,
			fmt.Sprintf("waiting to migrate the volume to node %v: snapshot purge is in progress for engine %v", TestNode2, tc.engines[0].Name)),
	}
	testCases["deferred: snapshot purge in progress"] = tc

	// All RW and no purge -> migration starts
	tc = newMigrationTestCase(longhorn.DataEngineTypeV1, allRWReplicaModes(), false, false)
	tc.copyCurrentToExpect()
	tc.expectedVol.Spec.MigrationNodeID = TestNode2
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"csi-node1": sourceTicketSatisfied(),
		"csi-node2": migrationTicketStatus("csi-node2", false, "", fmt.Sprintf("the volume is currently attached to different node %v ", TestNode1)),
	}
	testCases["starts: all replicas RW and no purge"] = tc

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)
		s.runVolumeAttachmentTestCase(c, tc)
	}
}

// TestVolumeMigrationRollbackForBlockedSource covers the rollback for a migration that started while the active engine
// was blocked. The decision depends only on persisted state (active engine blocked, target ticket never satisfied,
// target not usable), and the target ticket is never satisfied during a rollback while confirmation still satisfies it.
func (s *TestSuite) TestVolumeMigrationRollbackForBlockedSource(c *C) {
	testCases := map[string]*volumeAttachmentTestCase{}

	expectRolledBack := func(tc *volumeAttachmentTestCase) {
		tc.copyCurrentToExpect()
		tc.expectedVol.Spec.MigrationNodeID = ""
		tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
			"csi-node1": sourceTicketSatisfied(),
			"csi-node2": targetTicketWaitingForMigration(),
		}
	}
	expectKept := func(tc *volumeAttachmentTestCase, targetSatisfied bool) {
		tc.copyCurrentToExpect()
		target := targetTicketWaitingForMigration()
		if targetSatisfied {
			target = migrationTicketStatus("csi-node2", true, "", migratingTicketSatisfiedMsg)
		}
		tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
			"csi-node1": sourceTicketSatisfied(),
			"csi-node2": target,
		}
	}

	// Blocked source, target never published and not usable -> roll back, regardless of migration engine CRs
	tc := newMigrationTestCase(longhorn.DataEngineTypeV1, pendingReplicaModes(), false, true)
	expectRolledBack(tc)
	testCases["blocked source, no migration engine -> roll back"] = tc

	tc = newMigrationTestCase(longhorn.DataEngineTypeV1, pendingReplicaModes(), false, true)
	addMigrationTargetEngine(tc, migrationTargetDeleting)
	expectRolledBack(tc)
	testCases["blocked source, deleting migration engine -> roll back"] = tc

	tc = newMigrationTestCase(longhorn.DataEngineTypeV1, pendingReplicaModes(), false, true)
	addMigrationTargetEngine(tc, migrationTargetPreparing)
	expectRolledBack(tc)
	testCases["blocked source, migration engine being prepared -> roll back"] = tc

	tc = newMigrationTestCase(longhorn.DataEngineTypeV1, allRWReplicaModes(), true, true)
	addMigrationTargetEngine(tc, migrationTargetPreparing)
	expectRolledBack(tc)
	testCases["purging source, migration engine being prepared -> roll back"] = tc

	// Unblocked source -> never roll back
	tc = newMigrationTestCase(longhorn.DataEngineTypeV1, allRWReplicaModes(), false, true)
	addMigrationTargetEngine(tc, migrationTargetPreparing)
	expectKept(tc, false)
	testCases["unblocked source, migration engine being prepared -> no rollback"] = tc

	// Usable target (v1, and v2 with a ready target engine frontend) -> never roll back, ticket satisfied
	for _, dataEngine := range []longhorn.DataEngineType{longhorn.DataEngineTypeV1, longhorn.DataEngineTypeV2} {
		tc = newMigrationTestCase(dataEngine, pendingReplicaModes(), false, true)
		addMigrationTargetEngine(tc, migrationTargetReady)
		expectKept(tc, true)
		testCases[fmt.Sprintf("%v: blocked source, usable target -> no rollback", dataEngine)] = tc
	}

	// Target ticket already satisfied (CSI may have published it), target temporarily not usable -> never roll back,
	// and the ticket stays satisfied while the migration to the target continues
	tc = newMigrationTestCase(longhorn.DataEngineTypeV1, pendingReplicaModes(), false, true)
	addMigrationTargetEngine(tc, migrationTargetPreparing)
	tc.volAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"csi-node2": migrationTicketStatus("csi-node2", true, "", migratingTicketSatisfiedMsg),
	}
	expectKept(tc, true)
	testCases["blocked source, target ticket previously satisfied -> no rollback"] = tc

	// Rollback already chosen, target became ready before cleanup completes -> ticket stays unsatisfied
	tc = newMigrationTestCase(longhorn.DataEngineTypeV1, pendingReplicaModes(), false, true)
	addMigrationTargetEngine(tc, migrationTargetReady)
	tc.vol.Spec.MigrationNodeID = ""
	expectKept(tc, false)
	testCases["rollback in progress, target ready before cleanup -> ticket stays unsatisfied"] = tc

	// Confirmation is preserved: source ticket gone, target ready -> Spec.NodeID switches, ticket satisfied
	tc = newMigrationTestCase(longhorn.DataEngineTypeV1, allRWReplicaModes(), false, true)
	addMigrationTargetEngine(tc, migrationTargetReady)
	delete(tc.volAttachment.Spec.AttachmentTickets, "csi-node1")
	tc.copyCurrentToExpect()
	tc.expectedVol.Spec.NodeID = TestNode2
	tc.expectedVol.Spec.MigrationNodeID = ""
	tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
		"csi-node2": migrationTicketStatus("csi-node2", true, "", migratingTicketSatisfiedMsg),
	}
	testCases["confirmation -> target ticket satisfied"] = tc

	for name, tc := range testCases {
		fmt.Printf("testing %v\n", name)
		s.runVolumeAttachmentTestCase(c, tc)
	}
}

// TestVolumeMigrationRollbackIgnoresReplacementEngines reproduces the #14153 loop as seen by the VolumeAttachment
// controller: processMigration replaces each deleting migration engine before the attachment is reconciled, so the
// cache never shows "no migration engine". The rollback must still happen on every reconcile.
func (s *TestSuite) TestVolumeMigrationRollbackIgnoresReplacementEngines(c *C) {
	for generation := 1; generation <= 3; generation++ {
		fmt.Printf("testing generation %v: %v deleting migration engine(s) + 1 replacement\n", generation, generation)
		tc := newMigrationTestCase(longhorn.DataEngineTypeV1, pendingReplicaModes(), false, true)
		for i := 0; i < generation; i++ {
			addMigrationTargetEngine(tc, migrationTargetDeleting)
		}
		addMigrationTargetEngine(tc, migrationTargetPreparing) // replacement already in the cache
		tc.copyCurrentToExpect()
		tc.expectedVol.Spec.MigrationNodeID = ""
		tc.expectedVolAttachment.Status.AttachmentTicketStatuses = map[string]*longhorn.AttachmentTicketStatus{
			"csi-node1": sourceTicketSatisfied(),
			"csi-node2": targetTicketWaitingForMigration(),
		}
		s.runVolumeAttachmentTestCase(c, tc)
	}
}

func newVolumeAttachment(name string) *longhorn.VolumeAttachment {
	return &longhorn.VolumeAttachment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: TestNamespace,
			Finalizers: []string{
				longhorn.SchemeGroupVersion.Group,
			},
			Labels: map[string]string{
				"longhornvolume": name,
			},
		},
		Spec: longhorn.VolumeAttachmentSpec{
			Volume: name,
		},
		Status: longhorn.VolumeAttachmentStatus{},
	}
}

func generateVolumeAttachmentTestCaseTemplate(name string) *volumeAttachmentTestCase {
	return &volumeAttachmentTestCase{
		volAttachment: newVolumeAttachment(name),
		vol:           newVolume(name, 1),
	}
}
