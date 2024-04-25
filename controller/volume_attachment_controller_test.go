package controller

import (
	"context"
	"fmt"

	"github.com/sirupsen/logrus"

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

	. "gopkg.in/check.v1"
)

type volumeAttachmentTestCase struct {
	volAttachment *longhorn.VolumeAttachment
	vol           *longhorn.Volume

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

func (s *TestSuite) runVolumeAttachmentTestCase(c *C, tc *volumeAttachmentTestCase) {
	kubeClient := fake.NewSimpleClientset()
	lhClient := lhfake.NewSimpleClientset()
	extensionsClient := apiextensionsfake.NewSimpleClientset()

	informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, 0)

	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)
	logger := logrus.StandardLogger()

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	volumeAttachmentIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().VolumeAttachments().Informer().GetIndexer()

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
