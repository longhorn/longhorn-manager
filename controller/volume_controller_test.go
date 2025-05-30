package controller

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/record"
	"k8s.io/kubernetes/pkg/controller"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/backupstore"

	imutil "github.com/longhorn/longhorn-instance-manager/pkg/util"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"

	. "gopkg.in/check.v1"
)

func getVolumeLabelSelector(volumeName string) string {
	return "longhornvolume=" + volumeName
}

func setVolumeConditionWithoutTimestamp(originConditions []longhorn.Condition, conditionType string, conditionValue longhorn.ConditionStatus, reason, message string) []longhorn.Condition {
	return types.SetConditionWithoutTimestamp(originConditions, conditionType, conditionValue, reason, message)
}

func initSettingsNameValue(name, value string) *longhorn.Setting {
	setting := &longhorn.Setting{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
		},
		Value: value,
	}
	return setting
}

func newTestVolumeController(lhClient *lhfake.Clientset, kubeClient *fake.Clientset, extensionsClient *apiextensionsfake.Clientset,
	informerFactories *util.InformerFactories, controllerID string) (*VolumeController, error) {
	ds := datastore.NewDataStore(TestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	proxyConnCounter := util.NewAtomicCounter()

	logger := logrus.StandardLogger()

	vc, err := NewVolumeController(logger, ds, scheme.Scheme, kubeClient, TestNamespace, controllerID, TestShareManagerImage, proxyConnCounter)
	if err != nil {
		return nil, err
	}

	fakeRecorder := record.NewFakeRecorder(100)
	vc.eventRecorder = fakeRecorder
	for index := range vc.cacheSyncs {
		vc.cacheSyncs[index] = alwaysReady
	}
	vc.nowHandler = getTestNow

	return vc, nil
}

type VolumeTestCase struct {
	volume      *longhorn.Volume
	engines     map[string]*longhorn.Engine
	replicas    map[string]*longhorn.Replica
	nodes       []*longhorn.Node
	engineImage *longhorn.EngineImage

	expectVolume   *longhorn.Volume
	expectEngines  map[string]*longhorn.Engine
	expectReplicas map[string]*longhorn.Replica

	replicaNodeSoftAntiAffinity                 string
	volumeAutoSalvage                           string
	replicaReplenishmentWaitInterval            string
	allowVolumeCreationWithDegradedAvailability string
}

func (s *TestSuite) TestVolumeLifeCycle(c *C) {
	testBackupURL := backupstore.EncodeBackupURL(TestBackupName, TestVolumeName, TestBackupTarget)

	var tc *VolumeTestCase
	testCases := map[string]*VolumeTestCase{}

	// We have to skip lister check for unit tests
	// Because the changes written through the API won't be reflected in the listers
	datastore.SkipListerCheck = true

	// unable to create volume because no node to schedule, and no replica CRDs exist initially.
	// Controller attempts to create replicas but fails precheck.
	tc = generateVolumeTestCaseTemplate()
	for i := range tc.nodes {
		tc.nodes[i].Spec.AllowScheduling = false // Nodes are made unschedulable
	}
	tc.copyCurrentToExpect() // This populates tc.expectVolume, tc.expectEngines, tc.expectReplicas
	                        // with copies of the template's defaults.

	// Scenario: Controller starts with no existing replica or engine CRDs.
	tc.replicas = nil // Controller will find no existing replica CRDs.
	tc.engines = nil  // Controller will find no existing engine CRDs; it will create one.

	// Adjust expectations for what the controller will achieve/create:
	tc.expectVolume.Status.State = longhorn.VolumeStateCreating
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image // Should be set by controller.
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessFaulted // Controller marks it faulted.

	// Expected message when prechecks fail, preventing any replicas from being created (e.g., no schedulable nodes).
	// The controller sees 0 replica objects but expects 2, so it reports that it needs to create more.
	expectedMessageReplicaCreationFailure := "Replica scheduling issues: replica count mismatch (expected 2, have 0 created replica objects); replenishment needed for 2 replica(s)"
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.expectVolume.Status.Conditions,
		longhorn.VolumeConditionTypeScheduled, longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonReplicaSchedulingFailure,
		expectedMessageReplicaCreationFailure)

	// Controller WILL create an engine CRD.
	// tc.expectEngines was populated by copyCurrentToExpect() with one engine template.
	// Adjust its spec for a newly created engine.
	var firstExpectedEngineCreationFailure *longhorn.Engine
	for _, e := range tc.expectEngines { // Should be only one from the template
		firstExpectedEngineCreationFailure = e
		break
	}
	if firstExpectedEngineCreationFailure != nil {
		firstExpectedEngineCreationFailure.Spec.Image = tc.expectVolume.Status.CurrentImage // Match volume's current image.
		firstExpectedEngineCreationFailure.Spec.DesireState = longhorn.InstanceStateStopped
		firstExpectedEngineCreationFailure.Spec.Active = true
		// Other fields like VolumeName, VolumeSize are from newEngineForVolume via template.
	} else {
		panic("Expected tc.expectEngines to have a template engine after copyCurrentToExpect for 'replica creation failure'")
	}

	// Controller will ATTEMPT to create replica CRDs but will fail precheck.
	// Therefore, no replica CRDs should exist in the datastore after the sync.
	tc.expectReplicas = map[string]*longhorn.Replica{} // Expect an empty map of replicas.

	testCases["volume create - replica creation failure"] = tc

	// unable to create volume because no node to schedule, and no replica CRDs exist initially.
	// Controller attempts to create replicas but fails precheck.
	tc = generateVolumeTestCaseTemplate()
	for i := range tc.nodes {
		tc.nodes[i].Spec.AllowScheduling = false // Nodes are made unschedulable
	}
	tc.copyCurrentToExpect() // This populates tc.expectVolume, tc.expectEngines, tc.expectReplicas
	                        // with copies of the template's defaults.

	// Scenario: Controller starts with no existing replica or engine CRDs.
	tc.replicas = nil // Controller will find no existing replica CRDs.
	tc.engines = nil  // Controller will find no existing engine CRDs; it will create one.

	// Adjust expectations for what the controller will achieve/create:
	tc.expectVolume.Status.State = longhorn.VolumeStateCreating
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image // Should be set by controller.
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessFaulted // Controller marks it faulted.
      
	// Expected message when no replicas can be created because of precheck failures (e.g., no schedulable nodes).
	// The controller finds 0 existing replica objects but expects 2, so it knows more replicas are needed.
	// The message parts are sorted alphabetically by the controller before they are joined.
	var part1CreationFailureMsg string // Use var for clarity or ensure := is in a fresh scope
	var part2CreationFailureMsg string
	var creationFailurePartsList []string
	var expectedMsgReplicaCreationFailure string

	part1CreationFailureMsg = fmt.Sprintf("replica count mismatch (expected %d, have %d created replica objects)", tc.volume.Spec.NumberOfReplicas, 0)
	part2CreationFailureMsg = fmt.Sprintf("replenishment needed for %d replica(s)", tc.volume.Spec.NumberOfReplicas)
	creationFailurePartsList = []string{part1CreationFailureMsg, part2CreationFailureMsg}
	sort.Strings(creationFailurePartsList) // Make sure test matches controller's sorting
	expectedMsgReplicaCreationFailure = "Replica scheduling issues: " + strings.Join(creationFailurePartsList, "; ")

	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.expectVolume.Status.Conditions,
		longhorn.VolumeConditionTypeScheduled, longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonReplicaSchedulingFailure,
		expectedMsgReplicaCreationFailure)

	// Controller WILL create an engine CRD.
	// tc.expectEngines was populated by copyCurrentToExpect() with one engine template.
	// Adjust its spec for a newly created engine.
	var currentEngineForReplicaCreationFailure *longhorn.Engine // Changed name
	for _, e := range tc.expectEngines { // Should be only one from the template
		currentEngineForReplicaCreationFailure = e
		break
	}
	if currentEngineForReplicaCreationFailure != nil {
		currentEngineForReplicaCreationFailure.Spec.Image = tc.expectVolume.Status.CurrentImage // Match volume's current image.
		currentEngineForReplicaCreationFailure.Spec.DesireState = longhorn.InstanceStateStopped
		currentEngineForReplicaCreationFailure.Spec.Active = true
	} else {
		panic("Expected tc.expectEngines to have a template engine after copyCurrentToExpect for 'replica creation failure'")
	}

	// Controller will ATTEMPT to create replica CRDs but will fail precheck.
	// Therefore, no replica CRDs should exist in the datastore after the sync.
	tc.expectReplicas = map[string]*longhorn.Replica{} // Expect an empty map of replicas.

	testCases["volume create - replica creation failure"] = tc

	// unable to create volume because no node to schedule, but replica CRDs already exist (unscheduled).
	// Controller attempts to schedule existing replicas but fails.
	tc = generateVolumeTestCaseTemplate()
	for i := range tc.nodes {
		tc.nodes[i].Spec.AllowScheduling = false // Nodes are made unschedulable.
	}
	tc.copyCurrentToExpect() // Populates tc.expectVolume, tc.expectEngines, tc.expectReplicas.

	// Scenario: Controller finds existing (but unscheduled) replica CRDs.
	// Engine object will be created by the controller.
	tc.engines = nil

	// Adjust expected replicas: NodeID, DiskID, etc., will remain empty as scheduling fails.
	// These are also the input replicas for the controller.
	inputReplicaNamesForSchedulingFailure := []string{}
	for rName, r := range tc.expectReplicas {
		r.Spec.NodeID = ""
		r.Spec.DiskID = ""
		r.Spec.DiskPath = ""
		r.Spec.DataDirectoryName = ""
		inputReplicaNamesForSchedulingFailure = append(inputReplicaNamesForSchedulingFailure, rName)
	}
	tc.replicas = tc.expectReplicas // These are the input replicas the controller will find.

	// Adjust expectations for the volume:
	tc.expectVolume.Status.State = longhorn.VolumeStateCreating
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessFaulted // Controller marks it faulted.

	// Construct the concise expected message for the Scheduled condition.
	var replicaMessagePartsSchedFailure []string // Use var for clarity
	var expectedMsgSchedFailure string

	for _, rName := range inputReplicaNamesForSchedulingFailure {
		// With the new controller logic, the message will be concise.
		msgPart := fmt.Sprintf("replica %s exists but is not yet scheduled to a node", rName)
		replicaMessagePartsSchedFailure = append(replicaMessagePartsSchedFailure, msgPart)
	}
	// The controller sorts these messages.
	sort.Strings(replicaMessagePartsSchedFailure)
	expectedMsgSchedFailure = "Replica scheduling issues: " + strings.Join(replicaMessagePartsSchedFailure, "; ")
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.expectVolume.Status.Conditions,
		longhorn.VolumeConditionTypeScheduled, longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonReplicaSchedulingFailure, expectedMsgSchedFailure)

	// Adjust expected engine (similar to the "creation failure" case).
	var currentEngineForReplicaSchedulingFailure *longhorn.Engine // Changed name
	for _, e := range tc.expectEngines { // Should be only one from the template
		currentEngineForReplicaSchedulingFailure = e
		break
	}
	if currentEngineForReplicaSchedulingFailure != nil {
		currentEngineForReplicaSchedulingFailure.Spec.Image = tc.expectVolume.Status.CurrentImage
		currentEngineForReplicaSchedulingFailure.Spec.DesireState = longhorn.InstanceStateStopped
		currentEngineForReplicaSchedulingFailure.Spec.Active = true
	} else {
		panic("Expected tc.expectEngines to have a template engine after copyCurrentToExpect for 'replica scheduling failure'")
	}
	// tc.expectReplicas already reflects that replicas remain unscheduled.

	testCases["volume create - replica scheduling failure"] = tc

	// detaching after creation
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Status.State = longhorn.VolumeStateCreating
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusFalse, "", "")
	tc.expectVolume.Status.State = longhorn.VolumeStateDetaching
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessUnknown
	testCases["volume detaching after being created"] = tc

	// after creation, volume in detached state
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Status.State = longhorn.VolumeStateDetaching
	for _, e := range tc.engines {
		e.Status.CurrentState = longhorn.InstanceStateStopped
	}
	for _, r := range tc.replicas {
		r.Status.CurrentState = longhorn.InstanceStateStopped
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusFalse, "", "")
	tc.expectVolume.Status.State = longhorn.VolumeStateDetached
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessUnknown
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	testCases["volume detached"] = tc

	// volume attaching, start replicas
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	for _, r := range tc.replicas {
		r.Status.CurrentState = longhorn.InstanceStateStopped
	}
	tc.volume.Status.State = longhorn.VolumeStateDetached
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = longhorn.VolumeStateAttaching
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	// replicas will be started first
	// engine will be started only after all the replicas are running
	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = longhorn.InstanceStateRunning
	}
	testCases["volume attaching - start replicas"] = tc

	// volume attaching, start engine
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	for _, r := range tc.replicas {
		r.Spec.DesireState = longhorn.InstanceStateRunning
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
	}
	tc.volume.Status.State = longhorn.VolumeStateAttaching
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = longhorn.VolumeStateAttaching
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	for _, e := range tc.expectEngines {
		e.Spec.NodeID = tc.volume.Spec.NodeID
		e.Spec.DesireState = longhorn.InstanceStateRunning
	}
	for name, r := range tc.expectReplicas {
		// TODO update to r.Spec.AssociatedEngine
		for _, e := range tc.expectEngines {
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
		}
	}
	testCases["volume attaching - start controller"] = tc

	// volume attached
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateAttaching

	for _, e := range tc.engines {
		e.Spec.NodeID = tc.volume.Spec.NodeID
		e.Spec.DesireState = longhorn.InstanceStateRunning
		e.Status.CurrentState = longhorn.InstanceStateRunning
		e.Status.IP = randomIP()
		e.Status.StorageIP = e.Status.IP
		e.Status.Port = randomPort()
		e.Status.Endpoint = "/dev/" + tc.volume.Name
		e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}
	}
	for name, r := range tc.replicas {
		r.Spec.DesireState = longhorn.InstanceStateRunning
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		// TODO update to r.Spec.AssociatedEngine
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
			e.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
		}
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = longhorn.VolumeStateAttached
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	tc.expectVolume.Status.CurrentNodeID = tc.volume.Spec.NodeID
	for _, r := range tc.expectReplicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
	}
	testCases["volume attached"] = tc

	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Spec.FromBackup = testBackupURL
	tc.volume.Spec.Standby = false
	tc.volume.Spec.DisableFrontend = true
	tc.volume.Status.State = longhorn.VolumeStateAttaching
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.volume.Status.RestoreRequired = true
	tc.volume.Status.RestoreInitiated = true
	tc.volume.Status.LastBackup = TestBackupName
	tc.volume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusFalse, "", "")
	for _, e := range tc.engines {
		e.Spec.BackupVolume = TestBackupVolumeName
		e.Spec.RequestedBackupRestore = TestBackupName
		e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}
	}
	for _, r := range tc.replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Spec.DesireState = longhorn.InstanceStateRunning
		r.Spec.Image = TestEngineImage
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.CurrentImage = TestEngineImage
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.FrontendDisabled = true
	for _, e := range tc.expectEngines {
		e.Spec.NodeID = TestNode1
		e.Spec.DesireState = longhorn.InstanceStateRunning
		e.Spec.BackupVolume = TestBackupVolumeName
		e.Spec.RequestedBackupRestore = TestBackupName
		e.Spec.DisableFrontend = true
	}
	for name, r := range tc.expectReplicas {
		for _, e := range tc.expectEngines {
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
		}
	}
	// Set replica node soft anti-affinity
	tc.replicaNodeSoftAntiAffinity = "true"
	testCases["restored volume is automatically attaching after creation"] = tc

	// Newly restored volume changed from attaching to attached
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Spec.FromBackup = testBackupURL
	tc.volume.Spec.Standby = false
	tc.volume.Spec.DisableFrontend = true
	tc.volume.Status.State = longhorn.VolumeStateAttaching
	tc.volume.Status.LastBackup = TestBackupName
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.volume.Status.RestoreRequired = true
	tc.volume.Status.RestoreInitiated = true
	tc.volume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusFalse, "", "")
	for _, e := range tc.engines {
		e.Spec.NodeID = TestNode1
		e.Spec.DesireState = longhorn.InstanceStateRunning
		e.Spec.BackupVolume = TestBackupVolumeName
		e.Spec.RequestedBackupRestore = TestBackupName
		e.Spec.DisableFrontend = true
		e.Status.OwnerID = TestNode1
		e.Status.CurrentState = longhorn.InstanceStateRunning
		e.Status.IP = randomIP()
		e.Status.StorageIP = e.Status.IP
		e.Status.Port = randomPort()
		e.Status.Endpoint = "/dev/" + tc.volume.Name
		e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}
	}
	for name, r := range tc.replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Spec.DesireState = longhorn.InstanceStateRunning
		r.Spec.Image = TestEngineImage
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.CurrentImage = TestEngineImage
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
			e.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
		}
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.FrontendDisabled = true
	tc.expectVolume.Status.State = longhorn.VolumeStateAttached
	tc.expectVolume.Status.CurrentNodeID = TestNode1
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusTrue, longhorn.VolumeConditionReasonRestoreInProgress, "")
	testCases["newly restored volume attaching to attached"] = tc

	// Newly restored volume is waiting for restoration completed
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Spec.FromBackup = testBackupURL
	tc.volume.Spec.Standby = false
	tc.volume.Spec.DisableFrontend = true
	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.OwnerID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateAttached
	tc.volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.volume.Status.FrontendDisabled = true
	tc.volume.Status.RestoreRequired = true
	tc.volume.Status.RestoreInitiated = true
	tc.volume.Status.LastBackup = TestBackupName
	tc.volume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusTrue, longhorn.VolumeConditionReasonRestoreInProgress, "")
	for _, e := range tc.engines {
		e.Spec.NodeID = TestNode1
		e.Spec.DesireState = longhorn.InstanceStateRunning
		e.Spec.BackupVolume = TestBackupVolumeName
		e.Spec.RequestedBackupRestore = TestBackupName
		e.Spec.DisableFrontend = true
		e.Status.OwnerID = TestNode1
		e.Status.CurrentState = longhorn.InstanceStateRunning
		e.Status.IP = randomIP()
		e.Status.StorageIP = e.Status.IP
		e.Status.Port = randomPort()
		e.Status.Endpoint = "/dev/" + tc.volume.Name
		e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}
		e.Status.LastRestoredBackup = ""
	}
	for name, r := range tc.replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		r.Spec.DesireState = longhorn.InstanceStateRunning
		r.Status.CurrentState = longhorn.InstanceStateRunning
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
			e.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
		}
	}
	tc.copyCurrentToExpect()
	testCases["newly restored volume is waiting for restoration completed"] = tc

	// try to detach newly restored volume after restoration completed
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = ""
	tc.volume.Spec.FromBackup = testBackupURL
	tc.volume.Spec.Standby = false
	tc.volume.Spec.DisableFrontend = true
	tc.volume.Status.OwnerID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateAttached
	tc.volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.volume.Status.FrontendDisabled = true
	tc.volume.Status.RestoreInitiated = true
	tc.volume.Status.LastBackup = TestBackupName
	tc.volume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusTrue, longhorn.VolumeConditionReasonRestoreInProgress, "")
	for _, e := range tc.engines {
		e.Spec.NodeID = TestNode1
		e.Spec.DesireState = longhorn.InstanceStateRunning
		e.Spec.BackupVolume = TestBackupVolumeName
		e.Spec.RequestedBackupRestore = TestBackupName
		e.Spec.DisableFrontend = tc.volume.Status.FrontendDisabled
		e.Status.OwnerID = TestNode1
		e.Status.CurrentState = longhorn.InstanceStateRunning
		e.Status.IP = randomIP()
		e.Status.StorageIP = e.Status.IP
		e.Status.Port = randomPort()
		e.Status.Endpoint = "/dev/" + tc.volume.Name
		e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}
		e.Status.LastRestoredBackup = TestBackupName
	}
	for name, r := range tc.replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		r.Spec.DesireState = longhorn.InstanceStateRunning
		r.Status.CurrentState = longhorn.InstanceStateRunning
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
			e.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
		}
	}
	tc.copyCurrentToExpect()
	for _, e := range tc.expectEngines {
		e.Spec.NodeID = ""
		e.Spec.DesireState = longhorn.InstanceStateStopped
		e.Spec.BackupVolume = ""
		e.Spec.RequestedBackupRestore = ""
	}
	tc.expectVolume.Spec.NodeID = ""
	tc.expectVolume.Spec.DisableFrontend = true
	tc.expectVolume.Status.CurrentNodeID = TestNode1
	tc.expectVolume.Status.State = longhorn.VolumeStateDetaching
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessUnknown
	tc.expectVolume.Status.FrontendDisabled = true
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.expectVolume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusFalse, "", "")
	for _, r := range tc.expectReplicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
	}
	testCases["try to detach newly restored volume after restoration completed"] = tc

	// newly restored volume is being detaching after restoration completed
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = ""
	tc.volume.Spec.FromBackup = testBackupURL
	tc.volume.Spec.Standby = false
	tc.volume.Spec.DisableFrontend = true
	tc.volume.Status.OwnerID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateDetaching
	tc.volume.Status.Robustness = longhorn.VolumeRobustnessUnknown
	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.volume.Status.FrontendDisabled = true
	tc.volume.Status.RestoreInitiated = true
	tc.volume.Status.LastBackup = TestBackupName
	tc.volume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusFalse, "", "")
	for _, e := range tc.engines {
		e.Spec.NodeID = ""
		e.Spec.DesireState = longhorn.InstanceStateStopped
		e.Spec.BackupVolume = ""
		e.Spec.RequestedBackupRestore = ""
		e.Status.OwnerID = TestNode1
		e.Status.CurrentState = longhorn.InstanceStateStopped
		e.Status.IP = ""
		e.Status.StorageIP = ""
		e.Status.Port = 0
		e.Status.Endpoint = ""
		e.Status.CurrentImage = ""
		e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}
		e.Status.LastRestoredBackup = ""
	}
	for name, r := range tc.replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		r.Spec.DesireState = longhorn.InstanceStateStopped
		r.Status.CurrentState = longhorn.InstanceStateRunning
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
		}
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Spec.NodeID = ""
	tc.expectVolume.Spec.DisableFrontend = true
	tc.expectVolume.Status.CurrentNodeID = TestNode1
	tc.expectVolume.Status.State = longhorn.VolumeStateDetaching
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessUnknown
	for _, r := range tc.expectReplicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
	}
	testCases["newly restored volume is being detaching after restoration completed"] = tc

	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Spec.FromBackup = testBackupURL
	tc.volume.Spec.Standby = false
	tc.volume.Spec.DisableFrontend = true
	tc.volume.Spec.Image = TestEngineImage
	tc.volume.Status.OwnerID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateAttached
	tc.volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.volume.Status.FrontendDisabled = true
	tc.volume.Status.RestoreRequired = true
	tc.volume.Status.RestoreInitiated = true
	tc.volume.Status.LastBackup = TestBackupName
	tc.volume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusTrue, longhorn.VolumeConditionReasonRestoreInProgress, "")
	for _, e := range tc.engines {
		e.Spec.NodeID = TestNode1
		e.Spec.DesireState = longhorn.InstanceStateRunning
		e.Spec.BackupVolume = TestBackupVolumeName
		e.Spec.RequestedBackupRestore = TestBackupName
		e.Spec.Image = TestEngineImage
		e.Status.OwnerID = TestNode1
		e.Status.CurrentState = longhorn.InstanceStateRunning
		e.Status.IP = randomIP()
		e.Status.StorageIP = e.Status.IP
		e.Status.Port = randomPort()
		e.Status.Endpoint = "/dev/" + tc.volume.Name
		e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}
		e.Status.LastRestoredBackup = TestBackupName
		e.Status.CurrentImage = TestEngineImage
	}
	// Pick up one replica as the failed replica
	failedReplicaName := ""
	for name := range tc.replicas {
		failedReplicaName = name
		break
	}
	for name, r := range tc.replicas {
		r.Spec.NodeID = TestNode1
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Spec.DesireState = longhorn.InstanceStateRunning
		if name != failedReplicaName {
			r.Status.CurrentState = longhorn.InstanceStateRunning
			r.Status.IP = randomIP()
			r.Status.StorageIP = r.Status.IP
			r.Status.Port = randomPort()
		} else {
			r.Status.CurrentState = longhorn.InstanceStateError
		}
		for _, e := range tc.engines {
			e.Spec.DisableFrontend = true
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
			if name != failedReplicaName {
				e.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
			} else {
				e.Status.ReplicaModeMap[name] = longhorn.ReplicaModeERR
			}
		}
	}
	tc.allowVolumeCreationWithDegradedAvailability = "false"
	tc.copyCurrentToExpect()
	newReplicaMap := map[string]*longhorn.Replica{}
	for name, r := range tc.replicas {
		if name != failedReplicaName {
			newReplicaMap[name] = r
		}
	}
	tc.expectReplicas = newReplicaMap
	for _, e := range tc.expectEngines {
		delete(e.Spec.ReplicaAddressMap, failedReplicaName)
		e.Spec.LogRequested = true
	}
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessDegraded
	tc.expectVolume.Status.LastDegradedAt = getTestNow()
	// When a replica fails and needs replenishment, the Scheduled condition becomes False.
	expectedSchedMsg := "Replica scheduling issues: replenishment needed for 1 replica(s)"
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.expectVolume.Status.Conditions,
		longhorn.VolumeConditionTypeScheduled, longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonReplicaSchedulingFailure, expectedSchedMsg)
	testCases["the restored volume keeps and wait for the rebuild after the restoration completed"] = tc

	// try to update the volume as Faulted if all replicas failed to restore data
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = ""
	tc.volume.Spec.FromBackup = testBackupURL
	tc.volume.Spec.Standby = true
	tc.volume.Spec.DisableFrontend = true
	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.FrontendDisabled = true
	tc.volume.Status.OwnerID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateAttached
	tc.volume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.volume.Status.RestoreInitiated = true
	tc.volume.Status.RestoreRequired = true
	tc.volume.Status.IsStandby = true
	tc.volume.Status.LastBackup = TestBackupName
	tc.volume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusTrue, longhorn.VolumeConditionReasonRestoreInProgress, "")
	for _, e := range tc.engines {
		e.Spec.NodeID = TestNode1
		e.Spec.DesireState = longhorn.InstanceStateRunning
		e.Spec.BackupVolume = TestBackupVolumeName
		e.Spec.RequestedBackupRestore = TestBackupName
		e.Spec.DisableFrontend = tc.volume.Status.FrontendDisabled
		e.Status.OwnerID = TestNode1
		e.Status.CurrentState = longhorn.InstanceStateRunning
		e.Status.IP = randomIP()
		e.Status.StorageIP = e.Status.IP
		e.Status.Port = randomPort()
		e.Status.Endpoint = "/dev/" + tc.volume.Name
		e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}
		e.Status.LastRestoredBackup = TestBackupName
	}
	for name, r := range tc.replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		r.Spec.DesireState = longhorn.InstanceStateRunning
		r.Status.CurrentState = longhorn.InstanceStateRunning
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
			e.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
			if e.Status.RestoreStatus == nil {
				e.Status.RestoreStatus = map[string]*longhorn.RestoreStatus{}
			}
			e.Status.RestoreStatus[name] = &longhorn.RestoreStatus{
				Error: "Test restore error",
			}
		}
	}

	tc.copyCurrentToExpect()

	// --- Expected state after controller sync ---
	tc.expectVolume.Spec.NodeID = ""
	tc.expectVolume.Spec.DisableFrontend = true
	tc.expectVolume.Status.CurrentNodeID = TestNode1
	tc.expectVolume.Status.State = longhorn.VolumeStateDetaching
	tc.expectVolume.Status.FrontendDisabled = true
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessFaulted

	// Update Restore condition
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.expectVolume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonRestoreFailure, "All replica restore failed and the volume became Faulted")

	expectedScheduledMessage := "Replica scheduling issues: replenishment needed for 2 replica(s)"
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.expectVolume.Status.Conditions,
		longhorn.VolumeConditionTypeScheduled, longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonReplicaSchedulingFailure,
		expectedScheduledMessage)

	for _, e := range tc.expectEngines {
		e.Spec.NodeID = ""
		e.Spec.DesireState = longhorn.InstanceStateStopped
		e.Spec.LogRequested = true
		e.Spec.RequestedBackupRestore = ""
	}
	for _, r := range tc.expectReplicas {
		r.Spec.FailedAt = getTestNow()
		r.Spec.LastFailedAt = r.Spec.FailedAt
		r.Spec.DesireState = longhorn.InstanceStateStopped
		r.Spec.LogRequested = true
	}
	testCases["newly restored volume becomes faulted after all replica error"] = tc

	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Spec.FromBackup = testBackupURL
	tc.volume.Spec.Standby = true
	tc.volume.Spec.DisableFrontend = true
	tc.volume.Status.CurrentNodeID = ""
	tc.volume.Status.State = longhorn.VolumeStateAttaching
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.volume.Status.FrontendDisabled = true
	tc.volume.Status.RestoreRequired = true
	tc.volume.Status.RestoreInitiated = true
	tc.volume.Status.IsStandby = true
	tc.volume.Status.LastBackup = TestBackupName
	tc.volume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusTrue, longhorn.VolumeConditionReasonRestoreInProgress, "")
	for _, e := range tc.engines {
		e.Spec.RequestedBackupRestore = TestBackupName
		e.Spec.BackupVolume = TestBackupVolumeName
	}
	for _, r := range tc.replicas {
		r.Spec.HealthyAt = ""
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.CurrentState = longhorn.InstanceStateStopped
	}
	tc.copyCurrentToExpect()
	for _, e := range tc.expectEngines {
		e.Spec.RequestedBackupRestore = TestBackupName
	}
	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = longhorn.InstanceStateRunning
	}
	testCases["new standby volume is automatically attaching"] = tc

	// New standby volume changed from attaching to attached, and it's not automatically detached
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Spec.FromBackup = testBackupURL
	tc.volume.Spec.Standby = true
	tc.volume.Spec.DisableFrontend = true
	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateAttached
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.volume.Status.LastBackup = TestBackupName
	tc.volume.Status.FrontendDisabled = true
	tc.volume.Status.RestoreRequired = true
	tc.volume.Status.RestoreInitiated = true
	tc.volume.Status.IsStandby = true
	tc.volume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusTrue, longhorn.VolumeConditionReasonRestoreInProgress, "")
	for _, e := range tc.engines {
		e.Spec.NodeID = tc.volume.Status.CurrentNodeID
		e.Spec.DesireState = longhorn.InstanceStateRunning
		e.Spec.BackupVolume = TestBackupVolumeName
		e.Spec.RequestedBackupRestore = TestBackupName
		e.Spec.DisableFrontend = tc.volume.Status.FrontendDisabled
		e.Status.CurrentState = longhorn.InstanceStateRunning
		e.Status.IP = randomIP()
		e.Status.StorageIP = e.Status.IP
		e.Status.Port = randomPort()
		e.Status.Endpoint = "/dev/" + tc.volume.Name
		e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}
		e.Status.LastRestoredBackup = TestBackupName
	}
	for name, r := range tc.replicas {
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Spec.DesireState = longhorn.InstanceStateRunning
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
			e.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
		}
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = longhorn.VolumeStateAttached
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessHealthy
	testCases["standby volume is not automatically detached"] = tc

	// volume detaching - stop engine
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = ""
	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateAttached
	for _, e := range tc.engines {
		e.Spec.NodeID = TestNode1
		e.Spec.DesireState = longhorn.InstanceStateRunning
		e.Status.CurrentState = longhorn.InstanceStateRunning
		e.Status.IP = randomIP()
		e.Status.StorageIP = e.Status.IP
		e.Status.Port = randomPort()
		e.Status.Endpoint = "/dev/" + tc.volume.Name
		e.Status.ReplicaModeMap = make(map[string]longhorn.ReplicaMode)
	}
	for name, r := range tc.replicas {
		r.Spec.DesireState = longhorn.InstanceStateRunning
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		// TODO update to r.Spec.AssociatedEngine
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
			e.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
		}
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = longhorn.VolumeStateDetaching
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessUnknown
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusFalse, "", "")
	for _, e := range tc.expectEngines {
		e.Spec.NodeID = ""
		e.Spec.DesireState = longhorn.InstanceStateStopped
	}
	testCases["volume detaching - stop engine"] = tc

	// volume detaching - stop replicas
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = ""
	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateAttached
	for _, e := range tc.engines {
		e.Spec.NodeID = ""
		e.Status.CurrentState = longhorn.InstanceStateStopped
	}
	for name, r := range tc.replicas {
		r.Spec.DesireState = longhorn.InstanceStateRunning
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		// TODO update to r.Spec.AssociatedEngine
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
		}
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = longhorn.VolumeStateDetaching
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessUnknown
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusFalse, "", "")
	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = longhorn.InstanceStateStopped
	}
	testCases["volume detaching - stop replicas"] = tc

	// volume deleting
	tc = generateVolumeTestCaseTemplate()
	now := metav1.NewTime(time.Now())
	tc.volume.SetDeletionTimestamp(&now)
	tc.volume.Status.Conditions = []longhorn.Condition{}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = longhorn.VolumeStateDeleting
	tc.expectEngines = nil
	tc.expectReplicas = nil
	testCases["volume deleting"] = tc

	// volume attaching, start replicas, one node down
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateDetached
	tc.volume.Spec.StaleReplicaTimeout = 1<<24 - 1
	tc.nodes[1] = newNode(TestNode2, TestNamespace, false, longhorn.ConditionStatusFalse, string(longhorn.NodeConditionReasonKubernetesNodeGone))
	for _, r := range tc.replicas {
		// Assume the volume is previously attached then detached.
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.CurrentState = longhorn.InstanceStateStopped
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.CurrentNodeID = ""
	tc.expectVolume.Status.State = longhorn.VolumeStateAttaching
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	for _, r := range tc.expectReplicas {
		if r.Spec.NodeID == TestNode2 {
			r.Spec.DesireState = longhorn.InstanceStateStopped
			r.Spec.FailedAt = getTestNow()
			r.Spec.LastFailedAt = r.Spec.FailedAt
		} else {
			r.Spec.DesireState = longhorn.InstanceStateRunning
		}
	}
	testCases["volume attaching - start replicas - node failed"] = tc

	// Disable revision counter
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Status.OwnerID = TestNode1
	tc.volume.Status.CurrentNodeID = ""

	// Since default node 2 is scheduling disabled,
	// enable node soft anti-affinity for 2 replicas.
	tc.replicaNodeSoftAntiAffinity = "true"
	tc.volume.Spec.RevisionCounterDisabled = true
	tc.copyCurrentToExpect()
	tc.engines = nil
	tc.replicas = nil

	tc.expectVolume.Status.State = longhorn.VolumeStateCreating
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	tc.expectVolume.Status.CurrentNodeID = ""
	// tc.expectVolume.Status.Conditions should already have Scheduled:True from the template

	// Setup expectations for created engine and replicas
	expectedCreatedEngine := newEngineForVolume(tc.volume)
	expectedCreatedEngine.Spec.RevisionCounterDisabled = true
	expectedCreatedEngine.Spec.Image = tc.expectVolume.Status.CurrentImage
	expectedCreatedEngine.Spec.DesireState = longhorn.InstanceStateStopped
	expectedCreatedEngine.Spec.Active = true
	tc.expectEngines = map[string]*longhorn.Engine{"templateKeyForExpectedEngine": expectedCreatedEngine}

	tc.expectReplicas = map[string]*longhorn.Replica{}
	for i := 0; i < tc.volume.Spec.NumberOfReplicas; i++ {
		replicaNamePlaceholder := fmt.Sprintf("templateKeyForExpectedReplica%d", i+1)
		r := newReplicaForVolume(tc.volume, expectedCreatedEngine, "", "")
		r.Spec.RevisionCounterDisabled = true
		r.Spec.Image = tc.expectVolume.Status.CurrentImage
		r.Spec.DesireState = longhorn.InstanceStateStopped
		tc.expectReplicas[replicaNamePlaceholder] = r
	}

	var node2ToModify *longhorn.Node
	for _, n := range tc.nodes {
		if n.Name == TestNode2 {
			node2ToModify = n
			break
		}
	}
	if node2ToModify != nil {
		node2ToModify.Spec.AllowScheduling = true
	} else {
		panic(fmt.Sprintf("Test setup error: Node %s not found in tc.nodes for test case 'volume revision counter disabled'", TestNode2))
	}

	testCases["volume revision counter disabled - engine and replica revision counter disabled"] = tc

	// Salvage Requested
	tc = generateVolumeTestCaseTemplate()
	// volume is Faulted and the VA controller already unset spec.NodeID for this volume
	tc.volume.Spec.NodeID = ""
	tc.volume.Spec.RevisionCounterDisabled = true
	tc.volume.Status.State = longhorn.VolumeStateDetached
	tc.volume.Status.Robustness = longhorn.VolumeRobustnessFaulted
	tc.volume.Status.CurrentNodeID = ""
	tc.volumeAutoSalvage = "true"

	for _, e := range tc.engines {
		e.Status.CurrentState = longhorn.InstanceStateStopped
		e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}
		e.Status.SalvageExecuted = false
		e.Spec.DesireState = longhorn.InstanceStateStopped
	}
	for _, r := range tc.replicas {
		r.Status.CurrentState = longhorn.InstanceStateStopped
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Spec.FailedAt = getTestNow()
		r.Spec.LastFailedAt = r.Spec.FailedAt
		r.Spec.DesireState = longhorn.InstanceStateStopped
	}

	tc.copyCurrentToExpect()

	for _, e := range tc.expectEngines {
		e.Spec.SalvageRequested = true
	}

	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = longhorn.InstanceStateStopped
		r.Spec.FailedAt = ""
	}

	tc.expectVolume.Status.State = longhorn.VolumeStateDetached
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	tc.expectVolume.Status.CurrentNodeID = ""
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessUnknown
	tc.expectVolume.Status.RemountRequestedAt = getTestNow()

	// Update the expected Scheduled condition
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(
		tc.expectVolume.Status.Conditions,
		string(longhorn.VolumeConditionTypeScheduled), // Use longhorn.VolumeConditionTypeScheduled
		longhorn.ConditionStatusFalse,
		longhorn.VolumeConditionReasonReplicaSchedulingFailure,
		"Replica scheduling issues: replenishment needed for 2 replica(s)",
	)

	testCases["volume salvage requested - all replica failed"] = tc

	// volume attaching, start replicas, manager restart
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Status.CurrentNodeID = ""
	tc.volume.Status.State = longhorn.VolumeStateDetached
	tc.nodes[1] = newNode(TestNode2, TestNamespace, false, longhorn.ConditionStatusFalse, string(longhorn.NodeConditionReasonManagerPodDown))
	for _, r := range tc.replicas {
		r.Status.CurrentState = longhorn.InstanceStateStopped
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = longhorn.VolumeStateAttaching
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	expectRs := map[string]*longhorn.Replica{}
	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = longhorn.InstanceStateRunning
		expectRs[r.Name] = r
	}
	tc.expectReplicas = expectRs
	testCases["volume attaching - start replicas - manager down"] = tc

	// restoring volume reattaching, stop replicas
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = ""
	tc.volume.Status.CurrentNodeID = ""
	tc.volume.Status.State = longhorn.VolumeStateDetaching
	for _, e := range tc.engines {
		e.Spec.NodeID = ""
		e.Status.CurrentState = longhorn.InstanceStateStopped
	}
	for name, r := range tc.replicas {
		r.Spec.DesireState = longhorn.InstanceStateRunning
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		r.Status.CurrentState = longhorn.InstanceStateRunning
		r.Status.IP = randomIP()
		r.Status.StorageIP = r.Status.IP
		r.Status.Port = randomPort()
		for _, e := range tc.engines {
			e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
		}
	}
	tc.copyCurrentToExpect()
	tc.expectVolume.Status.State = longhorn.VolumeStateDetaching
	tc.expectVolume.Status.Robustness = longhorn.VolumeRobustnessUnknown
	tc.expectVolume.Status.CurrentImage = tc.volume.Spec.Image
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.volume.Status.Conditions,
		longhorn.VolumeConditionTypeRestore, longhorn.ConditionStatusFalse, "", "")
	for _, r := range tc.expectReplicas {
		r.Spec.DesireState = longhorn.InstanceStateStopped
	}
	testCases["restoring volume reattaching - stop replicas"] = tc

	// replica rebuilding - reuse failed replica
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateAttached
	tc.volume.Status.Robustness = longhorn.VolumeRobustnessDegraded
	tc.volume.Status.LastDegradedAt = "2014-01-02T00:00:00Z"
	for _, e := range tc.engines {
		e.Spec.NodeID = TestNode1
		e.Spec.DesireState = longhorn.InstanceStateRunning
		e.Spec.Image = TestEngineImage
		e.Status.CurrentState = longhorn.InstanceStateRunning
		e.Status.CurrentImage = TestEngineImage
		e.Status.CurrentSize = TestVolumeSize
		e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}
	}
	var failedReplica *longhorn.Replica
	for name, r := range tc.replicas {
		if r.Spec.NodeID == TestNode1 {
			r.Spec.DesireState = longhorn.InstanceStateStopped
			r.Status.CurrentState = longhorn.InstanceStateStopped
			r.Spec.FailedAt = getTestNow()
			r.Spec.LastFailedAt = r.Spec.FailedAt
			failedReplica = r
		} else {
			r.Spec.DesireState = longhorn.InstanceStateRunning
			r.Status.CurrentState = longhorn.InstanceStateRunning
			r.Status.IP = randomIP()
			r.Status.StorageIP = r.Status.IP
			r.Status.Port = randomPort()
		}
		r.Spec.HealthyAt = getTestNow()
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		for _, e := range tc.engines {
			if r.Spec.FailedAt == "" {
				e.Status.ReplicaModeMap[name] = "RW"
				e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
			}
		}
	}
	tc.copyCurrentToExpect()
	for _, r := range tc.expectReplicas {
		if r.Name == failedReplica.Name {
			r.Spec.DesireState = longhorn.InstanceStateRunning
			r.Spec.FailedAt = ""
			// r.Spec.LastFailedAt will NOT be "".
			r.Spec.HealthyAt = ""
			// r.Spec.LastHealthyAt will NOT be "".
			r.Spec.RebuildRetryCount = 1
			break
		}
	}
	testCases["replica rebuilding - reuse failed replica"] = tc

	// replica rebuilding - delay replica replenishment
	tc = generateVolumeTestCaseTemplate()
	tc.volume.Spec.NodeID = TestNode1
	tc.volume.Status.CurrentImage = TestEngineImage
	tc.volume.Status.CurrentNodeID = TestNode1
	tc.volume.Status.State = longhorn.VolumeStateAttached
	tc.volume.Status.Robustness = longhorn.VolumeRobustnessDegraded
	// Using a fixed past time for LastDegradedAt might be more stable than getTestNow() if exact timing matters
	// For this issue, getTestNow() is probably fine.
	tc.volume.Status.LastDegradedAt = "2015-01-02T00:00:00Z" // Or getTestNow() if precise timing for this field isn't critical to the test logic

	for _, e := range tc.engines {
		e.Spec.NodeID = TestNode1
		e.Spec.DesireState = longhorn.InstanceStateRunning
		e.Spec.Image = TestEngineImage
		e.Status.CurrentState = longhorn.InstanceStateRunning
		e.Status.CurrentImage = TestEngineImage
		e.Status.CurrentSize = TestVolumeSize
		e.Status.ReplicaModeMap = map[string]longhorn.ReplicaMode{}
		e.Status.ReplicaTransitionTimeMap = make(map[string]string) // Initialize this map
	}
	// var failedReplicaInTest *longhorn.Replica // Use a distinct name
	for name, r := range tc.replicas {
		if r.Spec.NodeID == TestNode2 { // Replica on Node2 is the one that "failed"
			r.Spec.DesireState = longhorn.InstanceStateStopped
			r.Status.CurrentState = longhorn.InstanceStateStopped
			// Use a fixed past time for FailedAt for more predictable test behavior with replenishment intervals
			r.Spec.FailedAt = "2015-01-01T00:00:00Z"
			r.Spec.LastFailedAt = r.Spec.FailedAt
			// failedReplicaInTest = r // No longer assigning to an unused variable
		} else { // Replica on Node1 is healthy
			r.Spec.DesireState = longhorn.InstanceStateRunning
			r.Status.CurrentState = longhorn.InstanceStateRunning
			r.Status.IP = randomIP()
			r.Status.StorageIP = r.Status.IP
			r.Status.Port = randomPort()
		}
		// All replicas were healthy at some point
		r.Spec.HealthyAt = "2014-12-31T00:00:00Z" // Fixed past time
		r.Spec.LastHealthyAt = r.Spec.HealthyAt
		for _, e := range tc.engines {
			if r.Spec.FailedAt == "" { // Only the healthy one (on Node1)
				e.Status.ReplicaModeMap[name] = longhorn.ReplicaModeRW
				e.Spec.ReplicaAddressMap[name] = imutil.GetURL(r.Status.StorageIP, r.Status.Port)
				// Set transition time to avoid the "BUG" log. Use a time consistent with HealthyAt.
				e.Status.ReplicaTransitionTimeMap[name] = r.Spec.HealthyAt
			}
		}
	}
	// Corrected: Use '=' for struct field assignment. Your change to ':=' caused an error.
	tc.replicaReplenishmentWaitInterval = strconv.Itoa(math.MaxInt32) // Huge delay
	tc.copyCurrentToExpect()

	// Corrected: Use '=' because 'expectedScheduledMessage' is likely pre-declared in this scope,
	// as indicated by the "no new variables on left side of :=" error.
	expectedScheduledMessage = "Replica scheduling issues: replenishment needed for 1 replica(s)"
	tc.expectVolume.Status.Conditions = setVolumeConditionWithoutTimestamp(tc.expectVolume.Status.Conditions,
		longhorn.VolumeConditionTypeScheduled, longhorn.ConditionStatusFalse, longhorn.VolumeConditionReasonReplicaSchedulingFailure,
		expectedScheduledMessage)

	testCases["replica rebuilding - delay replica replenishment"] = tc

	s.runTestCases(c, testCases)
}

func newVolume(name string, replicaCount int) *longhorn.Volume {
	return &longhorn.Volume{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Finalizers: []string{
				longhorn.SchemeGroupVersion.Group,
			},
		},
		Spec: longhorn.VolumeSpec{
			Frontend:            longhorn.VolumeFrontendBlockDev,
			NumberOfReplicas:    replicaCount,
			Size:                TestVolumeSize,
			StaleReplicaTimeout: TestVolumeStaleTimeout,
			Image:               TestEngineImage,
			DataEngine:          longhorn.DataEngineTypeV1,
			BackupTargetName:    TestBackupTargetName,
		},
		Status: longhorn.VolumeStatus{
			OwnerID: TestOwnerID1,
			Conditions: []longhorn.Condition{
				{
					Type:   string(longhorn.VolumeConditionTypeWaitForBackingImage),
					Status: longhorn.ConditionStatusFalse,
				},
				{
					Type:   string(longhorn.VolumeConditionTypeTooManySnapshots),
					Status: longhorn.ConditionStatusFalse,
				},
				{
					Type:   string(longhorn.VolumeConditionTypeScheduled),
					Status: longhorn.ConditionStatusTrue,
				},
			},
		},
	}
}

func newEngineForVolume(v *longhorn.Volume) *longhorn.Engine {
	return &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name:      v.Name + "-e-" + util.RandomID(),
			Namespace: TestNamespace,
			Labels: map[string]string{
				"longhornvolume": v.Name,
			},
		},
		Spec: longhorn.EngineSpec{
			InstanceSpec: longhorn.InstanceSpec{
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				Image:       TestEngineImage,
				DesireState: longhorn.InstanceStateStopped,
			},
			Frontend:                  longhorn.VolumeFrontendBlockDev,
			ReplicaAddressMap:         map[string]string{},
			UpgradedReplicaAddressMap: map[string]string{},
			Active:                    true,
		},
	}
}

func newReplicaForVolume(v *longhorn.Volume, e *longhorn.Engine, nodeID, diskID string) *longhorn.Replica {
	replicaName := v.Name + "-r-" + util.RandomID()
	return &longhorn.Replica{
		ObjectMeta: metav1.ObjectMeta{
			Name: replicaName,
			Labels: map[string]string{
				"longhornvolume":          v.Name,
				types.LonghornNodeKey:     nodeID,
				types.LonghornDiskUUIDKey: diskID,
			},
		},
		Spec: longhorn.ReplicaSpec{
			InstanceSpec: longhorn.InstanceSpec{
				NodeID:      nodeID,
				VolumeName:  v.Name,
				VolumeSize:  v.Spec.Size,
				Image:       TestEngineImage,
				DesireState: longhorn.InstanceStateStopped,
			},
			EngineName:        e.Name,
			DiskID:            diskID,
			DiskPath:          TestDefaultDataPath,
			DataDirectoryName: replicaName,
			Active:            true,
		},
	}
}

func newDaemonPod(phase corev1.PodPhase, name, namespace, nodeID, podIP string, mountpropagation *corev1.MountPropagationMode) *corev1.Pod {
	podStatus := corev1.ConditionFalse
	if phase == corev1.PodRunning {
		podStatus = corev1.ConditionTrue
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app": "longhorn-manager",
			},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeID,
			Containers: []corev1.Container{
				{
					Name:  "longhorn-manager",
					Image: TestEngineImage,
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:             "longhorn",
							MountPath:        TestDefaultDataPath,
							MountPropagation: mountpropagation,
						},
					},
				},
			},
			ServiceAccountName: TestServiceAccount,
		},
		Status: corev1.PodStatus{
			Conditions: []corev1.PodCondition{
				{
					Type:   corev1.PodReady,
					Status: podStatus,
				},
			},
			Phase: phase,
			PodIP: podIP,
		},
	}
}

func generateVolumeTestCaseTemplate() *VolumeTestCase {
	volume := newVolume(TestVolumeName, 2)
	engine := newEngineForVolume(volume)
	engineImage := newEngineImage(TestEngineImage, longhorn.EngineImageStateDeployed)
	replica1 := newReplicaForVolume(volume, engine, TestNode1, TestDiskID1)
	replica2 := newReplicaForVolume(volume, engine, TestNode2, TestDiskID1)
	node1 := newNode(TestNode1, TestNamespace, true, longhorn.ConditionStatusTrue, "")
	engineImage.Status.NodeDeploymentMap[node1.Name] = true
	node2 := newNode(TestNode2, TestNamespace, false, longhorn.ConditionStatusTrue, "")
	engineImage.Status.NodeDeploymentMap[node2.Name] = true

	return &VolumeTestCase{
		volume: volume,
		engines: map[string]*longhorn.Engine{
			engine.Name: engine,
		},
		engineImage: engineImage,
		replicas: map[string]*longhorn.Replica{
			replica1.Name: replica1,
			replica2.Name: replica2,
		},
		nodes: []*longhorn.Node{
			node1,
			node2,
		},

		expectVolume:   nil,
		expectEngines:  map[string]*longhorn.Engine{},
		expectReplicas: map[string]*longhorn.Replica{},

		replicaReplenishmentWaitInterval: "0",
	}
}

func (tc *VolumeTestCase) copyCurrentToExpect() {
	tc.expectVolume = tc.volume.DeepCopy()
	for n, e := range tc.engines {
		tc.expectEngines[n] = e.DeepCopy()
	}
	for n, r := range tc.replicas {
		tc.expectReplicas[n] = r.DeepCopy()
	}
}

func (s *TestSuite) runTestCases(c *C, testCases map[string]*VolumeTestCase) {
	// testCases = map[string]*VolumeTestCase{}
	for name, tc := range testCases {
		var err error
		fmt.Printf("testing %v\n", name)

		kubeClient := fake.NewSimpleClientset()
		lhClient := lhfake.NewSimpleClientset()
		extensionsClient := apiextensionsfake.NewSimpleClientset()

		informerFactories := util.NewInformerFactories(TestNamespace, kubeClient, lhClient, controller.NoResyncPeriodFunc())

		vIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
		eIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Engines().Informer().GetIndexer()
		rIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Replicas().Informer().GetIndexer()
		nIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Nodes().Informer().GetIndexer()

		pIndexer := informerFactories.KubeInformerFactory.Core().V1().Pods().Informer().GetIndexer()
		knIndexer := informerFactories.KubeInformerFactory.Core().V1().Nodes().Informer().GetIndexer()
		sIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer()

		vc, err := newTestVolumeController(lhClient, kubeClient, extensionsClient, informerFactories, TestOwnerID1)
		c.Assert(err, IsNil)

		// Need to create daemon pod for node
		daemon1 := newDaemonPod(corev1.PodRunning, TestDaemon1, TestNamespace, TestNode1, TestIP1, nil)
		p, err := kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), daemon1, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(p)
		c.Assert(err, IsNil)
		daemon2 := newDaemonPod(corev1.PodRunning, TestDaemon2, TestNamespace, TestNode2, TestIP2, nil)
		p, err = kubeClient.CoreV1().Pods(TestNamespace).Create(context.TODO(), daemon2, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = pIndexer.Add(p)
		c.Assert(err, IsNil)

		ei, err := lhClient.LonghornV1beta2().EngineImages(TestNamespace).Create(context.TODO(), tc.engineImage, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		eiIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().EngineImages().Informer().GetIndexer()
		err = eiIndexer.Add(ei)
		c.Assert(err, IsNil)

		instanceManager1, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(
			context.TODO(),
			newInstanceManager(
				TestInstanceManagerName+"-"+TestNode1, longhorn.InstanceManagerStateRunning,
				TestOwnerID1, TestNode1, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			metav1.CreateOptions{},
		)
		c.Assert(err, IsNil)
		instanceManager2, err := lhClient.LonghornV1beta2().InstanceManagers(TestNamespace).Create(
			context.TODO(),
			newInstanceManager(
				TestInstanceManagerName+"-"+TestNode2, longhorn.InstanceManagerStateRunning,
				TestOwnerID2, TestNode2, TestIP1,
				map[string]longhorn.InstanceProcess{},
				map[string]longhorn.InstanceProcess{},
				longhorn.DataEngineTypeV1,
				TestInstanceManagerImage,
				false,
			),
			metav1.CreateOptions{},
		)
		c.Assert(err, IsNil)
		imIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().InstanceManagers().Informer().GetIndexer()
		err = imIndexer.Add(instanceManager1)
		c.Assert(err, IsNil)
		err = imIndexer.Add(instanceManager2)
		c.Assert(err, IsNil)

		if tc.volume.Spec.FromBackup != "" {
			bName, bvName, _, err := backupstore.DecodeBackupURL(tc.volume.Spec.FromBackup)
			c.Assert(err, IsNil)

			backupVolume := &longhorn.BackupVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: bvName,
					Finalizers: []string{
						longhorn.SchemeGroupVersion.Group,
					},
					Labels: map[string]string{
						types.LonghornLabelBackupTarget: TestBackupTargetName,
						types.LonghornLabelBackupVolume: bvName,
					},
				},
				Spec: longhorn.BackupVolumeSpec{
					BackupTargetName: TestBackupTargetName,
					VolumeName:       bvName,
				},
				Status: longhorn.BackupVolumeStatus{
					LastBackupName: bName,
				},
			}

			bv, err := lhClient.LonghornV1beta2().BackupVolumes(TestNamespace).Create(context.TODO(), backupVolume, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			bvIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().BackupVolumes().Informer().GetIndexer()
			err = bvIndexer.Add(bv)
			c.Assert(err, IsNil)
		}

		// Set allow-volume-creation-with-degraded-availability setting
		if tc.allowVolumeCreationWithDegradedAvailability != "" {
			s := initSettingsNameValue(
				string(types.SettingNameAllowVolumeCreationWithDegradedAvailability),
				tc.allowVolumeCreationWithDegradedAvailability)
			setting, err :=
				lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = sIndexer.Add(setting)
			c.Assert(err, IsNil)
		}
		// Set replica node soft anti-affinity setting
		if tc.replicaNodeSoftAntiAffinity != "" {
			s := initSettingsNameValue(
				string(types.SettingNameReplicaSoftAntiAffinity),
				tc.replicaNodeSoftAntiAffinity)
			setting, err :=
				lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = sIndexer.Add(setting)
			c.Assert(err, IsNil)
		}
		// Set auto salvage setting
		if tc.volumeAutoSalvage != "" {
			s := initSettingsNameValue(
				string(types.SettingNameAutoSalvage),
				tc.volumeAutoSalvage)
			setting, err :=
				lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = sIndexer.Add(setting)
			c.Assert(err, IsNil)
		}
		// Set New Replica Replenishment Wait Interval
		if tc.replicaReplenishmentWaitInterval != "" {
			s := initSettingsNameValue(
				string(types.SettingNameReplicaReplenishmentWaitInterval), tc.replicaReplenishmentWaitInterval)
			setting, err :=
				lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = sIndexer.Add(setting)
			c.Assert(err, IsNil)
		}
		// Set Default Engine Image
		s := initSettingsNameValue(
			string(types.SettingNameDefaultEngineImage), TestEngineImage)
		setting, err :=
			lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)
		// Set Default Instance Manager Image
		s = initSettingsNameValue(
			string(types.SettingNameDefaultInstanceManagerImage), TestInstanceManagerImage)
		setting, err =
			lhClient.LonghornV1beta2().Settings(TestNamespace).Create(context.TODO(), s, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = sIndexer.Add(setting)
		c.Assert(err, IsNil)

		// need to create default node
		for _, node := range tc.nodes {
			n, err := lhClient.LonghornV1beta2().Nodes(TestNamespace).Create(context.TODO(), node, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			c.Assert(n, NotNil)
			err = nIndexer.Add(n)
			c.Assert(err, IsNil)

			knodeCondition := corev1.ConditionTrue

			condition := types.GetCondition(node.Status.Conditions, longhorn.NodeConditionTypeReady)
			if condition.Status != longhorn.ConditionStatusTrue {
				knodeCondition = corev1.ConditionFalse
			}
			knode := newKubernetesNode(node.Name, knodeCondition, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionFalse, corev1.ConditionTrue)
			kn, err := kubeClient.CoreV1().Nodes().Create(context.TODO(), knode, metav1.CreateOptions{})
			c.Assert(err, IsNil)
			err = knIndexer.Add(kn)
			c.Assert(err, IsNil)
		}

		// Need to put it into both fakeclientset and Indexer
		v, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Create(context.TODO(), tc.volume, metav1.CreateOptions{})
		c.Assert(err, IsNil)
		err = vIndexer.Add(v)
		c.Assert(err, IsNil)

		if tc.engines != nil {
			for _, e := range tc.engines {
				e, err := lhClient.LonghornV1beta2().Engines(TestNamespace).Create(context.TODO(), e, metav1.CreateOptions{})
				c.Assert(err, IsNil)
				err = eIndexer.Add(e)
				c.Assert(err, IsNil)
			}
		}

		if tc.replicas != nil {
			for _, r := range tc.replicas {
				r, err = lhClient.LonghornV1beta2().Replicas(TestNamespace).Create(context.TODO(), r, metav1.CreateOptions{})
				c.Assert(err, IsNil)
				err = rIndexer.Add(r)
				c.Assert(err, IsNil)
			}
		}

		err = vc.syncVolume(getKey(v, c))
		c.Assert(err, IsNil)

		retV, err := lhClient.LonghornV1beta2().Volumes(TestNamespace).Get(context.TODO(), v.Name, metav1.GetOptions{})
		c.Assert(err, IsNil)
		c.Assert(retV.Spec, DeepEquals, tc.expectVolume.Spec)
		// mask timestamps
		for ctype, condition := range retV.Status.Conditions {
			condition.LastTransitionTime = ""
			retV.Status.Conditions[ctype] = condition
		}
		c.Assert(retV.Status, DeepEquals, tc.expectVolume.Status)

		retEs, err := lhClient.LonghornV1beta2().Engines(TestNamespace).List(context.TODO(), metav1.ListOptions{LabelSelector: getVolumeLabelSelector(v.Name)})
		c.Assert(err, IsNil)
		for _, retE := range retEs.Items {
			if tc.engines == nil {
				// test creation, name would be different
				var expectEngine *longhorn.Engine
				for _, expectEngine = range tc.expectEngines {
					break
				}
				c.Assert(retE.Spec, DeepEquals, expectEngine.Spec)
				c.Assert(retE.Status, DeepEquals, expectEngine.Status)
			} else {
				c.Assert(retE.Spec, DeepEquals, tc.expectEngines[retE.Name].Spec)
				c.Assert(retE.Status, DeepEquals, tc.expectEngines[retE.Name].Status)
			}
		}

		retRs, err := lhClient.LonghornV1beta2().Replicas(TestNamespace).List(context.TODO(), metav1.ListOptions{LabelSelector: getVolumeLabelSelector(v.Name)})
		c.Assert(err, IsNil)
		for _, retR := range retRs.Items {
			if tc.replicas == nil {
				// test creation, name would be different
				var expectR *longhorn.Replica
				for _, expectR = range tc.expectReplicas {
					break
				}
				if expectR.Spec.NodeID != "" {
					// validate DataPath and NodeID of replica have been set in scheduler
					c.Assert(retR.Spec.NodeID, Not(Equals), "")
					c.Assert(retR.Spec.DiskID, Not(Equals), "")
					c.Assert(retR.Spec.DiskPath, Not(Equals), "")
					c.Assert(retR.Spec.DataDirectoryName, Not(Equals), "")
					c.Assert(retR.Spec.NodeID, Equals, TestNode1)
				} else {
					// not schedulable
					c.Assert(retR.Spec.NodeID, Equals, "")
					c.Assert(retR.Spec.DiskID, Equals, "")
					c.Assert(retR.Spec.DiskPath, Equals, "")
					c.Assert(retR.Spec.DataDirectoryName, Equals, "")
				}
				c.Assert(retR.Status, DeepEquals, expectR.Status)
			} else {
				c.Assert(retR.Spec, DeepEquals, tc.expectReplicas[retR.Name].Spec)
				c.Assert(retR.Status, DeepEquals, tc.expectReplicas[retR.Name].Status)
			}
		}
	}
}
