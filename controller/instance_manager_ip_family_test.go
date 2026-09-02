package controller

import (
	"encoding/json"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/engineapi"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (imc *InstanceManagerController) isSettingDataEngineIPFamilySynced(setting *longhorn.Setting, pod *corev1.Pod) (bool, error) {
	family := ""
	specified := false
	valid := false
	for _, container := range pod.Spec.Containers {
		if container.Name != "instance-manager" {
			continue
		}
		family, specified, valid = types.ParseDataEngineIPFamilyArgs(container.Args)
		break
	}
	if !valid {
		return false, nil
	}
	desired := normalizePreferredDataEngineIPFamily(setting.Value)
	if desired == types.DataEngineIPFamilyDefault {
		return !specified, nil
	}

	detached, err := imc.ds.AreAllVolumesDetachedState()
	if err != nil {
		return false, err
	}
	if !detached {
		return false, &types.ErrorInvalidState{
			Reason: "failed to apply preferred-data-engine-ip-family setting to Longhorn components when there are attached volumes. It will be eventually applied",
		}
	}

	return specified && family == desired, nil
}

func TestInstanceManagerStatusIPFamilyDistinguishesUninitializedAndDefault(t *testing.T) {
	status := longhorn.InstanceManagerStatus{}
	require.Nil(t, status.IPFamily)

	defaultFamily := types.DataEngineIPFamilyDefault
	status.IPFamily = &defaultFamily
	require.NotNil(t, status.IPFamily)
	require.Equal(t, types.DataEngineIPFamilyDefault, *status.IPFamily)

	ipv4 := types.DataEngineIPFamilyIPv4
	status.IPFamily = &ipv4
	require.Equal(t, types.DataEngineIPFamilyIPv4, *status.IPFamily)
}

func TestInstanceManagerStatusIPFamilyDeepCopyDoesNotAliasPointer(t *testing.T) {
	family := types.DataEngineIPFamilyIPv6
	original := &longhorn.InstanceManager{Status: longhorn.InstanceManagerStatus{IPFamily: &family}}
	copy := original.DeepCopy()

	require.NotNil(t, copy.Status.IPFamily)
	require.Equal(t, family, *copy.Status.IPFamily)
	*copy.Status.IPFamily = types.DataEngineIPFamilyIPv4
	require.Equal(t, types.DataEngineIPFamilyIPv6, *original.Status.IPFamily)
}

func TestInstanceProcessStatusIPFamilyIsObservable(t *testing.T) {
	process := longhorn.InstanceProcess{Status: longhorn.InstanceProcessStatus{IPFamily: types.DataEngineIPFamilyIPv6}}
	require.Equal(t, types.DataEngineIPFamilyIPv6, process.Status.IPFamily)

	process.Status.IPFamily = types.DataEngineIPFamilyDefault
	require.Equal(t, types.DataEngineIPFamilyDefault, process.Status.IPFamily)
}
func TestNormalizeV1InstanceProcessIPFamilyUsesAppliedManagerStatus(t *testing.T) {
	appliedFamily := types.DataEngineIPFamilyIPv6
	im := &longhorn.InstanceManager{
		Status: longhorn.InstanceManagerStatus{IPFamily: &appliedFamily},
	}
	instances := instanceProcessMap{
		"v1": {
			Spec: longhorn.InstanceProcessSpec{DataEngine: longhorn.DataEngineTypeV1},
			Status: longhorn.InstanceProcessStatus{
				IPFamily: types.DataEngineIPFamilyDefault,
			},
		},
		"v2": {
			Spec: longhorn.InstanceProcessSpec{DataEngine: longhorn.DataEngineTypeV2},
			Status: longhorn.InstanceProcessStatus{
				IPFamily: types.DataEngineIPFamilyDefault,
			},
		},
	}

	normalizeInstanceProcessIPFamilies(im, instances)

	require.Equal(t, types.DataEngineIPFamilyIPv6, instances["v1"].Status.IPFamily)
	require.Equal(t, types.DataEngineIPFamilyDefault, instances["v2"].Status.IPFamily)
}

func TestPerInstanceIPFamilyCapabilityVersion(t *testing.T) {
	require.Equal(t, 8, engineapi.MinInstanceManagerAPIVersionForPerInstanceIPFamily)
	require.True(t, engineapi.IsV2IPFamilySupported(engineapi.MinInstanceManagerAPIVersionForPerInstanceIPFamily, types.DataEngineIPFamilyIPv4))
	require.False(t, engineapi.IsV2IPFamilySupported(engineapi.MinInstanceManagerAPIVersionForPerInstanceIPFamily-1, types.DataEngineIPFamilyIPv6))
}

func TestInitializeInstanceManagerIPFamilyUsesPeerConsensus(t *testing.T) {
	ipv4 := types.DataEngineIPFamilyIPv4
	ipv6 := types.DataEngineIPFamilyIPv6
	legacyEmpty := ""
	tests := []struct {
		name          string
		peers         []*longhorn.InstanceManager
		wantFamily    string
		wantConsensus bool
	}{
		{name: "no initialized peers default to default", wantFamily: types.DataEngineIPFamilyDefault, wantConsensus: true},
		{
			name: "legacy empty peer normalizes to default",
			peers: []*longhorn.InstanceManager{
				newIPFamilyTestInstanceManager("peer-empty", longhorn.DataEngineTypeV1, &legacyEmpty, true),
			},
			wantFamily: types.DataEngineIPFamilyDefault, wantConsensus: true,
		},
		{
			name: "one initialized peer",
			peers: []*longhorn.InstanceManager{
				newIPFamilyTestInstanceManager("peer-a", longhorn.DataEngineTypeV1, &ipv4, true),
			},
			wantFamily: ipv4, wantConsensus: true,
		},
		{
			name: "matching initialized peers",
			peers: []*longhorn.InstanceManager{
				newIPFamilyTestInstanceManager("peer-a", longhorn.DataEngineTypeV1, &ipv6, true),
				newIPFamilyTestInstanceManager("peer-b", longhorn.DataEngineTypeV1, &ipv6, true),
			},
			wantFamily: ipv6, wantConsensus: true,
		},
		{
			name: "disagreement remains unsynchronized",
			peers: []*longhorn.InstanceManager{
				newIPFamilyTestInstanceManager("peer-a", longhorn.DataEngineTypeV1, &ipv4, true),
				newIPFamilyTestInstanceManager("peer-b", longhorn.DataEngineTypeV1, &ipv6, true),
			},
			wantFamily: "", wantConsensus: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV1, nil, false)
			sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false, append(tc.peers, target)...)
			imc := &InstanceManagerController{
				baseController: newBaseController("longhorn-instance-manager", logrus.StandardLogger()),
				ds:             sc.ds,
			}

			gotFamily, gotConsensus, err := imc.initializeInstanceManagerIPFamily(target)
			require.NoError(t, err)
			require.Equal(t, tc.wantFamily, gotFamily)
			require.Equal(t, tc.wantConsensus, gotConsensus)
		})
	}
}

func TestInitializeInstanceManagerIPFamilyNeverAdoptsDesiredSetting(t *testing.T) {
	target := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV1, nil, false)
	sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false, target)
	imc := &InstanceManagerController{
		baseController: newBaseController("longhorn-instance-manager", logrus.StandardLogger()),
		ds:             sc.ds,
	}

	family, consensus, err := imc.initializeInstanceManagerIPFamily(target)
	require.NoError(t, err)
	require.True(t, consensus)
	require.Equal(t, types.DataEngineIPFamilyDefault, family)
}

func TestSyncInstanceManagerIPFamilyBlocksAttachedVolumesWithoutMutation(t *testing.T) {
	oldFamily := types.DataEngineIPFamilyIPv4
	target := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV1, &oldFamily, true)
	sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, true, target)
	volume := newVolume(TestVolumeName, 1)
	volume.Namespace = TestNamespace
	volume.Status.State = longhorn.VolumeStateAttached
	require.NoError(t, sc.ds.VolumeInformer.GetStore().Add(volume))
	imc := &InstanceManagerController{
		baseController: newBaseController("longhorn-instance-manager", logrus.StandardLogger()),
		ds:             sc.ds,
	}

	blocked, err := imc.syncInstanceManagerIPFamily(target)
	require.NoError(t, err)
	require.True(t, blocked)
	require.NotNil(t, target.Status.IPFamily)
	require.Equal(t, oldFamily, *target.Status.IPFamily)
	require.Equal(t, longhorn.ConditionStatusFalse,
		types.GetCondition(target.Status.Conditions, longhorn.InstanceManagerConditionTypeSettingSynced).Status)
}

func TestSyncInstanceManagerIPFamilyRejectsOldV2ExplicitFamilyWithoutRestart(t *testing.T) {
	oldFamily := types.DataEngineIPFamilyIPv4
	target := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV2, &oldFamily, true)
	target.Status.CurrentState = longhorn.InstanceManagerStateRunning
	target.Status.APIVersion = engineapi.MinInstanceManagerAPIVersionForPerInstanceIPFamily - 1
	sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false, target)
	v2Setting, err := sc.ds.GetSettingWithAutoFillingRO(types.SettingNameV2DataEngine)
	require.NoError(t, err)
	v2Setting.Value = "true"
	imc := &InstanceManagerController{
		baseController: newBaseController("longhorn-instance-manager", logrus.StandardLogger()),
		ds:             sc.ds,
	}

	blocked, err := imc.syncInstanceManagerIPFamily(target)
	require.NoError(t, err)
	require.True(t, blocked)
	require.Equal(t, oldFamily, *target.Status.IPFamily)
	require.Equal(t, longhorn.ConditionStatusFalse,
		types.GetCondition(target.Status.Conditions, longhorn.InstanceManagerConditionTypeSettingSynced).Status)
}

func TestCheckDataEngineIPFamilyForStorageNetworkKeepsControlEndpoint(t *testing.T) {
	family := types.DataEngineIPFamilyIPv6
	im := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV1, &family, true)
	im.Status.IP = "192.0.2.20"
	sc, _ := newIPFamilySettingControllerFixture(t, family, false, im)

	storageSetting := newSetting(string(types.SettingNameStorageNetwork), "longhorn-system/storage")
	require.NoError(t, sc.ds.SettingInformer.GetStore().Add(storageSetting))
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "target",
			Namespace: TestNamespace,
			Annotations: map[string]string{
				string(types.CNIAnnotationNetworks): types.CreateCniAnnotationFromSetting(storageSetting, types.StorageNetworkInterface),
			},
		},
		Status: corev1.PodStatus{
			PodIP:  "192.0.2.20",
			PodIPs: []corev1.PodIP{{IP: "192.0.2.20"}},
		},
	}

	imc := &InstanceManagerController{
		baseController: newBaseController("longhorn-instance-manager", logrus.StandardLogger()),
		ds:             sc.ds,
	}
	blocked, err := imc.checkDataEngineIPFamilyForStorageNetwork(im, pod)
	require.NoError(t, err)
	require.True(t, blocked)
	require.Equal(t, "192.0.2.20", im.Status.IP)
	require.NotNil(t, im.Status.IPFamily)
	require.Equal(t, family, *im.Status.IPFamily)
	require.Equal(t, longhorn.ConditionStatusFalse,
		types.GetCondition(im.Status.Conditions, longhorn.InstanceManagerConditionTypeSettingSynced).Status)
}

func TestActiveInstanceManagerBackupStateGate(t *testing.T) {
	for _, state := range []string{"", "new", "pending", "in_progress", "in-progress", "starting"} {
		require.True(t, isActiveInstanceManagerBackupState(state), "state %q should block family transitions", state)
	}
	for _, state := range []string{"completed", "failed", "unknown"} {
		require.False(t, isActiveInstanceManagerBackupState(state), "state %q should not block family transitions", state)
	}
}
func TestInstanceManagerInstancesAllowIPFamilyTransition(t *testing.T) {
	require.True(t, instanceManagerInstancesAllowIPFamilyTransition(nil))
	require.True(t, instanceManagerInstancesAllowIPFamilyTransition(map[string]longhorn.InstanceProcess{
		"replica": {
			Status: longhorn.InstanceProcessStatus{
				Type:  longhorn.InstanceTypeReplica,
				State: longhorn.InstanceStateStopped,
			},
		},
	}))

	for name, process := range map[string]longhorn.InstanceProcess{
		"engine": {
			Status: longhorn.InstanceProcessStatus{Type: longhorn.InstanceTypeEngine},
		},
		"frontend": {
			Status: longhorn.InstanceProcessStatus{Type: longhorn.InstanceTypeEngineFrontend},
		},
		"running replica": {
			Status: longhorn.InstanceProcessStatus{
				Type:  longhorn.InstanceTypeReplica,
				State: longhorn.InstanceStateRunning,
			},
		},
		"exposed replica": {
			Status: longhorn.InstanceProcessStatus{
				Type:      longhorn.InstanceTypeReplica,
				State:     longhorn.InstanceStateStopped,
				PortStart: 20001,
				PortEnd:   20001,
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			require.False(t, instanceManagerInstancesAllowIPFamilyTransition(map[string]longhorn.InstanceProcess{
				name: process,
			}))
		})
	}
}

func TestSyncInstanceManagerIPFamilyConvergesV1FromAppliedTarget(t *testing.T) {
	oldFamily := types.DataEngineIPFamilyIPv4
	target := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV1, &oldFamily, true)
	sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false, target)
	imc := &InstanceManagerController{
		baseController: newBaseController("longhorn-instance-manager", logrus.StandardLogger()),
		ds:             sc.ds,
	}

	blocked, err := imc.syncInstanceManagerIPFamily(target)
	require.NoError(t, err)
	require.False(t, blocked)
	require.NotNil(t, target.Status.IPFamily)
	require.Equal(t, types.DataEngineIPFamilyIPv6, *target.Status.IPFamily)
}

func TestSyncInstanceManagerIPFamilyKeepsAlreadyMatchingAttachedManagerSynced(t *testing.T) {
	family := types.DataEngineIPFamilyIPv6
	target := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV1, &family, true)
	sc, _ := newIPFamilySettingControllerFixture(t, family, true, target)
	volume := newVolume(TestVolumeName, 1)
	volume.Namespace = TestNamespace
	volume.Status.State = longhorn.VolumeStateAttached
	require.NoError(t, sc.ds.VolumeInformer.GetStore().Add(volume))
	imc := &InstanceManagerController{
		baseController: newBaseController("longhorn-instance-manager", logrus.StandardLogger()),
		ds:             sc.ds,
	}

	blocked, err := imc.syncInstanceManagerIPFamily(target)
	require.NoError(t, err)
	require.False(t, blocked)
	require.Equal(t, longhorn.ConditionStatusTrue,
		types.GetCondition(target.Status.Conditions, longhorn.InstanceManagerConditionTypeSettingSynced).Status)
}

func TestInitializeInstanceManagerIPFamilyIncludesCrossEnginePeers(t *testing.T) {
	ipv4 := types.DataEngineIPFamilyIPv4
	ipv6 := types.DataEngineIPFamilyIPv6
	tests := []struct {
		name       string
		peers      []*longhorn.InstanceManager
		wantFamily string
		consensus  bool
	}{
		{
			name: "opposite-engine initialized peer supplies family",
			peers: []*longhorn.InstanceManager{
				newIPFamilyTestInstanceManager("v2-peer", longhorn.DataEngineTypeV2, &ipv6, true),
			},
			wantFamily: ipv6,
			consensus:  true,
		},
		{
			name: "opposite-engine disagreement blocks initialization",
			peers: []*longhorn.InstanceManager{
				newIPFamilyTestInstanceManager("v1-peer", longhorn.DataEngineTypeV1, &ipv4, true),
				newIPFamilyTestInstanceManager("v2-peer", longhorn.DataEngineTypeV2, &ipv6, true),
			},
			consensus: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV1, nil, false)
			sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false, append(tc.peers, target)...)
			v2Setting, err := sc.ds.GetSettingWithAutoFillingRO(types.SettingNameV2DataEngine)
			require.NoError(t, err)
			v2Setting.Value = "true"
			imc := &InstanceManagerController{
				baseController: newBaseController("longhorn-instance-manager", logrus.StandardLogger()),
				ds:             sc.ds,
			}

			family, consensus, err := imc.initializeInstanceManagerIPFamily(target)
			require.NoError(t, err)
			require.Equal(t, tc.wantFamily, family)
			require.Equal(t, tc.consensus, consensus)
		})
	}
}

func TestSyncInstanceManagerIPFamilyBlocksActiveV2Instances(t *testing.T) {
	family := types.DataEngineIPFamilyIPv4
	tests := []struct {
		name string
		set  func(*longhorn.InstanceManager)
	}{
		{
			name: "engine",
			set: func(im *longhorn.InstanceManager) {
				im.Status.InstanceEngines = map[string]longhorn.InstanceProcess{
					"engine-a": {},
				}
			},
		},
		{
			name: "engine frontend",
			set: func(im *longhorn.InstanceManager) {
				im.Status.InstanceEngineFrontends = map[string]longhorn.InstanceProcess{
					"frontend-a": {},
				}
			},
		},
		{
			name: "exposed replica",
			set: func(im *longhorn.InstanceManager) {
				im.Status.InstanceReplicas = map[string]longhorn.InstanceProcess{
					"replica-a": {
						Status: longhorn.InstanceProcessStatus{
							State:    longhorn.InstanceStateRunning,
							Endpoint: "192.0.2.20:4420",
						},
					},
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV2, &family, true)
			target.Status.CurrentState = longhorn.InstanceManagerStateRunning
			target.Status.APIVersion = engineapi.MinInstanceManagerAPIVersionForPerInstanceIPFamily
			tc.set(target)
			sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false, target)
			v2Setting, err := sc.ds.GetSettingWithAutoFillingRO(types.SettingNameV2DataEngine)
			require.NoError(t, err)
			v2Setting.Value = "true"
			imc := &InstanceManagerController{
				baseController: newBaseController("longhorn-instance-manager", logrus.StandardLogger()),
				ds:             sc.ds,
			}

			blocked, err := imc.syncInstanceManagerIPFamily(target)
			require.NoError(t, err)
			require.True(t, blocked)
			require.Equal(t, family, *target.Status.IPFamily)
		})
	}
}

func TestInstanceStorageIPUsesAppliedFamilyAndClearsOnFailure(t *testing.T) {
	storageNetwork := "longhorn-system/dual-stack"
	storageSetting := newSetting(string(types.SettingNameStorageNetwork), storageNetwork)
	sc, _ := newIPFamilySettingControllerFixture(t, types.DataEngineIPFamilyIPv6, false)
	require.NoError(t, sc.ds.SettingInformer.GetStore().Add(storageSetting))

	networkStatus, err := json.Marshal([]types.CniNetwork{{
		Name: storageNetwork,
		IPs:  []string{"192.0.2.20", "2001:db8::20"},
	}})
	require.NoError(t, err)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "target",
			Namespace: TestNamespace,
			Annotations: map[string]string{
				string(types.CNIAnnotationNetworkStatus): string(networkStatus),
			},
		},
		Status: corev1.PodStatus{PodIP: "192.0.2.20"},
	}
	require.NoError(t, sc.ds.PodInformer.GetStore().Add(pod))

	appliedFamily := types.DataEngineIPFamilyIPv6
	im := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV1, &appliedFamily, true)
	im.Namespace = TestNamespace
	im.Status.CurrentState = longhorn.InstanceManagerStateRunning
	im.Status.IP = "192.0.2.10"
	im.Spec.NodeID = TestNode1
	spec := &longhorn.InstanceSpec{VolumeName: "volume-a", NodeID: TestNode1, Image: "engine-image"}
	status := &longhorn.InstanceStatus{StorageIP: "192.0.2.99"}
	instance := longhorn.InstanceProcess{
		Spec: longhorn.InstanceProcessSpec{Name: "engine-a", DataEngine: longhorn.DataEngineTypeV1},
		Status: longhorn.InstanceProcessStatus{
			State:     longhorn.InstanceStateRunning,
			IPFamily:  types.DataEngineIPFamilyIPv4,
			PortStart: 4420,
		},
	}
	handler := &InstanceHandler{ds: sc.ds}
	handler.syncStatusWithInstanceManager(logrus.NewEntry(logrus.New()), im, instance.Spec.Name, spec, status,
		map[string]longhorn.InstanceProcess{instance.Spec.Name: instance})
	require.Equal(t, "2001:db8::20", status.StorageIP)

	networkStatus, err = json.Marshal([]types.CniNetwork{{
		Name: storageNetwork,
		IPs:  []string{"192.0.2.20"},
	}})
	require.NoError(t, err)
	pod.Annotations[string(types.CNIAnnotationNetworkStatus)] = string(networkStatus)
	require.NoError(t, sc.ds.PodInformer.GetStore().Update(pod))
	status.StorageIP = "192.0.2.99"
	handler.syncStatusWithInstanceManager(logrus.NewEntry(logrus.New()), im, instance.Spec.Name, spec, status,
		map[string]longhorn.InstanceProcess{instance.Spec.Name: instance})
	require.Empty(t, status.StorageIP)
}

func TestPeerInitializedFamilyStillValidatesMissingEndpoint(t *testing.T) {
	family := types.DataEngineIPFamilyIPv6
	peer := newIPFamilyTestInstanceManager("peer", longhorn.DataEngineTypeV1, &family, true)
	target := newIPFamilyTestInstanceManager("target", longhorn.DataEngineTypeV1, nil, false)
	sc, _ := newIPFamilySettingControllerFixture(t, family, false, peer, target)
	storageSetting := newSetting(string(types.SettingNameStorageNetwork), "longhorn-system/storage")
	require.NoError(t, sc.ds.SettingInformer.GetStore().Add(storageSetting))
	networkStatus, err := json.Marshal([]types.CniNetwork{{
		Name: storageSetting.Value,
		IPs:  []string{"192.0.2.20"},
	}})
	require.NoError(t, err)
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      target.Name,
			Namespace: TestNamespace,
			Annotations: map[string]string{
				string(types.CNIAnnotationNetworks):      types.CreateCniAnnotationFromSetting(storageSetting, types.StorageNetworkInterface),
				string(types.CNIAnnotationNetworkStatus): string(networkStatus),
			},
		},
		Status: corev1.PodStatus{PodIP: "192.0.2.20"},
	}
	require.NoError(t, sc.ds.PodInformer.GetStore().Add(pod))
	target.Status.IP = "192.0.2.10"
	imc := &InstanceManagerController{
		baseController: newBaseController("longhorn-instance-manager", logrus.StandardLogger()),
		ds:             sc.ds,
	}

	blocked, err := imc.syncInstanceManagerIPFamily(target)
	require.NoError(t, err)
	require.False(t, blocked)
	require.NotNil(t, target.Status.IPFamily)
	require.Equal(t, family, *target.Status.IPFamily)

	blocked, err = imc.checkDataEngineIPFamilyForStorageNetwork(target, pod)
	require.NoError(t, err)
	require.True(t, blocked)
	require.Equal(t, "192.0.2.10", target.Status.IP)
	require.Equal(t, family, *target.Status.IPFamily)
}
