package datastore

import (
	"context"
	"errors"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func modeStateClients(objects ...runtime.Object) (*lhfake.Clientset, *kubefake.Clientset) {
	settings := []runtime.Object{}
	for name, value := range map[types.SettingName]string{types.SettingNameV1DataEngine: "false", types.SettingNameV2DataEngine: "true"} {
		settings = append(settings, &longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: "longhorn-system", UID: k8stypes.UID(name), ResourceVersion: "1"}, Value: value})
	}
	return lhfake.NewClientset(append(settings, objects...)...), kubefake.NewClientset()
}

func TestV2OnlyStateChecksUncachedStorageWithoutNodes(t *testing.T) {
	for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeAll, "unrecognized"} {
		for _, kind := range []string{"volume", "engine", "replica", "instance-manager", "deleting-volume"} {
			t.Run(kind+"/"+string(engine), func(t *testing.T) {
				metadata := metav1.ObjectMeta{Name: "retained-without-node", Namespace: "longhorn-system"}
				var resource runtime.Object
				switch kind {
				case "volume", "deleting-volume":
					if kind == "deleting-volume" {
						now := metav1.Now()
						metadata.DeletionTimestamp = &now
					}
					resource = &longhorn.Volume{ObjectMeta: metadata, Spec: longhorn.VolumeSpec{DataEngine: engine}}
				case "engine":
					resource = &longhorn.Engine{ObjectMeta: metadata, Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: engine}}}
				case "replica":
					resource = &longhorn.Replica{ObjectMeta: metadata, Spec: longhorn.ReplicaSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: engine}}}
				case "instance-manager":
					resource = &longhorn.InstanceManager{ObjectMeta: metadata, Spec: longhorn.InstanceManagerSpec{DataEngine: engine}}
				}
				lh, kube := modeStateClients(resource)
				if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil {
					t.Fatal("uncached legacy, unknown or deleting storage cleared V2-only startup")
				}
			})
		}
	}
}

func TestV2OnlyStateRequiresExplicitSettings(t *testing.T) {
	for _, name := range []types.SettingName{types.SettingNameV1DataEngine, types.SettingNameV2DataEngine} {
		for _, value := range []string{"missing", "", "unexpected", map[types.SettingName]string{types.SettingNameV1DataEngine: "true", types.SettingNameV2DataEngine: "false"}[name]} {
			t.Run(string(name)+"/"+value, func(t *testing.T) {
				lh, kube := modeStateClients()
				lh.PrependReactor("get", "settings", func(action k8stesting.Action) (bool, runtime.Object, error) {
					if action.(k8stesting.GetAction).GetName() != string(name) {
						return false, nil, nil
					}
					if value == "missing" {
						return true, nil, errors.New("not found")
					}
					return true, &longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: "longhorn-system", UID: k8stypes.UID(name), ResourceVersion: "1"}, Value: value}, nil
				})
				if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil {
					t.Fatal("absent or contradictory settings cleared V2-only startup")
				}
			})
		}
	}
}

func TestV2OnlyStateRequiresCompleteSuccessfulInventories(t *testing.T) {
	for _, resource := range []string{"volumes", "engines", "replicas", "instancemanagers", "pods"} {
		for _, partial := range []bool{false, true} {
			t.Run(resource+map[bool]string{false: "/read-error", true: "/incomplete-page"}[partial], func(t *testing.T) {
				lh, kube := modeStateClients()
				read := func(k8stesting.Action) (bool, runtime.Object, error) {
					if !partial {
						return true, nil, errors.New("inventory unavailable")
					}
					metadata := metav1.ListMeta{Continue: "another-page"}
					switch resource {
					case "volumes":
						return true, &longhorn.VolumeList{ListMeta: metadata}, nil
					case "engines":
						return true, &longhorn.EngineList{ListMeta: metadata}, nil
					case "replicas":
						return true, &longhorn.ReplicaList{ListMeta: metadata}, nil
					case "instancemanagers":
						return true, &longhorn.InstanceManagerList{ListMeta: metadata}, nil
					default:
						return true, &corev1.PodList{ListMeta: metadata}, nil
					}
				}
				if resource == "pods" {
					kube.PrependReactor("list", resource, read)
				} else {
					lh.PrependReactor("list", resource, read)
				}
				if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil {
					t.Fatal("failed or incomplete uncached inventory cleared V2-only startup")
				}
			})
		}
	}
}

func TestV2OnlyStateRejectsOrphanInstanceManagerRuntime(t *testing.T) {
	for _, labels := range []map[string]string{
		nil,
		{types.GetLonghornLabelComponentKey(): types.LonghornLabelInstanceManager},
		{types.GetLonghornLabelComponentKey(): types.LonghornLabelInstanceManager, types.GetLonghornLabelKey(types.LonghornLabelDataEngine): "v1"},
		{types.GetLonghornLabelComponentKey(): types.LonghornLabelInstanceManager, types.GetLonghornLabelKey(types.LonghornLabelDataEngine): "unrecognized"},
	} {
		lh, kube := modeStateClients()
		_, err := kube.CoreV1().Pods("longhorn-system").Create(context.Background(), &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "instance-manager-orphan", Namespace: "longhorn-system", Labels: labels}}, metav1.CreateOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if err = ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil {
			t.Fatal("orphan runtime without a Node or instance-manager CR cleared V2-only startup")
		}
	}
}

func TestV1CapableStateRetainsDefaultBehavior(t *testing.T) {
	for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable} {
		if err := ValidateDataEngineModeState(context.Background(), mode, "", nil, nil); err != nil {
			t.Fatal("default mode acquired a new storage-state requirement")
		}
	}
	if err := ValidateDataEngineModeState(context.Background(), "invalid", "", nil, nil); err == nil {
		t.Fatal("unknown installation mode cleared startup")
	}
}

func TestV2OnlyStateAcceptsEmptyQualifiedInstallation(t *testing.T) {
	lh, kube := modeStateClients()
	if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err != nil {
		t.Fatal(err)
	}
}

func TestV2OnlyStateAcceptsExplicitV2StorageAndRuntime(t *testing.T) {
	metadata := metav1.ObjectMeta{Name: "instance-manager-v2", Namespace: "longhorn-system"}
	manager := modeStateV2Manager()
	lh, kube := modeStateClients(
		&longhorn.Volume{ObjectMeta: metadata, Spec: longhorn.VolumeSpec{DataEngine: longhorn.DataEngineTypeV2}},
		&longhorn.Engine{ObjectMeta: metadata, Spec: longhorn.EngineSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: longhorn.DataEngineTypeV2}}},
		&longhorn.Replica{ObjectMeta: metadata, Spec: longhorn.ReplicaSpec{InstanceSpec: longhorn.InstanceSpec{DataEngine: longhorn.DataEngineTypeV2}}},
		manager,
	)
	if _, err := kube.CoreV1().Pods(metadata.Namespace).Create(context.Background(), modeStateV2Pod(manager), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err != nil {
		t.Fatal(err)
	}
}

func modeStateV2Manager() *longhorn.InstanceManager {
	return &longhorn.InstanceManager{ObjectMeta: metav1.ObjectMeta{Name: "instance-manager-v2", Namespace: "longhorn-system", UID: "manager-uid"}, Spec: longhorn.InstanceManagerSpec{DataEngine: longhorn.DataEngineTypeV2, NodeID: "node", Image: "longhornio/longhorn-instance-manager:tested"}}
}

// Source-derived native launch profile from createInstanceManagerPodSpec.
func modeStateV2Pod(manager *longhorn.InstanceManager) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: manager.Name, Namespace: manager.Namespace, UID: "pod-uid", OwnerReferences: GetOwnerReferencesForInstanceManager(manager), Labels: types.GetInstanceManagerLabels(manager.Spec.NodeID, manager.Spec.Image, longhorn.InstanceManagerTypeAllInOne, longhorn.DataEngineTypeV2)}, Spec: corev1.PodSpec{
		NodeName: manager.Spec.NodeID,
		Containers: []corev1.Container{{Name: "instance-manager", Image: manager.Spec.Image, Args: []string{
			"start-spdk-tgt", "--spdk-log", "", "--spdk-cpumask", "0x3", "--spdk-core-number", "2", "--spdk-memory-size", "2048",
			"--longhorn-control-path", types.DefaultControlPath, "--enable-spdk", "--debug", "daemon", "--spdk-enabled", "--listen", "0.0.0.0:8500",
		}, Env: []corev1.EnvVar{{Name: types.EnvDataEngine, Value: "v2"}}}},
	}}
}

func TestV2OnlyStateRejectsContradictoryObservedProcesses(t *testing.T) {
	for _, engine := range []longhorn.DataEngineType{"", longhorn.DataEngineTypeV1, longhorn.DataEngineTypeAll, "unrecognized"} {
		for _, kind := range []string{"engine", "replica", "frontend", "shard", "shard-group"} {
			t.Run(kind+"/"+string(engine), func(t *testing.T) {
				manager := modeStateV2Manager()
				processes := map[string]longhorn.InstanceProcess{"observed-process": {Spec: longhorn.InstanceProcessSpec{Name: "observed-process", DataEngine: engine}}}
				switch kind {
				case "engine":
					manager.Status.InstanceEngines = processes
				case "replica":
					manager.Status.InstanceReplicas = processes
				case "frontend":
					manager.Status.InstanceEngineFrontends = processes
				case "shard":
					manager.Status.InstanceShards = processes
				case "shard-group":
					manager.Status.InstanceShardGroups = processes
				}
				lh, kube := modeStateClients(manager)
				if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil {
					t.Fatal("requested V2 spec concealed a non-V2 observed process")
				}
			})
		}
	}
}

func TestV2OnlyStateRejectsRelabelledOrUnboundRuntime(t *testing.T) {
	for name, change := range map[string]func(*corev1.Pod){
		"V1 launch": func(p *corev1.Pod) {
			p.Spec.Containers[0].Args = []string{"instance-manager", "--debug", "daemon", "--listen", ":8500"}
		},
		"command override": func(p *corev1.Pod) { p.Spec.Containers[0].Command = []string{"/bin/sh", "-c"} },
		"foreign image":    func(p *corev1.Pod) { p.Spec.Containers[0].Image = "another-image" },
		"foreign owner":    func(p *corev1.Pod) { p.OwnerReferences[0].UID = "another-manager" },
		"no owner":         func(p *corev1.Pod) { p.OwnerReferences = nil },
		"another node":     func(p *corev1.Pod) { p.Spec.NodeName = "another-node" },
		"another mode":     func(p *corev1.Pod) { p.Spec.Containers[0].Env[0].Value = "v1" },
		"unresolved environment": func(p *corev1.Pod) {
			p.Spec.Containers[0].Env[0].ValueFrom = &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}
		},
		"missing daemon SPDK enablement": func(p *corev1.Pod) { p.Spec.Containers[0].Args[14] = "--debug" },
		"flag hidden in option value": func(p *corev1.Pod) {
			p.Spec.Containers[0].Args[4] = "--enable-spdk"
			p.Spec.Containers[0].Args = append(p.Spec.Containers[0].Args[:11], p.Spec.Containers[0].Args[12:]...)
		},
		"unknown argument": func(p *corev1.Pod) {
			p.Spec.Containers[0].Args = append(p.Spec.Containers[0].Args, "--unknown-launch-mode")
		},
	} {
		t.Run(name, func(t *testing.T) {
			manager := modeStateV2Manager()
			pod := modeStateV2Pod(manager)
			change(pod)
			lh, kube := modeStateClients(manager)
			if _, err := kube.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
				t.Fatal(err)
			}
			if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil {
				t.Fatal("V2 labels concealed contradictory or unresolved runtime")
			}
		})
	}
	manager := modeStateV2Manager()
	pod := modeStateV2Pod(manager)
	lh, kube := modeStateClients()
	if _, err := kube.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil {
		t.Fatal("an orphan carrying V2 labels cleared runtime inventory")
	}
}

func TestV2OnlyStateAcceptsNativeOptionalSPDKProfiles(t *testing.T) {
	for _, runtimePinning := range []bool{false, true} {
		manager := modeStateV2Manager()
		pod := modeStateV2Pod(manager)
		args := pod.Spec.Containers[0].Args
		if runtimePinning {
			args[4] = ""
		}
		withPools := append([]string(nil), args[:13]...)
		withPools = append(withPools, "--spdk-iobuf-large-pool-size", "1048576", "--spdk-iobuf-small-pool-size", "2097152")
		withPools = append(withPools, args[13:]...)
		pod.Spec.Containers[0].Args = append(withPools, "--spdk-interrupt-mode", "--enable-irq-affinity", "--enable-workqueue-affinity", "--enable-rps", "--spdk-no-hugepage")
		lh, kube := modeStateClients(manager)
		if _, err := kube.CoreV1().Pods(pod.Namespace).Create(context.Background(), pod, metav1.CreateOptions{}); err != nil {
			t.Fatal(err)
		}
		if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err != nil {
			t.Fatal(err)
		}
	}
}

func TestV2OnlyStateRejectsSettingsThatChangeDuringInventory(t *testing.T) {
	for _, replace := range []bool{false, true} {
		lh, kube := modeStateClients()
		reads := 0
		lh.PrependReactor("get", "settings", func(action k8stesting.Action) (bool, runtime.Object, error) {
			if action.(k8stesting.GetAction).GetName() != string(types.SettingNameV1DataEngine) {
				return false, nil, nil
			}
			reads++
			if reads == 1 {
				return false, nil, nil
			}
			metadata := metav1.ObjectMeta{Name: string(types.SettingNameV1DataEngine), Namespace: "longhorn-system", UID: k8stypes.UID(types.SettingNameV1DataEngine), ResourceVersion: "1"}
			if replace {
				metadata.UID = "replaced-setting"
			} else {
				metadata.ResourceVersion = "2"
			}
			return true, &longhorn.Setting{ObjectMeta: metadata, Value: "false"}, nil
		})
		if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil {
			t.Fatal("changed or replaced Settings cleared the inventory observation")
		}
	}
}

func TestV2OnlyStateRefusesMissingInputsAndCanceledReads(t *testing.T) {
	lh, kube := modeStateClients()
	for _, ctx := range []context.Context{nil, func() context.Context {
		canceled, cancel := context.WithCancel(context.Background())
		cancel()
		return canceled
	}()} {
		if err := ValidateDataEngineModeState(ctx, types.DataEngineModeV2Only, "longhorn-system", lh, kube); err == nil {
			t.Fatal("missing or canceled context cleared startup")
		}
	}
	if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "", lh, kube); err == nil {
		t.Fatal("empty namespace cleared startup")
	}
	if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", nil, kube); err == nil {
		t.Fatal("missing Longhorn client cleared startup")
	}
	if err := ValidateDataEngineModeState(context.Background(), types.DataEngineModeV2Only, "longhorn-system", lh, nil); err == nil {
		t.Fatal("missing Kubernetes client cleared startup")
	}
}
