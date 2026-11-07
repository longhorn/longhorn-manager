package datastore

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"

	"k8s.io/client-go/kubernetes"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhclientset "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned"
)

type dataEngineSettingIdentity struct {
	uid             k8stypes.UID
	resourceVersion string
}

func v2OnlyEngineSettingValue(name types.SettingName) string {
	if name == types.SettingNameV2DataEngine {
		return "true"
	}
	return "false"
}

func dataEngineModeSettingIdentity(setting *longhorn.Setting, name types.SettingName, namespace string) (dataEngineSettingIdentity, error) {
	if setting == nil || setting.Name != string(name) || setting.Namespace != namespace || setting.UID == "" || setting.ResourceVersion == "" || setting.DeletionTimestamp != nil || setting.Value != v2OnlyEngineSettingValue(name) {
		return dataEngineSettingIdentity{}, errors.New("v2-only mode requires explicit v1-data-engine=false and v2-data-engine=true Settings")
	}
	return dataEngineSettingIdentity{setting.UID, setting.ResourceVersion}, nil
}

func readDataEngineModeSettings(ctx context.Context, namespace string, lhClient lhclientset.Interface, allowMissing bool) ([2]dataEngineSettingIdentity, error) {
	var result [2]dataEngineSettingIdentity
	for index, name := range []types.SettingName{types.SettingNameV1DataEngine, types.SettingNameV2DataEngine} {
		setting, err := lhClient.LonghornV1beta2().Settings(namespace).Get(ctx, string(name), metav1.GetOptions{})
		if allowMissing && apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return result, errors.Wrapf(err, "failed to read explicit %s setting for v2-only validation", name)
		}
		identity, err := dataEngineModeSettingIdentity(setting, name, namespace)
		if err != nil {
			return result, err
		}
		result[index] = identity
	}
	return result, nil
}

// ValidateDataEngineModeState uses uncached clients so retained storage and
// orphan runtime cannot disappear behind an empty or node-scoped informer.
// Compatible Settings must already exist; admission/bootstrap is a separate
// phase. This check neither changes Settings nor removes legacy resources.
func ValidateDataEngineModeState(ctx context.Context, mode types.DataEngineMode, namespace string, lhClient lhclientset.Interface, kubeClient kubernetes.Interface) error {
	return validateDataEngineModeState(ctx, mode, namespace, lhClient, kubeClient, false)
}

// ValidateDataEngineModeBootstrapState permits absent engine Settings only
// before storage or instance-manager runtime exists. It never creates Settings.
func ValidateDataEngineModeBootstrapState(ctx context.Context, mode types.DataEngineMode, namespace string, lhClient lhclientset.Interface, kubeClient kubernetes.Interface) error {
	return validateDataEngineModeState(ctx, mode, namespace, lhClient, kubeClient, true)
}

func validateDataEngineModeState(ctx context.Context, mode types.DataEngineMode, namespace string, lhClient lhclientset.Interface, kubeClient kubernetes.Interface, allowMissing bool) error {
	parsed, err := types.ParseDataEngineMode(string(mode))
	if err != nil {
		return err
	}
	if parsed != types.DataEngineModeV2Only {
		return nil
	}
	if ctx == nil || namespace == "" || lhClient == nil || kubeClient == nil {
		return errors.New("v2-only validation requires a context, namespace and both API clients")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	before, err := readDataEngineModeSettings(ctx, namespace, lhClient, allowMissing)
	if err != nil {
		return err
	}
	checkList := func(kind, continuation string, err error) error {
		if err != nil {
			return errors.Wrapf(err, "failed to inventory %s for v2-only validation", kind)
		}
		if continuation != "" {
			return errors.Errorf("incomplete %s inventory for v2-only validation", kind)
		}
		return nil
	}
	checkEngine := func(kind, name string, engine longhorn.DataEngineType) error {
		if engine != longhorn.DataEngineTypeV2 {
			return errors.Errorf("cannot use v2-only mode while %s %s has data engine %q", kind, name, engine)
		}
		return nil
	}
	volumes, err := lhClient.LonghornV1beta2().Volumes(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return checkList("volumes", "", err)
	}
	if err := checkList("volumes", volumes.Continue, nil); err != nil {
		return err
	}
	storagePresent := len(volumes.Items) != 0
	for _, volume := range volumes.Items {
		if err := checkEngine("volume", volume.Name, volume.Spec.DataEngine); err != nil {
			return err
		}
	}
	engines, err := lhClient.LonghornV1beta2().Engines(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return checkList("engines", "", err)
	}
	if err := checkList("engines", engines.Continue, nil); err != nil {
		return err
	}
	storagePresent = storagePresent || len(engines.Items) != 0
	for _, engine := range engines.Items {
		if err := checkEngine("engine", engine.Name, engine.Spec.DataEngine); err != nil {
			return err
		}
	}
	replicas, err := lhClient.LonghornV1beta2().Replicas(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return checkList("replicas", "", err)
	}
	if err := checkList("replicas", replicas.Continue, nil); err != nil {
		return err
	}
	storagePresent = storagePresent || len(replicas.Items) != 0
	for _, replica := range replicas.Items {
		if err := checkEngine("replica", replica.Name, replica.Spec.DataEngine); err != nil {
			return err
		}
	}
	managers, err := lhClient.LonghornV1beta2().InstanceManagers(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return checkList("instance managers", "", err)
	}
	if err := checkList("instance managers", managers.Continue, nil); err != nil {
		return err
	}
	storagePresent = storagePresent || len(managers.Items) != 0
	names := map[string]longhorn.InstanceManager{}
	for _, manager := range managers.Items {
		if err := checkEngine("instance manager", manager.Name, manager.Spec.DataEngine); err != nil {
			return err
		}
		for _, processes := range []map[string]longhorn.InstanceProcess{manager.Status.InstanceEngines, manager.Status.InstanceReplicas, manager.Status.InstanceEngineFrontends, manager.Status.InstanceShards, manager.Status.InstanceShardGroups} {
			for name, process := range processes {
				if err := checkEngine("observed instance process", name, process.Spec.DataEngine); err != nil {
					return err
				}
			}
		}
		names[manager.Name] = manager
	}
	pods, err := kubeClient.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return checkList("pods", "", err)
	}
	if err := checkList("pods", pods.Continue, nil); err != nil {
		return err
	}
	for _, pod := range pods.Items {
		manager, matched := names[pod.Name]
		instanceManager := matched || strings.HasPrefix(pod.Name, "instance-manager-") || pod.Labels[types.GetLonghornLabelComponentKey()] == types.LonghornLabelInstanceManager
		for _, owner := range pod.OwnerReferences {
			instanceManager = instanceManager || owner.Kind == "InstanceManager"
		}
		if instanceManager {
			storagePresent = true
			if err := checkEngine("instance-manager pod", pod.Name, longhorn.DataEngineType(pod.Labels[types.GetLonghornLabelKey(types.LonghornLabelDataEngine)])); err != nil {
				return err
			}
			owner := metav1.GetControllerOf(&pod)
			if !matched || manager.UID == "" || manager.Spec.NodeID == "" || manager.Spec.Image == "" || pod.UID == "" || pod.Namespace != namespace || pod.Spec.NodeName != manager.Spec.NodeID ||
				len(pod.OwnerReferences) != 1 || owner == nil || owner.UID != manager.UID || owner.Name != manager.Name || owner.Kind != types.LonghornKindInstanceManager || owner.APIVersion != longhorn.SchemeGroupVersion.String() ||
				len(pod.Spec.Containers) != 1 || len(pod.Spec.InitContainers) != 0 || len(pod.Spec.EphemeralContainers) != 0 {
				return errors.New("instance-manager runtime has unresolved ownership or placement")
			}
			container := pod.Spec.Containers[0]
			if container.Name != "instance-manager" || container.Image != manager.Spec.Image || len(container.Command) != 0 || len(container.EnvFrom) != 0 || !nativeV2InstanceManagerArgs(container.Args) {
				return errors.New("instance-manager runtime does not use the native V2 launch profile")
			}
			modeEnvs := 0
			for _, env := range container.Env {
				if env.Name == types.EnvDataEngine {
					modeEnvs++
					if env.Value != string(longhorn.DataEngineTypeV2) || env.ValueFrom != nil {
						return errors.New("instance-manager runtime has unresolved data-engine environment")
					}
				}
			}
			if modeEnvs != 1 {
				return errors.New("instance-manager runtime lacks one explicit V2 data-engine environment")
			}
		}
	}
	after, err := readDataEngineModeSettings(ctx, namespace, lhClient, allowMissing)
	if err != nil {
		return err
	}
	if before != after {
		return errors.New("data engine Settings changed during v2-only validation")
	}
	if storagePresent && (before[0].uid == "" || before[1].uid == "") {
		return errors.New("cannot bootstrap missing engine Settings while storage or instance-manager runtime exists")
	}
	return ctx.Err()
}

// The controller's start-spdk-tgt branch selects V2; its instance-manager
// branch selects V1. Parse option/value boundaries instead of finding flag
// strings that could instead be another option's value. Unknown profiles hold
// the opt-in transition, never silently reclassify runtime from mutable labels.
func nativeV2InstanceManagerArgs(args []string) bool {
	if len(args) == 0 || args[0] != "start-spdk-tgt" {
		return false
	}
	seen := map[string]bool{}
	daemon := false
	for index := 1; index < len(args); index++ {
		option := args[index]
		if option == "daemon" && !daemon {
			daemon = true
			continue
		}
		if seen[option] {
			return false
		}
		seen[option] = true
		value, flag := false, false
		if !daemon {
			switch option {
			case "--spdk-log", "--spdk-cpumask", "--spdk-core-number", "--spdk-memory-size", "--spdk-iobuf-large-pool-size", "--spdk-iobuf-small-pool-size", "--longhorn-control-path":
				value = true
			case "--enable-spdk", "--debug":
				flag = true
			}
		} else {
			switch option {
			case "--listen":
				value = true
			case "--spdk-enabled", "--spdk-interrupt-mode", "--enable-irq-affinity", "--enable-workqueue-affinity", "--enable-rps", "--spdk-no-hugepage":
				flag = true
			}
		}
		if value {
			index++
			if index == len(args) {
				return false
			}
		} else if !flag {
			return false
		}
	}
	for _, required := range []string{"--spdk-log", "--spdk-cpumask", "--spdk-core-number", "--spdk-memory-size", "--longhorn-control-path", "--enable-spdk", "--debug", "--spdk-enabled", "--listen"} {
		if !seen[required] {
			return false
		}
	}
	return daemon
}
