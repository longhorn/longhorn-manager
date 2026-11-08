package controller

import (
	"context"
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	sigsyaml "sigs.k8s.io/yaml"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func validateV2OnlyManagerLaunch(container corev1.Container, command, managerImage string) error {
	parsed, err := parseArchivedManagerLaunch(container, command, managerImage)
	if err != nil {
		return err
	}
	if command == "daemon" || command == "global" {
		if parsed["--data-engine-mode"] != "v2-only" {
			return fmt.Errorf("archived manager would change the installation capability")
		}
	}
	if command == "daemon" {
		for _, flag := range []string{"--engine-image", "--instance-manager-image", "--share-manager-image", "--backing-image-manager-image", "--support-bundle-manager-image", "--manager-image", "--service-account"} {
			if parsed[flag] == "" {
				return fmt.Errorf("archived manager lacks a required native launch value")
			}
		}
	}
	if (command == "daemon" || command == "deploy-driver") && parsed["--manager-image"] != managerImage {
		return fmt.Errorf("archived manager would change the installation image")
	}
	if command == "csi" {
		if parsed["--drivername"] != types.LonghornDriverName || parsed["--nodeid"] == "" || parsed["--endpoint"] == "" || parsed["--manager-url"] == "" {
			return fmt.Errorf("archived CSI launch is unresolved")
		}
	}
	if command == "deploy-driver" && parsed["--manager-url"] == "" {
		return fmt.Errorf("archived driver deployer launch is unresolved")
	}
	return nil
}

func parseArchivedManagerLaunch(container corev1.Container, command, managerImage string) (map[string]string, error) {
	if managerImage == "" || container.Image != managerImage || (command != "daemon" && command != "global" && command != "csi" && command != "deploy-driver") {
		return nil, fmt.Errorf("archived manager image or command is incompatible with v2-only mode")
	}
	argv := append([]string{}, container.Command...)
	argv = append(argv, container.Args...)
	if len(argv) == 0 || argv[0] != "longhorn-manager" {
		return nil, fmt.Errorf("archived manager does not use a native launch")
	}
	index := 1
	rootFlags := map[string]bool{}
	for index < len(argv) {
		flag := ""
		switch argv[index] {
		case "-d", "--debug":
			flag = "debug"
		case "-j", "--log-json":
			flag = "json"
		}
		if flag == "" {
			break
		}
		if rootFlags[flag] {
			return nil, fmt.Errorf("archived manager has duplicate root flags")
		}
		rootFlags[flag] = true
		index++
	}
	if index >= len(argv) || argv[index] != command {
		return nil, fmt.Errorf("archived manager subcommand is unresolved")
	}
	values := map[string]bool{}
	booleans := map[string]bool{}
	if command == "daemon" || command == "global" {
		values["--data-engine-mode"], values["--kube-config"] = true, true
	}
	if command == "daemon" {
		for _, flag := range []string{"--engine-image", "--instance-manager-image", "--share-manager-image", "--backing-image-manager-image", "--support-bundle-manager-image", "--manager-image", "--service-account"} {
			values[flag] = true
		}
		booleans["--upgrade-version-check"] = true
	}
	if command == "csi" {
		for _, flag := range []string{"--nodeid", "--endpoint", "--drivername", "--manager-url"} {
			values[flag] = true
		}
		booleans["--volume-group-snapshot-enabled"] = true
	}
	if command == "deploy-driver" {
		for _, flag := range []string{"--manager-image", "--manager-url", "--csi-attacher-image", "--csi-provisioner-image", "--csi-resizer-image", "--csi-snapshotter-image", "--csi-node-driver-registrar-image", "--csi-liveness-probe-image"} {
			values[flag] = true
		}
	}
	seen, parsed := map[string]bool{}, map[string]string{}
	for index++; index < len(argv); index++ {
		name, value, assigned := strings.Cut(argv[index], "=")
		if seen[name] || name == "--" {
			return nil, fmt.Errorf("archived manager has ambiguous options")
		}
		seen[name] = true
		if booleans[name] {
			if assigned && value != "true" && value != "false" {
				return nil, fmt.Errorf("archived manager has an invalid Boolean option")
			}
			continue
		}
		if !values[name] {
			return nil, fmt.Errorf("archived manager has an unknown launch option")
		}
		if !assigned {
			index++
			if index >= len(argv) || strings.HasPrefix(argv[index], "-") {
				return nil, fmt.Errorf("archived manager option lacks a literal value")
			}
			value = argv[index]
		}
		if value == "" {
			return nil, fmt.Errorf("archived manager has an empty option value")
		}
		parsed[name] = value
	}
	return parsed, nil
}

func (c *SystemRolloutController) prepareV2OnlySystemRestore() error {
	if c.ds == nil {
		return fmt.Errorf("missing system restore datastore")
	}
	mode, err := types.ParseDataEngineMode(string(c.ds.DataEngineMode()))
	if err != nil || mode != types.DataEngineModeV2Only {
		return err
	}
	// Restoring may resume in a new process or after a failed attempt. Never
	// trust the old in-memory cache, including when a fresh read fails.
	c.extractedResources = extractedResources{}
	if c.systemRestore == nil || c.downloadPath == "" {
		return fmt.Errorf("missing system restore archive identity")
	}
	resources, err := loadV2OnlySystemRestoreArchive(c.getYAMLDirectory(""))
	if err != nil {
		return err
	}
	if err := validateV2OnlySystemRestoreProfile(&resources, c.systemRestore.Namespace, c.archiveManagerImage); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := c.ds.ValidateDataEngineModeState(ctx); err != nil {
		return err
	}
	if err := c.ds.ValidateDataEngineModeRestoreCRDs(ctx, resources.customResourceDefinitionList); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.extractedResources = resources
	return nil
}

func validateV2OnlySystemRestoreProfile(resources *extractedResources, namespace, managerImage string) error {
	if resources == nil || namespace == "" || managerImage == "" {
		return fmt.Errorf("v2-only restore requires an identified archive and installation")
	}
	lists := []runtime.Object{
		resources.customResourceDefinitionList, resources.clusterRoleList, resources.clusterRoleBindingList,
		resources.roleList, resources.roleBindingList, resources.daemonSetList, resources.deploymentList,
		resources.configMapList, resources.persistentVolumeList, resources.persistentVolumeClaimList,
		resources.serviceList, resources.serviceAccountList, resources.storageClassList,
		resources.engineImageList, resources.recurringJobList, resources.settingList,
		resources.volumeList, resources.backingImageList, resources.backupTargetList,
	}
	for _, list := range lists {
		if reflect.ValueOf(list).IsNil() {
			return fmt.Errorf("v2-only restore requires every native resource list")
		}
		items, err := meta.ExtractList(list)
		if err != nil {
			return fmt.Errorf("cannot inspect archived resource list")
		}
		for _, item := range items {
			metadata, err := meta.Accessor(item)
			if err != nil || metadata.GetName() == "" || metadata.GetDeletionTimestamp() != nil {
				return fmt.Errorf("archived resource identity is unresolved or terminating")
			}
			// Native backups may include claims from any tenant namespace,
			// including pending claims. Other namespaced resources belong to
			// the source Longhorn installation.
			if metadata.GetNamespace() != "" && list != resources.persistentVolumeClaimList && metadata.GetNamespace() != namespace {
				return fmt.Errorf("archived resource belongs to a different installation")
			}
		}
	}
	settings := map[string]string{}
	for _, setting := range resources.settingList.Items {
		if _, duplicate := settings[setting.Name]; duplicate {
			return fmt.Errorf("duplicate archived Setting")
		}
		settings[setting.Name] = setting.Value
	}
	if settings[string(types.SettingNameV1DataEngine)] != "false" || settings[string(types.SettingNameV2DataEngine)] != "true" {
		return fmt.Errorf("archived Settings do not explicitly select v2-only mode")
	}
	for _, volume := range resources.volumeList.Items {
		if volume.Spec.DataEngine != longhorn.DataEngineTypeV2 || volume.Spec.BackingImage != "" {
			return fmt.Errorf("archived Volume is incompatible with v2-only restore")
		}
	}
	if len(resources.backingImageList.Items) != 0 {
		return fmt.Errorf("backing images are not supported by the v2-only restore profile")
	}
	for _, class := range resources.storageClassList.Items {
		if err := validateV2OnlyRestoreStorageClass(class); err != nil {
			return err
		}
	}
	for _, volume := range resources.persistentVolumeList.Items {
		if volume.Spec.CSI == nil || volume.Spec.CSI.Driver != types.LonghornDriverName || volume.Spec.CSI.VolumeHandle == "" {
			return fmt.Errorf("archived PersistentVolume lacks a native Longhorn CSI identity")
		}
		if engine, present := volume.Spec.CSI.VolumeAttributes["dataEngine"]; present && engine != string(longhorn.DataEngineTypeV2) {
			return fmt.Errorf("archived PersistentVolume has contradictory engine intent")
		}
	}
	defaultSettingsFound := false
	for _, configMap := range resources.configMapList.Items {
		switch configMap.Name {
		case types.DefaultDefaultSettingConfigMapName:
			if defaultSettingsFound {
				return fmt.Errorf("duplicate archived default Settings ConfigMap")
			}
			defaultSettingsFound = true
			raw, present := configMap.Data[types.DefaultSettingYAMLFileName]
			if !present {
				return fmt.Errorf("archived default Settings document is missing")
			}
			values := map[string]string{}
			if err := decodeV2OnlyRestoreDocument(raw, &values); err != nil {
				return err
			}
			for _, name := range []string{string(types.SettingNameV1DataEngine), string(types.SettingNameV2DataEngine)} {
				if value, present := values[name]; present && value != settings[name] {
					return fmt.Errorf("archived default Settings contradict v2-only mode")
				}
			}
		case types.DefaultStorageClassConfigMapName:
			raw, present := configMap.Data["storageclass.yaml"]
			if !present {
				return fmt.Errorf("archived default StorageClass document is missing")
			}
			// Decode through Kubernetes' strict JSON representation, rather
			// than yaml.v3's field-name rules for Go API structs.
			class, err := decodeV2OnlyRestoreStorageClass(raw)
			if err != nil {
				return err
			}
			if class.Provisioner != types.LonghornDriverName {
				return fmt.Errorf("archived default StorageClass has an unexpected provisioner")
			}
			if err := validateV2OnlyRestoreStorageClass(*class); err != nil {
				return err
			}
		}
	}
	if !defaultSettingsFound {
		return fmt.Errorf("archived default Settings ConfigMap is missing")
	}
	managerFound, globalFound := false, false
	for _, daemonSet := range resources.daemonSetList.Items {
		if err := validateV2OnlyRestorePod(daemonSet.Spec.Template.Spec, "DaemonSet", daemonSet.Name, managerImage); err != nil {
			return err
		}
		if daemonSet.Name == types.LonghornManagerDaemonSetName {
			if managerFound {
				return fmt.Errorf("duplicate archived manager DaemonSet")
			}
			managerFound = true
			if err := validateV2OnlyRestoreManagerContainer(daemonSet.Spec.Template.Spec, "longhorn-manager", "daemon", managerImage); err != nil {
				return err
			}
		}
	}
	for _, deployment := range resources.deploymentList.Items {
		if err := validateV2OnlyRestorePod(deployment.Spec.Template.Spec, "Deployment", deployment.Name, managerImage); err != nil {
			return err
		}
		if deployment.Name == types.LonghornGlobalManagerName {
			if globalFound {
				return fmt.Errorf("duplicate archived global manager Deployment")
			}
			globalFound = true
			if err := validateV2OnlyRestoreManagerContainer(deployment.Spec.Template.Spec, "longhorn-global-manager", "global", managerImage); err != nil {
				return err
			}
		}
	}
	if !managerFound || !globalFound {
		return fmt.Errorf("archived manager workloads are incomplete")
	}
	return nil
}

func decodeV2OnlyRestoreDocument(raw string, target interface{}) error {
	decoder := yaml.NewDecoder(strings.NewReader(raw))
	if err := decoder.Decode(target); err != nil && err != io.EOF {
		return fmt.Errorf("invalid archived configuration document")
	}
	var extra interface{}
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("multiple or invalid archived configuration documents")
	}
	return nil
}

func validateV2OnlyRestoreStorageClass(class storagev1.StorageClass) error {
	if class.Provisioner == types.LonghornDriverName && class.Parameters["dataEngine"] != string(longhorn.DataEngineTypeV2) {
		return fmt.Errorf("archived Longhorn StorageClass does not explicitly select v2")
	}
	return nil
}

func decodeV2OnlyRestoreStorageClass(raw string) (*storagev1.StorageClass, error) {
	var document map[string]interface{}
	if err := decodeV2OnlyRestoreDocument(raw, &document); err != nil {
		return nil, err
	}
	var class storagev1.StorageClass
	if err := sigsyaml.UnmarshalStrict([]byte(raw), &class); err != nil || class.APIVersion != storagev1.SchemeGroupVersion.String() || class.Kind != "StorageClass" || class.Name != types.DefaultStorageClassName || class.Namespace != "" {
		return nil, fmt.Errorf("invalid archived StorageClass document")
	}
	return &class, nil
}

func validateV2OnlyRestoreManagerContainer(pod corev1.PodSpec, name, command, image string) error {
	found := false
	for _, container := range pod.Containers {
		if container.Name == name {
			if found {
				return fmt.Errorf("duplicate archived manager container")
			}
			found = true
			if err := validateV2OnlyManagerLaunch(container, command, image); err != nil {
				return err
			}
		}
	}
	if !found {
		return fmt.Errorf("archived manager container is missing")
	}
	return nil
}
