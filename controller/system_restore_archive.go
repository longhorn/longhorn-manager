package controller

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	kubernetesscheme "k8s.io/client-go/kubernetes/scheme"

	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type systemRestoreArchiveList struct {
	directory, filename string
	gvk                 schema.GroupVersionKind
	namespaced          bool
	bind                func(runtime.Object) bool
}

func archiveList[T runtime.Object](directory, filename string, target *T, gvk schema.GroupVersionKind) systemRestoreArchiveList {
	return systemRestoreArchiveList{directory: directory, filename: filename, gvk: gvk, namespaced: true, bind: func(obj runtime.Object) bool {
		list, ok := obj.(T)
		if ok {
			*target = list
		}
		return ok
	}}
}

func clusterArchiveList[T runtime.Object](directory, filename string, target *T, gvk schema.GroupVersionKind) systemRestoreArchiveList {
	list := archiveList(directory, filename, target, gvk)
	list.namespaced = false
	return list
}

func systemRestoreArchiveLists(target *extractedResources) []systemRestoreArchiveList {
	kube, lh, extensions := types.SystemBackupSubDirKubernetes, types.SystemBackupSubDirLonghorn, types.SystemBackupSubDirAPIExtensions
	return []systemRestoreArchiveList{
		clusterArchiveList(extensions, "customresourcedefinitions.yaml", &target.customResourceDefinitionList, apiextensionsv1.SchemeGroupVersion.WithKind(types.APIExtensionsKindCustomResourceDefinitionList)),
		// Native producers use these two historical filenames in reverse order.
		clusterArchiveList(kube, "clusterroles.yaml", &target.clusterRoleBindingList, rbacv1.SchemeGroupVersion.WithKind(types.KubernetesKindClusterRoleBindingList)),
		clusterArchiveList(kube, "clusterrolebindings.yaml", &target.clusterRoleList, rbacv1.SchemeGroupVersion.WithKind(types.KubernetesKindClusterRoleList)),
		archiveList(kube, "roles.yaml", &target.roleList, rbacv1.SchemeGroupVersion.WithKind(types.KubernetesKindRoleList)),
		archiveList(kube, "rolebindings.yaml", &target.roleBindingList, rbacv1.SchemeGroupVersion.WithKind(types.KubernetesKindRoleBindingList)),
		archiveList(kube, "daemonsets.yaml", &target.daemonSetList, appsv1.SchemeGroupVersion.WithKind(types.KubernetesKindDaemonSetList)),
		archiveList(kube, "deployments.yaml", &target.deploymentList, appsv1.SchemeGroupVersion.WithKind(types.KubernetesKindDeploymentList)),
		archiveList(kube, "configmaps.yaml", &target.configMapList, corev1.SchemeGroupVersion.WithKind(types.KubernetesKindConfigMapList)),
		clusterArchiveList(kube, "persistentvolumes.yaml", &target.persistentVolumeList, corev1.SchemeGroupVersion.WithKind(types.KubernetesKindPersistentVolumeList)),
		archiveList(kube, "persistentvolumeclaims.yaml", &target.persistentVolumeClaimList, corev1.SchemeGroupVersion.WithKind(types.KubernetesKindPersistentVolumeClaimList)),
		archiveList(kube, "services.yaml", &target.serviceList, corev1.SchemeGroupVersion.WithKind(types.KubernetesKindServiceList)),
		archiveList(kube, "serviceaccounts.yaml", &target.serviceAccountList, corev1.SchemeGroupVersion.WithKind(types.KubernetesKindServiceAccountList)),
		clusterArchiveList(kube, "storageclasses.yaml", &target.storageClassList, storagev1.SchemeGroupVersion.WithKind(types.KubernetesKindStorageClassList)),
		archiveList(lh, "setting.yaml", &target.settingList, longhorn.SchemeGroupVersion.WithKind(types.LonghornKindSettingList)),
		archiveList(lh, "engineimages.yaml", &target.engineImageList, longhorn.SchemeGroupVersion.WithKind(types.LonghornKindEngineImageList)),
		archiveList(lh, "volumes.yaml", &target.volumeList, longhorn.SchemeGroupVersion.WithKind(types.LonghornKindVolumeList)),
		archiveList(lh, "recurringjobs.yaml", &target.recurringJobList, longhorn.SchemeGroupVersion.WithKind(types.LonghornKindRecurringJobList)),
		archiveList(lh, "backingimages.yaml", &target.backingImageList, longhorn.SchemeGroupVersion.WithKind(types.LonghornKindBackingImageList)),
		archiveList(lh, "backuptargets.yaml", &target.backupTargetList, longhorn.SchemeGroupVersion.WithKind(types.LonghornKindBackupTargetList)),
	}
}

// loadV2OnlySystemRestoreArchive is deliberately separate from the legacy
// permissive loader. No caller cache is published on a partial read.
func loadV2OnlySystemRestoreArchive(root string) (extractedResources, error) {
	rootInfo, err := os.Lstat(root)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return extractedResources{}, fmt.Errorf("missing or invalid archived resource root")
	}
	var pending extractedResources
	lists := systemRestoreArchiveLists(&pending)
	directories := map[string]map[string]bool{}
	for _, list := range lists {
		if directories[list.directory] == nil {
			directories[list.directory] = map[string]bool{}
		}
		directories[list.directory][list.filename] = true
	}
	for directory, expected := range directories {
		path := filepath.Join(root, directory)
		info, err := os.Lstat(path)
		if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return extractedResources{}, fmt.Errorf("missing or invalid archived resource directory")
		}
		entries, err := os.ReadDir(path)
		if err != nil || len(entries) != len(expected) {
			return extractedResources{}, fmt.Errorf("incomplete archived resource file inventory")
		}
		for _, entry := range entries {
			if !expected[entry.Name()] || !entry.Type().IsRegular() {
				return extractedResources{}, fmt.Errorf("unexpected archived resource file")
			}
		}
	}
	scheme := runtime.NewScheme()
	for _, register := range []func(*runtime.Scheme) error{kubernetesscheme.AddToScheme, apiextensionsv1.SchemeBuilder.AddToScheme, longhorn.SchemeBuilder.AddToScheme} {
		if err := register(scheme); err != nil {
			return extractedResources{}, fmt.Errorf("cannot initialize archived resource decoder")
		}
	}
	decoder := serializer.NewCodecFactory(scheme, serializer.EnableStrict).UniversalDeserializer()
	totalBytes, totalItems := 0, 0
	for _, list := range lists {
		contents, err := readSystemRestoreManifest(filepath.Join(root, list.directory, list.filename))
		if err != nil {
			return extractedResources{}, err
		}
		totalBytes += len(contents)
		if totalBytes > 64<<20 {
			return extractedResources{}, fmt.Errorf("archived resource byte budget exceeded")
		}
		reader := utilyaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(contents)))
		document, err := reader.Read()
		if err != nil || len(bytes.TrimSpace(document)) == 0 {
			return extractedResources{}, fmt.Errorf("invalid archived %s document", list.gvk.Kind)
		}
		trailing, err := reader.Read()
		if err != io.EOF || len(bytes.TrimSpace(trailing)) != 0 {
			return extractedResources{}, fmt.Errorf("multiple archived %s documents", list.gvk.Kind)
		}
		object, gvk, err := decoder.Decode(document, nil, nil)
		if err != nil || gvk == nil || *gvk != list.gvk || object == nil {
			return extractedResources{}, fmt.Errorf("invalid archived %s", list.gvk.Kind)
		}
		jsonDocument, err := utilyaml.ToJSON(document)
		if err != nil {
			return extractedResources{}, fmt.Errorf("invalid archived %s envelope", list.gvk.Kind)
		}
		var envelope map[string]json.RawMessage
		if err := json.Unmarshal(jsonDocument, &envelope); err != nil || envelope["items"] == nil {
			return extractedResources{}, fmt.Errorf("missing archived %s items", list.gvk.Kind)
		}
		metadata, err := meta.ListAccessor(object)
		if err != nil || metadata.GetContinue() != "" || (metadata.GetRemainingItemCount() != nil && *metadata.GetRemainingItemCount() != 0) {
			return extractedResources{}, fmt.Errorf("incomplete archived %s", list.gvk.Kind)
		}
		items, err := meta.ExtractList(object)
		if err != nil || len(items) > 10000 {
			return extractedResources{}, fmt.Errorf("invalid archived %s item inventory", list.gvk.Kind)
		}
		totalItems += len(items)
		if totalItems > 25000 {
			return extractedResources{}, fmt.Errorf("archived resource item budget exceeded")
		}
		seen := map[string]bool{}
		for _, item := range items {
			identity, err := meta.Accessor(item)
			if err != nil || identity.GetName() == "" {
				return extractedResources{}, fmt.Errorf("unidentified archived %s item", list.gvk.Kind)
			}
			if (identity.GetNamespace() != "") != list.namespaced {
				return extractedResources{}, fmt.Errorf("invalid archived %s item scope", list.gvk.Kind)
			}
			key := identity.GetNamespace() + "/" + identity.GetName()
			if seen[key] {
				return extractedResources{}, fmt.Errorf("duplicate archived %s item", list.gvk.Kind)
			}
			seen[key] = true
		}
		if !list.bind(object) {
			return extractedResources{}, fmt.Errorf("unexpected archived %s concrete type", list.gvk.Kind)
		}
	}
	return pending, nil
}

func readSystemRestoreManifest(path string) ([]byte, error) {
	const maximumBytes = 16 << 20
	before, err := os.Lstat(path)
	if err != nil || !before.Mode().IsRegular() || before.Size() > maximumBytes {
		return nil, fmt.Errorf("missing or invalid archived resource file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot open archived resource file")
	}
	opened, statErr := file.Stat()
	contents, readErr := io.ReadAll(io.LimitReader(file, maximumBytes+1))
	after, afterErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil || readErr != nil || afterErr != nil || closeErr != nil || len(contents) > maximumBytes ||
		!os.SameFile(before, opened) || !os.SameFile(opened, after) || opened.Size() != after.Size() || !opened.ModTime().Equal(after.ModTime()) || int64(len(contents)) != after.Size() {
		return nil, fmt.Errorf("archived resource file changed or could not be read completely")
	}
	return contents, nil
}
