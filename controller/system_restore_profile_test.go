package controller

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/record"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	extensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/longhorn/go-common-libs/multierr"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func TestV2OnlyRestoreBindsNativeManagerLaunch(t *testing.T) {
	const image = "longhornio/longhorn-manager:fixture"
	for _, test := range []struct {
		name, command string
		argv, args    []string
		valid         bool
	}{
		{"native-daemon", "daemon", []string{"longhorn-manager", "-d", "-j", "daemon", "--manager-image", image, "--data-engine-mode", "v2-only"}, nil, true},
		{"native-global", "global", []string{"longhorn-manager", "-d", "global", "--data-engine-mode=v2-only"}, nil, true},
		{"split-args", "daemon", []string{"longhorn-manager", "-d"}, []string{"daemon", "--manager-image", image, "--data-engine-mode", "v2-only", "--upgrade-version-check=false"}, true},
		{"missing-mode", "daemon", []string{"longhorn-manager", "daemon", "--manager-image", image}, nil, false},
		{"legacy-mode", "daemon", []string{"longhorn-manager", "daemon", "--manager-image", image, "--data-engine-mode", "v1-capable"}, nil, false},
		{"duplicate-mode", "daemon", []string{"longhorn-manager", "daemon", "--manager-image", image, "--data-engine-mode", "v1-capable", "--data-engine-mode", "v2-only"}, nil, false},
		{"mode-as-value", "daemon", []string{"longhorn-manager", "daemon", "--engine-image=--data-engine-mode", "v2-only", "--manager-image", image}, nil, false},
		{"flag-as-image-value", "daemon", []string{"longhorn-manager", "daemon", "--engine-image", "--data-engine-mode", "v2-only", "--manager-image", image}, nil, false},
		{"wrong-manager-image", "daemon", []string{"longhorn-manager", "daemon", "--manager-image", "different", "--data-engine-mode", "v2-only"}, nil, false},
		{"wrapper", "daemon", []string{"sh", "-c", "longhorn-manager daemon --data-engine-mode v2-only"}, nil, false},
		{"end-of-options", "daemon", []string{"longhorn-manager", "daemon", "--", "--manager-image", image, "--data-engine-mode", "v2-only"}, nil, false},
		{"unresolved-mode", "global", []string{"longhorn-manager", "global", "--data-engine-mode", "$(MODE)"}, nil, false},
		{"unknown-flag", "global", []string{"longhorn-manager", "global", "--data-engine-mode", "v2-only", "--allow-v1"}, nil, false},
		{"wrong-command", "global", []string{"longhorn-manager", "daemon", "--data-engine-mode", "v2-only"}, nil, false},
		{"duplicate-root-flag", "global", []string{"longhorn-manager", "-d", "--debug", "global", "--data-engine-mode", "v2-only"}, nil, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			container := corev1.Container{Image: image, Command: test.argv, Args: test.args}
			if test.valid && test.command == "daemon" {
				container.Args = append(container.Args, requiredDaemonFixtureFlags()...)
			}
			if err := validateV2OnlyManagerLaunch(container, test.command, image); (err == nil) != test.valid {
				t.Fatalf("launch validity=%t want=%t err=%v", err == nil, test.valid, err)
			}
		})
	}
	if err := validateV2OnlyManagerLaunch(corev1.Container{Image: "different", Command: []string{"longhorn-manager", "global", "--data-engine-mode", "v2-only"}}, "global", image); err == nil {
		t.Fatal("different archived manager image cleared validation")
	}
}

func requiredDaemonFixtureFlags() []string {
	return []string{"--engine-image", "engine:fixture", "--instance-manager-image", "instance-manager:fixture", "--share-manager-image", "share-manager:fixture", "--backing-image-manager-image", "backing-image-manager:fixture", "--support-bundle-manager-image", "support-bundle-manager:fixture", "--service-account", "longhorn-service-account"}
}

func compatibleV2RestoreProfile(t *testing.T) extractedResources {
	t.Helper()
	resources, err := loadV2OnlySystemRestoreArchive(writeCompleteRestoreArchive(t))
	if err != nil {
		t.Fatal(err)
	}
	const image = "longhornio/longhorn-manager:fixture"
	metadata := func(name string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Name: name, Namespace: "longhorn-system"}
	}
	resources.settingList.Items = []longhorn.Setting{
		{ObjectMeta: metadata(string(types.SettingNameV1DataEngine)), Value: "false"},
		{ObjectMeta: metadata(string(types.SettingNameV2DataEngine)), Value: "true"},
	}
	resources.configMapList.Items = []corev1.ConfigMap{
		{ObjectMeta: metadata(types.DefaultDefaultSettingConfigMapName), Data: map[string]string{types.DefaultSettingYAMLFileName: "v1-data-engine: false\nv2-data-engine: true\n"}},
		{ObjectMeta: metadata(types.DefaultStorageClassConfigMapName), Data: map[string]string{"storageclass.yaml": "apiVersion: storage.k8s.io/v1\nkind: StorageClass\nmetadata: {name: longhorn}\nprovisioner: driver.longhorn.io\nparameters: {dataEngine: v2}\n"}},
	}
	resources.daemonSetList.Items = []appsv1.DaemonSet{{ObjectMeta: metadata(types.LonghornManagerDaemonSetName), Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "longhorn-manager", Image: image, Command: []string{"longhorn-manager", "-d", "daemon", "--manager-image", image, "--data-engine-mode", "v2-only"}}}}}}}}
	resources.deploymentList.Items = []appsv1.Deployment{{ObjectMeta: metadata(types.LonghornGlobalManagerName), Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "longhorn-global-manager", Image: image, Command: []string{"longhorn-manager", "-d", "global", "--data-engine-mode", "v2-only"}}}}}}}}
	resources.daemonSetList.Items[0].Spec.Template.Spec.ServiceAccountName = "longhorn-service-account"
	resources.daemonSetList.Items[0].Spec.Template.Spec.Containers[0].Args = requiredDaemonFixtureFlags()
	resources.deploymentList.Items[0].Spec.Template.Spec.Containers[0].Env = []corev1.EnvVar{
		{Name: "POD_NAME", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.name"}}},
		{Name: "POD_NAMESPACE", ValueFrom: &corev1.EnvVarSource{FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.namespace"}}},
	}
	resources.volumeList.Items = []longhorn.Volume{{ObjectMeta: metadata("restored-volume"), Spec: longhorn.VolumeSpec{DataEngine: longhorn.DataEngineTypeV2}}}
	resources.storageClassList.Items = []storagev1.StorageClass{{ObjectMeta: metav1.ObjectMeta{Name: "longhorn-v2"}, Provisioner: types.LonghornDriverName, Parameters: map[string]string{"dataEngine": "v2"}}}
	resources.customResourceDefinitionList.Items = []apiextensionsv1.CustomResourceDefinition{{ObjectMeta: metav1.ObjectMeta{Name: "volumes.longhorn.io"}, Spec: apiextensionsv1.CustomResourceDefinitionSpec{Group: longhorn.SchemeGroupVersion.Group}}}
	return resources
}

func TestV2OnlyRestoreProfileRejectsLegacyAndUnresolvedResources(t *testing.T) {
	const image = "longhornio/longhorn-manager:fixture"
	resources := compatibleV2RestoreProfile(t)
	if err := validateV2OnlySystemRestoreProfile(&resources, "longhorn-system", image); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*extractedResources)
	}{
		{"v1-enabled", func(r *extractedResources) { r.settingList.Items[0].Value = "true" }},
		{"missing-v2", func(r *extractedResources) { r.settingList.Items = r.settingList.Items[:1] }},
		{"foreign-setting", func(r *extractedResources) { r.settingList.Items[0].Namespace = "other" }},
		{"legacy-volume", func(r *extractedResources) { r.volumeList.Items[0].Spec.DataEngine = longhorn.DataEngineTypeV1 }},
		{"empty-volume-engine", func(r *extractedResources) { r.volumeList.Items[0].Spec.DataEngine = "" }},
		{"backing-image-dependency", func(r *extractedResources) { r.volumeList.Items[0].Spec.BackingImage = "legacy-image" }},
		{"backing-image", func(r *extractedResources) { r.backingImageList.Items = []longhorn.BackingImage{{}} }},
		{"v1-class", func(r *extractedResources) { r.storageClassList.Items[0].Parameters["dataEngine"] = "v1" }},
		{"implicit-class", func(r *extractedResources) { delete(r.storageClassList.Items[0].Parameters, "dataEngine") }},
		{"non-csi-pv", func(r *extractedResources) {
			r.persistentVolumeList.Items = []corev1.PersistentVolume{{ObjectMeta: metav1.ObjectMeta{Name: "pv"}}}
		}},
		{"foreign-csi-pv", func(r *extractedResources) {
			r.persistentVolumeList.Items = []corev1.PersistentVolume{{ObjectMeta: metav1.ObjectMeta{Name: "pv"}, Spec: corev1.PersistentVolumeSpec{PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: "foreign", VolumeHandle: "volume"}}}}}
		}},
		{"conflicting-cm", func(r *extractedResources) {
			r.configMapList.Items[0].Data[types.DefaultSettingYAMLFileName] = "v1-data-engine: true"
		}},
		{"malformed-cm", func(r *extractedResources) {
			r.configMapList.Items[0].Data[types.DefaultSettingYAMLFileName] = "v1-data-engine: [sentinel-secret"
		}},
		{"duplicate-cm-key", func(r *extractedResources) {
			r.configMapList.Items[0].Data[types.DefaultSettingYAMLFileName] = "v1-data-engine: false\nv1-data-engine: true"
		}},
		{"embedded-v1-class", func(r *extractedResources) {
			r.configMapList.Items[1].Data["storageclass.yaml"] = "apiVersion: storage.k8s.io/v1\nkind: StorageClass\nmetadata: {name: longhorn}\nprovisioner: driver.longhorn.io\nparameters: {dataEngine: v1}\n"
		}},
		{"embedded-wrong-class-name", func(r *extractedResources) {
			r.configMapList.Items[1].Data["storageclass.yaml"] = strings.ReplaceAll(r.configMapList.Items[1].Data["storageclass.yaml"], "name: longhorn}", "name: longhorn-v2}")
		}},
		{"manager-mode", func(r *extractedResources) {
			r.daemonSetList.Items[0].Spec.Template.Spec.Containers[0].Command = []string{"longhorn-manager", "daemon", "--manager-image", image}
		}},
		{"manager-image", func(r *extractedResources) {
			r.daemonSetList.Items[0].Spec.Template.Spec.Containers[0].Image = "different"
		}},
		{"global-mode", func(r *extractedResources) {
			r.deploymentList.Items[0].Spec.Template.Spec.Containers[0].Command = []string{"longhorn-manager", "global", "--data-engine-mode", "v1-capable"}
		}},
		{"missing-global", func(r *extractedResources) { r.deploymentList.Items = nil }},
		{"missing-list", func(r *extractedResources) { r.roleBindingList = nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			candidate := compatibleV2RestoreProfile(t)
			test.mutate(&candidate)
			if err := validateV2OnlySystemRestoreProfile(&candidate, "longhorn-system", image); err == nil || strings.Contains(err.Error(), "sentinel-secret") {
				t.Fatal("unresolved archive cleared the profile or exposed raw configuration")
			}
		})
	}
}

func TestV2OnlyRestorePreparationRevalidatesArchiveAndLiveState(t *testing.T) {
	resources := compatibleV2RestoreProfile(t)
	root := writeCompleteRestoreArchive(t)
	for path, object := range map[string]runtime.Object{
		"longhorn/setting.yaml": resources.settingList, "longhorn/volumes.yaml": resources.volumeList,
		"kubernetes/configmaps.yaml": resources.configMapList, "kubernetes/daemonsets.yaml": resources.daemonSetList,
		"kubernetes/deployments.yaml": resources.deploymentList, "kubernetes/storageclasses.yaml": resources.storageClassList,
		"apiextensions/customresourcedefinitions.yaml": resources.customResourceDefinitionList,
	} {
		document, err := json.Marshal(object)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, path), document, 0600); err != nil {
			t.Fatal(err)
		}
	}
	parent := t.TempDir()
	if err := os.Mkdir(filepath.Join(parent, "backup"), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(root, filepath.Join(parent, "backup", types.SystemBackupSubDirYaml)); err != nil {
		t.Fatal(err)
	}
	settingObjects := []runtime.Object{}
	for _, setting := range resources.settingList.Items {
		setting.UID, setting.ResourceVersion = "live-setting", "1"
		settingObjects = append(settingObjects, setting.DeepCopy())
	}
	lh := lhfake.NewClientset(settingObjects...)
	kube := kubefake.NewClientset()
	liveCRD := resources.customResourceDefinitionList.Items[0].DeepCopy()
	liveCRD.UID, liveCRD.ResourceVersion = "live-crd", "1"
	extensions := extensionsfake.NewClientset(liveCRD)
	ds := datastore.NewDataStoreForGlobal("longhorn-system", lh, kube, extensions, util.NewInformerFactories("longhorn-system", kube, lh, 0), types.DataEngineModeV2Only)
	c := &SystemRolloutController{ds: ds, baseController: &baseController{logger: logrus.New().WithField("test", "v2-only-restore")},
		systemRestore: &longhorn.SystemRestore{ObjectMeta: metav1.ObjectMeta{Name: "restore", Namespace: "longhorn-system"}, Spec: longhorn.SystemRestoreSpec{SystemBackup: "backup"}, Status: longhorn.SystemRestoreStatus{State: longhorn.SystemRestoreStateRestoring}},
		downloadPath:  filepath.Join(parent, "backup.zip"), archiveManagerImage: "longhornio/longhorn-manager:fixture"}
	if err := c.prepareV2OnlySystemRestore(); err != nil {
		t.Fatal(err)
	}
	if c.volumeList == nil || len(c.volumeList.Items) != 1 {
		t.Fatal("compatible preparation lost native resources")
	}
	for _, client := range []interface{ Actions() []k8stesting.Action }{lh, kube, extensions} {
		for _, action := range client.Actions() {
			if action.GetVerb() != "list" && action.GetVerb() != "get" {
				t.Fatal("restore preparation wrote archived resources")
			}
		}
	}
	c.systemRestore.Labels = types.GetSystemRestoreInProgressLabel()
	if err := ds.SystemRestoreInformer.GetStore().Add(c.systemRestore.DeepCopy()); err != nil {
		t.Fatal(err)
	}
	c.systemRestoreName, c.systemRestoredURL, c.systemRestoredAt = "restore", "s3://fixture", "receipt"
	c.eventRecorder, c.cacheErrors = record.NewFakeRecorder(100), multierr.NewMultiError()
	statusWritten := false
	lh.PrependReactor("update", "systemrestores", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updated := action.(k8stesting.UpdateAction).GetObject().(*longhorn.SystemRestore)
		if action.GetSubresource() != "status" || updated.Status.State != longhorn.SystemRestoreStateError {
			t.Fatal("failed schema binding did not fail the restore status")
		}
		statusWritten = true
		return true, updated, nil
	})
	extensions.PrependReactor("get", "customresourcedefinitions", func(k8stesting.Action) (bool, runtime.Object, error) {
		changed := liveCRD.DeepCopy()
		changed.Spec.Group = "changed.io"
		return true, changed, nil
	})
	if err := c.systemRollout(); err == nil || !statusWritten {
		t.Fatal("schema drift at the write boundary did not abort the real Restoring phase")
	}
	for _, client := range []interface{ Actions() []k8stesting.Action }{lh, kube, extensions} {
		for _, action := range client.Actions() {
			if action.GetVerb() == "list" || action.GetVerb() == "get" {
				continue
			}
			if action.GetVerb() != "update" || action.GetResource().Resource != "systemrestores" || action.GetSubresource() != "status" {
				t.Fatal("schema drift allowed a later archived resource write")
			}
		}
	}
	// A resumed Restoring phase must read the payload again instead of trusting
	// the cache populated by an earlier attempt.
	if err := os.Remove(filepath.Join(parent, "backup", types.SystemBackupSubDirYaml, "longhorn", "volumes.yaml")); err != nil {
		t.Fatal(err)
	}
	if err := c.prepareV2OnlySystemRestore(); err == nil || c.volumeList != nil {
		t.Fatal("resumed restore trusted a stale or partial archive cache")
	}
}

func TestV2OnlyRestorePreservesNativePVAndPendingTenantClaim(t *testing.T) {
	resources := compatibleV2RestoreProfile(t)
	resources.persistentVolumeList.Items = []corev1.PersistentVolume{{ObjectMeta: metav1.ObjectMeta{Name: "pv"}, Spec: corev1.PersistentVolumeSpec{StorageClassName: "static-class", PersistentVolumeSource: corev1.PersistentVolumeSource{CSI: &corev1.CSIPersistentVolumeSource{Driver: types.LonghornDriverName, VolumeHandle: "restored-volume"}}}}}
	resources.persistentVolumeClaimList.Items = []corev1.PersistentVolumeClaim{{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "tenant"}}}
	if err := validateV2OnlySystemRestoreProfile(&resources, "longhorn-system", "longhornio/longhorn-manager:fixture"); err != nil {
		t.Fatal(err)
	}
}

func TestV2OnlyRestoreRejectsAdditionalManagerExecution(t *testing.T) {
	const image = "longhornio/longhorn-manager:fixture"
	legacy := corev1.Container{Name: "extra", Image: image, Command: []string{"longhorn-manager", "daemon"}}
	for _, test := range []struct {
		name   string
		mutate func(*extractedResources)
	}{
		{"sidecar", func(r *extractedResources) {
			r.daemonSetList.Items[0].Spec.Template.Spec.Containers = append(r.daemonSetList.Items[0].Spec.Template.Spec.Containers, legacy)
		}},
		{"init-container", func(r *extractedResources) {
			r.daemonSetList.Items[0].Spec.Template.Spec.InitContainers = []corev1.Container{legacy}
		}},
		{"other-workload", func(r *extractedResources) {
			r.deploymentList.Items = append(r.deploymentList.Items, appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "other-manager", Namespace: "longhorn-system"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{legacy}}}}})
		}},
		{"lifecycle", func(r *extractedResources) {
			r.daemonSetList.Items[0].Spec.Template.Spec.Containers[0].Lifecycle = &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: legacy.Command}}}
		}},
		{"probe", func(r *extractedResources) {
			r.daemonSetList.Items[0].Spec.Template.Spec.Containers[0].StartupProbe = &corev1.Probe{ProbeHandler: corev1.ProbeHandler{Exec: &corev1.ExecAction{Command: legacy.Command}}}
		}},
		{"aliased-lifecycle", func(r *extractedResources) {
			r.daemonSetList.Items[0].Spec.Template.Spec.Containers = append(r.daemonSetList.Items[0].Spec.Template.Spec.Containers, corev1.Container{Name: "aliased", Image: "unrelated:old", Command: []string{"sleep", "infinity"}, Lifecycle: &corev1.Lifecycle{PostStart: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: legacy.Command}}}})
		}},
		{"ephemeral", func(r *extractedResources) {
			r.daemonSetList.Items[0].Spec.Template.Spec.EphemeralContainers = []corev1.EphemeralContainer{{EphemeralContainerCommon: corev1.EphemeralContainerCommon{Name: legacy.Name, Image: legacy.Image, Command: legacy.Command}}}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			resources := compatibleV2RestoreProfile(t)
			test.mutate(&resources)
			if err := validateV2OnlySystemRestoreProfile(&resources, "longhorn-system", image); err == nil {
				t.Fatal("additional manager execution cleared restore validation")
			}
		})
	}
}

func TestV2OnlyRestoreSupportsNativeCSIAndDriverLaunches(t *testing.T) {
	const image = "longhornio/longhorn-manager:fixture"
	resources := compatibleV2RestoreProfile(t)
	csi := corev1.Container{Name: types.CSIPluginName, Image: image,
		Args:      []string{"longhorn-manager", "-d", "csi", "--nodeid=$(NODE_ID)", "--endpoint=$(CSI_ENDPOINT)", "--drivername=driver.longhorn.io", "--manager-url=http://longhorn-backend:9500/v1"},
		Lifecycle: &corev1.Lifecycle{PreStop: &corev1.LifecycleHandler{Exec: &corev1.ExecAction{Command: []string{"/bin/sh", "-c", "rm -f /csi//*"}}}}}
	resources.daemonSetList.Items = append(resources.daemonSetList.Items, appsv1.DaemonSet{ObjectMeta: metav1.ObjectMeta{Name: types.CSIPluginName, Namespace: "longhorn-system"}, Spec: appsv1.DaemonSetSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{csi}}}}})
	driver := corev1.Container{Name: "longhorn-driver-deployer", Image: image, Command: []string{"longhorn-manager", "-d", "deploy-driver", "--manager-image", image, "--manager-url", "http://longhorn-backend:9500/v1"}}
	for _, suffix := range []string{"ATTACHER", "PROVISIONER", "RESIZER", "SNAPSHOTTER", "NODE_DRIVER_REGISTRAR", "LIVENESS_PROBE"} {
		driver.Env = append(driver.Env, corev1.EnvVar{Name: "CSI_" + suffix + "_IMAGE", Value: "csi:fixture"})
	}
	resources.deploymentList.Items = append(resources.deploymentList.Items, appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "longhorn-driver-deployer", Namespace: "longhorn-system"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{driver}, InitContainers: []corev1.Container{{Name: "wait-longhorn-manager", Image: image, Command: []string{"sh", "-c", archivedManagerWaitScript}}}}}}})
	if err := validateV2OnlySystemRestoreProfile(&resources, "longhorn-system", image); err != nil {
		t.Fatal(err)
	}
	resources.deploymentList.Items[1].Spec.Template.Spec.InitContainers[0].Command[2] += "; longhorn-manager daemon"
	if err := validateV2OnlySystemRestoreProfile(&resources, "longhorn-system", image); err == nil {
		t.Fatal("extended wait shell cleared native launch validation")
	}
}
