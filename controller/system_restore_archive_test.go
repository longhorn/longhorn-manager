package controller

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeCompleteRestoreArchive(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, manifest := range systemRestoreArchiveLists(&extractedResources{}) {
		directory := filepath.Join(root, manifest.directory)
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
		document := fmt.Sprintf("apiVersion: %s\nkind: %s\nmetadata: {}\nitems: []\n", manifest.gvk.GroupVersion(), manifest.gvk.Kind)
		if err := os.WriteFile(filepath.Join(directory, manifest.filename), []byte(document), 0600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

func TestV2OnlyRestoreArchiveLoadsEveryNativeListIncludingEmpty(t *testing.T) {
	root := writeCompleteRestoreArchive(t)
	bindings, err := os.ReadFile(filepath.Join(root, "kubernetes", "clusterroles.yaml"))
	if err != nil || !strings.Contains(string(bindings), "kind: ClusterRoleBindingList") {
		t.Fatal("fixture lost the native cluster RBAC filename mapping")
	}
	if _, err := os.Stat(filepath.Join(root, "longhorn", "setting.yaml")); err != nil {
		t.Fatal("fixture lost the native singular Setting filename")
	}
	resources, err := loadV2OnlySystemRestoreArchive(root)
	if err != nil {
		t.Fatal(err)
	}
	value := reflect.ValueOf(resources)
	if value.NumField() != 19 {
		t.Fatal("native restore list inventory changed")
	}
	for index := range value.NumField() {
		if value.Field(index).IsNil() {
			t.Fatalf("native list %d was treated as absent instead of empty", index)
		}
	}
}

func TestV2OnlyRestoreArchiveRejectsIncompleteOrAmbiguousPayload(t *testing.T) {
	for _, test := range []string{"missing", "extra", "symlink", "wrong-kind", "unknown-field", "duplicate-key", "multiple-documents", "missing-items", "continuation", "truncated", "duplicate-items"} {
		t.Run(test, func(t *testing.T) {
			root := writeCompleteRestoreArchive(t)
			path := filepath.Join(root, "longhorn", "setting.yaml")
			document, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			text := string(document)
			switch test {
			case "missing":
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
			case "extra":
				if err := os.WriteFile(filepath.Join(root, "longhorn", "extra.yaml"), document, 0600); err != nil {
					t.Fatal(err)
				}
			case "symlink":
				target := filepath.Join(t.TempDir(), "payload")
				if err := os.WriteFile(target, document, 0600); err != nil {
					t.Fatal(err)
				}
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				if err := os.Symlink(target, path); err != nil {
					t.Fatal(err)
				}
			case "wrong-kind":
				text = strings.Replace(text, "kind: SettingList", "kind: VolumeList", 1)
			case "unknown-field":
				text += "unrecognized: sentinel-secret\n"
			case "duplicate-key":
				text += "kind: VolumeList\n"
			case "multiple-documents":
				text += "---\napiVersion: v1\nkind: List\nitems: []\n"
			case "missing-items":
				text = strings.Replace(text, "items: []\n", "", 1)
			case "continuation":
				text = strings.Replace(text, "metadata: {}", "metadata:\n  continue: more", 1)
			case "truncated":
				text = text[:len(text)/2]
			case "duplicate-items":
				text = strings.Replace(text, "items: []", "items:\n- metadata: {name: duplicate, namespace: longhorn-system}\n  value: 'false'\n- metadata: {name: duplicate, namespace: longhorn-system}\n  value: 'true'", 1)
			}
			if test != "missing" && test != "extra" && test != "symlink" {
				if err := os.WriteFile(path, []byte(text), 0600); err != nil {
					t.Fatal(err)
				}
			}
			resources, err := loadV2OnlySystemRestoreArchive(root)
			if err == nil || strings.Contains(err.Error(), "sentinel-secret") {
				t.Fatal("partial archive cleared validation or leaked raw decoder diagnostics")
			}
			value := reflect.ValueOf(resources)
			for index := range value.NumField() {
				if !value.Field(index).IsNil() {
					t.Fatal("partial archive published a cache")
				}
			}
		})
	}
}

func TestV2OnlyRestoreArchiveBindsRootAndResourceScope(t *testing.T) {
	for _, test := range []string{"root-link", "cluster-namespace", "missing-namespace"} {
		t.Run(test, func(t *testing.T) {
			root := writeCompleteRestoreArchive(t)
			switch test {
			case "root-link":
				alias := filepath.Join(t.TempDir(), "linked")
				if err := os.Symlink(root, alias); err != nil {
					t.Fatal(err)
				}
				root = alias
			case "cluster-namespace":
				document := "apiVersion: storage.k8s.io/v1\nkind: StorageClassList\nitems:\n- metadata: {name: duplicate, namespace: first}\n- metadata: {name: duplicate, namespace: second}\n"
				if err := os.WriteFile(filepath.Join(root, "kubernetes", "storageclasses.yaml"), []byte(document), 0600); err != nil {
					t.Fatal(err)
				}
			case "missing-namespace":
				document := "apiVersion: longhorn.io/v1beta2\nkind: SettingList\nitems:\n- metadata: {name: orphan}\n  value: 'false'\n"
				if err := os.WriteFile(filepath.Join(root, "longhorn", "setting.yaml"), []byte(document), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := loadV2OnlySystemRestoreArchive(root); err == nil {
				t.Fatal("archive root or resource scope was not bound")
			}
		})
	}
}

func TestV2OnlyRestoreArchivePreservesNativeEmptyAndTenantClaims(t *testing.T) {
	root := writeCompleteRestoreArchive(t)
	settings := "apiVersion: longhorn.io/v1beta2\nkind: SettingList\nitems: null\n"
	if err := os.WriteFile(filepath.Join(root, "longhorn", "setting.yaml"), []byte(settings), 0600); err != nil {
		t.Fatal(err)
	}
	claims := "apiVersion: v1\nkind: PersistentVolumeClaimList\nitems:\n- metadata: {name: pending, namespace: tenant}\n  spec: {}\n"
	if err := os.WriteFile(filepath.Join(root, "kubernetes", "persistentvolumeclaims.yaml"), []byte(claims), 0600); err != nil {
		t.Fatal(err)
	}
	resources, err := loadV2OnlySystemRestoreArchive(root)
	if err != nil || resources.persistentVolumeClaimList.Items[0].Namespace != "tenant" {
		t.Fatalf("native empty lists or unbound tenant claims were rejected: %v", err)
	}
}

func TestV2OnlyRestoreArchiveRejectsResourceBudgetsAndIncompleteCounts(t *testing.T) {
	for _, test := range []string{"large-file", "too-many-items", "remaining-count"} {
		t.Run(test, func(t *testing.T) {
			root := writeCompleteRestoreArchive(t)
			path := filepath.Join(root, "longhorn", "setting.yaml")
			switch test {
			case "large-file":
				if err := os.Truncate(path, (16<<20)+1); err != nil {
					t.Fatal(err)
				}
			case "too-many-items":
				var document strings.Builder
				document.WriteString("apiVersion: longhorn.io/v1beta2\nkind: SettingList\nitems:\n")
				for index := range 10001 {
					fmt.Fprintf(&document, "- metadata: {name: setting-%d, namespace: longhorn-system}\n  value: 'false'\n", index)
				}
				if err := os.WriteFile(path, []byte(document.String()), 0600); err != nil {
					t.Fatal(err)
				}
			case "remaining-count":
				document := "apiVersion: longhorn.io/v1beta2\nkind: SettingList\nmetadata: {remainingItemCount: 1}\nitems: []\n"
				if err := os.WriteFile(path, []byte(document), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := loadV2OnlySystemRestoreArchive(root); err == nil {
				t.Fatal("unbounded or partial archive cleared validation")
			}
		})
	}
}
