/*
Copyright The Longhorn Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by client-gen. DO NOT EDIT.

package fake

import (
	v1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	longhornv1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/client/applyconfiguration/longhorn/v1beta2"
	typedlonghornv1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/typed/longhorn/v1beta2"
	gentype "k8s.io/client-go/gentype"
)

// fakeSystemBackups implements SystemBackupInterface
type fakeSystemBackups struct {
	*gentype.FakeClientWithListAndApply[*v1beta2.SystemBackup, *v1beta2.SystemBackupList, *longhornv1beta2.SystemBackupApplyConfiguration]
	Fake *FakeLonghornV1beta2
}

func newFakeSystemBackups(fake *FakeLonghornV1beta2, namespace string) typedlonghornv1beta2.SystemBackupInterface {
	return &fakeSystemBackups{
		gentype.NewFakeClientWithListAndApply[*v1beta2.SystemBackup, *v1beta2.SystemBackupList, *longhornv1beta2.SystemBackupApplyConfiguration](
			fake.Fake,
			namespace,
			v1beta2.SchemeGroupVersion.WithResource("systembackups"),
			v1beta2.SchemeGroupVersion.WithKind("SystemBackup"),
			func() *v1beta2.SystemBackup { return &v1beta2.SystemBackup{} },
			func() *v1beta2.SystemBackupList { return &v1beta2.SystemBackupList{} },
			func(dst, src *v1beta2.SystemBackupList) { dst.ListMeta = src.ListMeta },
			func(list *v1beta2.SystemBackupList) []*v1beta2.SystemBackup {
				return gentype.ToPointerSlice(list.Items)
			},
			func(list *v1beta2.SystemBackupList, items []*v1beta2.SystemBackup) {
				list.Items = gentype.FromPointerSlice(items)
			},
		),
		fake,
	}
}
