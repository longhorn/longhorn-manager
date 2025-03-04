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

// fakeOrphans implements OrphanInterface
type fakeOrphans struct {
	*gentype.FakeClientWithListAndApply[*v1beta2.Orphan, *v1beta2.OrphanList, *longhornv1beta2.OrphanApplyConfiguration]
	Fake *FakeLonghornV1beta2
}

func newFakeOrphans(fake *FakeLonghornV1beta2, namespace string) typedlonghornv1beta2.OrphanInterface {
	return &fakeOrphans{
		gentype.NewFakeClientWithListAndApply[*v1beta2.Orphan, *v1beta2.OrphanList, *longhornv1beta2.OrphanApplyConfiguration](
			fake.Fake,
			namespace,
			v1beta2.SchemeGroupVersion.WithResource("orphans"),
			v1beta2.SchemeGroupVersion.WithKind("Orphan"),
			func() *v1beta2.Orphan { return &v1beta2.Orphan{} },
			func() *v1beta2.OrphanList { return &v1beta2.OrphanList{} },
			func(dst, src *v1beta2.OrphanList) { dst.ListMeta = src.ListMeta },
			func(list *v1beta2.OrphanList) []*v1beta2.Orphan { return gentype.ToPointerSlice(list.Items) },
			func(list *v1beta2.OrphanList, items []*v1beta2.Orphan) { list.Items = gentype.FromPointerSlice(items) },
		),
		fake,
	}
}
