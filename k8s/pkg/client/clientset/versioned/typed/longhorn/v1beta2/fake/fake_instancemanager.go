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

// fakeInstanceManagers implements InstanceManagerInterface
type fakeInstanceManagers struct {
	*gentype.FakeClientWithListAndApply[*v1beta2.InstanceManager, *v1beta2.InstanceManagerList, *longhornv1beta2.InstanceManagerApplyConfiguration]
	Fake *FakeLonghornV1beta2
}

func newFakeInstanceManagers(fake *FakeLonghornV1beta2, namespace string) typedlonghornv1beta2.InstanceManagerInterface {
	return &fakeInstanceManagers{
		gentype.NewFakeClientWithListAndApply[*v1beta2.InstanceManager, *v1beta2.InstanceManagerList, *longhornv1beta2.InstanceManagerApplyConfiguration](
			fake.Fake,
			namespace,
			v1beta2.SchemeGroupVersion.WithResource("instancemanagers"),
			v1beta2.SchemeGroupVersion.WithKind("InstanceManager"),
			func() *v1beta2.InstanceManager { return &v1beta2.InstanceManager{} },
			func() *v1beta2.InstanceManagerList { return &v1beta2.InstanceManagerList{} },
			func(dst, src *v1beta2.InstanceManagerList) { dst.ListMeta = src.ListMeta },
			func(list *v1beta2.InstanceManagerList) []*v1beta2.InstanceManager {
				return gentype.ToPointerSlice(list.Items)
			},
			func(list *v1beta2.InstanceManagerList, items []*v1beta2.InstanceManager) {
				list.Items = gentype.FromPointerSlice(items)
			},
		),
		fake,
	}
}
