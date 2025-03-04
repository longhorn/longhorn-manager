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
	v1beta1 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
	longhornv1beta1 "github.com/longhorn/longhorn-manager/k8s/pkg/client/applyconfiguration/longhorn/v1beta1"
	typedlonghornv1beta1 "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/typed/longhorn/v1beta1"
	gentype "k8s.io/client-go/gentype"
)

// fakeEngines implements EngineInterface
type fakeEngines struct {
	*gentype.FakeClientWithListAndApply[*v1beta1.Engine, *v1beta1.EngineList, *longhornv1beta1.EngineApplyConfiguration]
	Fake *FakeLonghornV1beta1
}

func newFakeEngines(fake *FakeLonghornV1beta1, namespace string) typedlonghornv1beta1.EngineInterface {
	return &fakeEngines{
		gentype.NewFakeClientWithListAndApply[*v1beta1.Engine, *v1beta1.EngineList, *longhornv1beta1.EngineApplyConfiguration](
			fake.Fake,
			namespace,
			v1beta1.SchemeGroupVersion.WithResource("engines"),
			v1beta1.SchemeGroupVersion.WithKind("Engine"),
			func() *v1beta1.Engine { return &v1beta1.Engine{} },
			func() *v1beta1.EngineList { return &v1beta1.EngineList{} },
			func(dst, src *v1beta1.EngineList) { dst.ListMeta = src.ListMeta },
			func(list *v1beta1.EngineList) []*v1beta1.Engine { return gentype.ToPointerSlice(list.Items) },
			func(list *v1beta1.EngineList, items []*v1beta1.Engine) { list.Items = gentype.FromPointerSlice(items) },
		),
		fake,
	}
}
