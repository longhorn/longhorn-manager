/*
Copyright 2018 The Kubernetes Authors.

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

package fake

import (
	v1alpha1 "github.com/rancher/longhorn-manager/k8s/pkg/client/clientset/versioned/typed/longhorn/v1alpha1"
	rest "k8s.io/client-go/rest"
	testing "k8s.io/client-go/testing"
)

type FakeLonghornV1alpha1 struct {
	*testing.Fake
}

func (c *FakeLonghornV1alpha1) Engines(namespace string) v1alpha1.EngineInterface {
	return &FakeEngines{c, namespace}
}

func (c *FakeLonghornV1alpha1) Replicas(namespace string) v1alpha1.ReplicaInterface {
	return &FakeReplicas{c, namespace}
}

func (c *FakeLonghornV1alpha1) Settings(namespace string) v1alpha1.SettingInterface {
	return &FakeSettings{c, namespace}
}

func (c *FakeLonghornV1alpha1) Volumes(namespace string) v1alpha1.VolumeInterface {
	return &FakeVolumes{c, namespace}
}

// RESTClient returns a RESTClient that is used to communicate
// with API server by this client implementation.
func (c *FakeLonghornV1alpha1) RESTClient() rest.Interface {
	var ret *rest.RESTClient
	return ret
}
