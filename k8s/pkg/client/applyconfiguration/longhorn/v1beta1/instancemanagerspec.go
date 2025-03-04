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

// Code generated by applyconfiguration-gen. DO NOT EDIT.

package v1beta1

import (
	longhornv1beta1 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta1"
)

// InstanceManagerSpecApplyConfiguration represents a declarative configuration of the InstanceManagerSpec type for use
// with apply.
type InstanceManagerSpecApplyConfiguration struct {
	Image       *string                              `json:"image,omitempty"`
	NodeID      *string                              `json:"nodeID,omitempty"`
	Type        *longhornv1beta1.InstanceManagerType `json:"type,omitempty"`
	EngineImage *string                              `json:"engineImage,omitempty"`
}

// InstanceManagerSpecApplyConfiguration constructs a declarative configuration of the InstanceManagerSpec type for use with
// apply.
func InstanceManagerSpec() *InstanceManagerSpecApplyConfiguration {
	return &InstanceManagerSpecApplyConfiguration{}
}

// WithImage sets the Image field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the Image field is set to the value of the last call.
func (b *InstanceManagerSpecApplyConfiguration) WithImage(value string) *InstanceManagerSpecApplyConfiguration {
	b.Image = &value
	return b
}

// WithNodeID sets the NodeID field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the NodeID field is set to the value of the last call.
func (b *InstanceManagerSpecApplyConfiguration) WithNodeID(value string) *InstanceManagerSpecApplyConfiguration {
	b.NodeID = &value
	return b
}

// WithType sets the Type field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the Type field is set to the value of the last call.
func (b *InstanceManagerSpecApplyConfiguration) WithType(value longhornv1beta1.InstanceManagerType) *InstanceManagerSpecApplyConfiguration {
	b.Type = &value
	return b
}

// WithEngineImage sets the EngineImage field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the EngineImage field is set to the value of the last call.
func (b *InstanceManagerSpecApplyConfiguration) WithEngineImage(value string) *InstanceManagerSpecApplyConfiguration {
	b.EngineImage = &value
	return b
}
