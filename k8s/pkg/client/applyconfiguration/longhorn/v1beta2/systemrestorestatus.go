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

package v1beta2

import (
	longhornv1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// SystemRestoreStatusApplyConfiguration represents a declarative configuration of the SystemRestoreStatus type for use
// with apply.
type SystemRestoreStatusApplyConfiguration struct {
	OwnerID    *string                             `json:"ownerID,omitempty"`
	State      *longhornv1beta2.SystemRestoreState `json:"state,omitempty"`
	SourceURL  *string                             `json:"sourceURL,omitempty"`
	Conditions []ConditionApplyConfiguration       `json:"conditions,omitempty"`
}

// SystemRestoreStatusApplyConfiguration constructs a declarative configuration of the SystemRestoreStatus type for use with
// apply.
func SystemRestoreStatus() *SystemRestoreStatusApplyConfiguration {
	return &SystemRestoreStatusApplyConfiguration{}
}

// WithOwnerID sets the OwnerID field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the OwnerID field is set to the value of the last call.
func (b *SystemRestoreStatusApplyConfiguration) WithOwnerID(value string) *SystemRestoreStatusApplyConfiguration {
	b.OwnerID = &value
	return b
}

// WithState sets the State field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the State field is set to the value of the last call.
func (b *SystemRestoreStatusApplyConfiguration) WithState(value longhornv1beta2.SystemRestoreState) *SystemRestoreStatusApplyConfiguration {
	b.State = &value
	return b
}

// WithSourceURL sets the SourceURL field in the declarative configuration to the given value
// and returns the receiver, so that objects can be built by chaining "With" function invocations.
// If called multiple times, the SourceURL field is set to the value of the last call.
func (b *SystemRestoreStatusApplyConfiguration) WithSourceURL(value string) *SystemRestoreStatusApplyConfiguration {
	b.SourceURL = &value
	return b
}

// WithConditions adds the given value to the Conditions field in the declarative configuration
// and returns the receiver, so that objects can be build by chaining "With" function invocations.
// If called multiple times, values provided by each call will be appended to the Conditions field.
func (b *SystemRestoreStatusApplyConfiguration) WithConditions(values ...*ConditionApplyConfiguration) *SystemRestoreStatusApplyConfiguration {
	for i := range values {
		if values[i] == nil {
			panic("nil value passed to WithConditions")
		}
		b.Conditions = append(b.Conditions, *values[i])
	}
	return b
}
