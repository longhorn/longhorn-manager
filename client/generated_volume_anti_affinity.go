package client

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	VOLUME_ANTI_AFFINITY_TYPE = "volumeAntiAffinity"
)

type VolumeAntiAffinity struct {
	Resource `yaml:"-"`

	Labels map[string]string `json:"labels,omitempty" yaml:"labels,omitempty"`

	PendingInheritance bool `json:"pendingInheritance,omitempty" yaml:"pending_inheritance,omitempty"`

	Selectors []metav1.LabelSelector `json:"selectors,omitempty" yaml:"selectors,omitempty"`
}
