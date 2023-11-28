package util

// Collection of helper functions for converting constant values into pointers
// to memory containing a copy of that value. This is required in some places by
// the K8s API and having helper functions is cleaner than doing it with lambdas

import (
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
)

func Int32Ptr(i int32) *int32 {
	r := int32(i)
	return &r
}

func StringPtr(s string) *string {
	r := string(s)
	return &r
}

func CoreV1PersistentVolumeModePtr(mode corev1.PersistentVolumeMode) *corev1.PersistentVolumeMode {
	m := corev1.PersistentVolumeMode(mode)
	return &m
}

func NetworkingV1PathTypePtr(pathtype networkingv1.PathType) *networkingv1.PathType {
	p := networkingv1.PathType(pathtype)
	return &p
}
