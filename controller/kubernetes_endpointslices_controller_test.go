package controller

import (
	"io"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestIdentifyIPSubsetToDelete(t *testing.T) {
	c := &KubernetesEndpointSlicesController{}

	slice := &discoveryv1.EndpointSlice{
		Endpoints: []discoveryv1.Endpoint{
			{Addresses: []string{"10.0.0.1"}},
			{Addresses: []string{"10.0.0.2"}},
		},
	}

	desired := map[string]*corev1.Pod{
		"10.0.0.1": {ObjectMeta: metav1.ObjectMeta{Name: "pod1"}},
	}

	toDelete := c.identifyIPEndpointToDelete(slice, desired)

	assert.True(t, toDelete["10.0.0.2"])
	assert.False(t, toDelete["10.0.0.1"])
}

func TestIdentifyIPSubsetToAddOrUpdate(t *testing.T) {
	c := &KubernetesEndpointSlicesController{}

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "pod1", Namespace: "ns1", UID: "uid1"},
		Spec:       corev1.PodSpec{NodeName: "node1"},
	}

	existing := map[string]discoveryv1.Endpoint{
		"10.0.0.1": {
			Addresses: []string{"10.0.0.1"},
			TargetRef: &corev1.ObjectReference{Kind: "Pod", Name: "oldpod", Namespace: "ns1"},
		},
	}

	desired := map[string]*corev1.Pod{
		"10.0.0.1": pod,
		"10.0.0.2": pod,
	}

	add, update := c.identifyIPEndpointToAddOrUpdate(existing, desired, func(p *corev1.Pod, ip string) discoveryv1.EndpointSlice {
		return discoveryv1.EndpointSlice{
			Endpoints: []discoveryv1.Endpoint{
				{
					Addresses: []string{ip},
					TargetRef: &corev1.ObjectReference{
						Kind:      "Pod",
						Name:      p.Name,
						Namespace: p.Namespace,
						UID:       p.UID,
					},
				},
			},
		}
	})

	assert.Contains(t, add, "10.0.0.2")    // new one
	assert.Contains(t, update, "10.0.0.1") // changed TargetRef
}

func TestUpdateEndpointSlices(t *testing.T) {
	log := logrus.New()
	log.SetOutput(io.Discard)

	// Explicit vars to take address of
	nfs := "nfs"
	port := int32(2049)
	protocol := corev1.ProtocolTCP
	node1 := "vm1-virtualbox"
	node2 := "vm2-virtualbox"
	node3 := "vm3-virtualbox"
	trueVal := true
	falseVal := false

	// Create a fake EndpointSlice similar to a real one in the cluster
	slice := &discoveryv1.EndpointSlice{
		TypeMeta: metav1.TypeMeta{
			Kind:       "EndpointSlice",
			APIVersion: "discovery.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pvc-test-1234-xyz",
			Namespace: "longhorn-system",
			Labels: map[string]string{
				"kubernetes.io/service-name": "pvc-test-1234",
				"longhorn.io/managed-by":     "longhorn-manager",
				"longhorn.io/share-manager":  "pvc-test-1234",
			},
			OwnerReferences: []metav1.OwnerReference{
				{
					APIVersion:         "v1",
					Kind:               "Service",
					Name:               "pvc-test-1234",
					UID:                "11111111-2222-3333-4444-555555555555",
					Controller:         &trueVal,
					BlockOwnerDeletion: &trueVal,
				},
			},
		},
		AddressType: discoveryv1.AddressTypeIPv4,
		Ports:       []discoveryv1.EndpointPort{},
		Endpoints: []discoveryv1.Endpoint{
			{
				Addresses: []string{"10.0.0.1"},
				NodeName:  &node1,
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Name:      "share-manager-pvc-test-1234",
					Namespace: "longhorn-system",
					UID:       "aaaa-bbbb-cccc-dddd",
				},
				Conditions: discoveryv1.EndpointConditions{
					Ready:       &trueVal,
					Serving:     &trueVal,
					Terminating: &falseVal,
				},
			},
			{
				Addresses: []string{"10.0.0.2"},
				NodeName:  &node2,
				TargetRef: &corev1.ObjectReference{
					Kind:      "Pod",
					Name:      "share-manager-pvc-test-1234-alt",
					Namespace: "longhorn-system",
					UID:       "eeee-ffff-gggg-hhhh",
				},
				Conditions: discoveryv1.EndpointConditions{
					Ready:       &trueVal,
					Serving:     &trueVal,
					Terminating: &falseVal,
				},
			},
		},
	}

	// Fake diff results
	toDelete := map[string]bool{"10.0.0.1": true}
	toAdd := map[string]discoveryv1.EndpointSlice{
		"10.0.0.3": {
			Endpoints: []discoveryv1.Endpoint{
				{
					Addresses: []string{"10.0.0.3"},
					NodeName:  &node3,
					TargetRef: &corev1.ObjectReference{
						Kind:      "Pod",
						Name:      "share-manager-pvc-test-1234-new",
						Namespace: "longhorn-system",
						UID:       "iiii-jjjj-kkkk-llll",
					},
				},
			},
			Ports: []discoveryv1.EndpointPort{
				{
					Name:     &nfs,
					Port:     &port,
					Protocol: &protocol,
				},
			},
		},
	}
	toUpdate := map[string]discoveryv1.EndpointSlice{
		"10.0.0.2": {
			Endpoints: []discoveryv1.Endpoint{
				{
					Addresses: []string{"10.0.0.2"},
					NodeName:  &node2,
					TargetRef: &corev1.ObjectReference{
						Kind:      "Pod",
						Name:      "updated-pod",
						Namespace: "longhorn-system",
						UID:       "eeee-ffff-gggg-hhhh",
					},
				},
			},
			Ports: []discoveryv1.EndpointPort{
				{
					Name:     &nfs,
					Port:     &port,
					Protocol: &protocol,
				},
			},
		},
	}
	updateEndpointSlices(slice, toDelete, toAdd, toUpdate, log)

	byIP := map[string]string{}
	byPort := map[string]int32{}
	for _, e := range slice.Endpoints {
		if len(e.Addresses) > 0 {
			byIP[e.Addresses[0]] = e.TargetRef.Name
		}
	}
	for _, p := range slice.Ports {
		if p.Port != nil {
			byPort[*p.Name] = *p.Port
		}
	}

	assert.Equal(t, "updated-pod", byIP["10.0.0.2"])
	assert.Equal(t, "share-manager-pvc-test-1234-new", byIP["10.0.0.3"])
	assert.Equal(t, int32(2049), byPort["nfs"])
}

func TestCreateDesiredEndpointForShareManager(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "sm-pod", Namespace: "ns1", UID: "uid1"},
		Spec:       corev1.PodSpec{NodeName: "node1"},
	}

	slice := createDesiredEndpointForShareManager(pod, "10.0.0.5")

	require.Len(t, slice.Endpoints, 1)
	ep := slice.Endpoints[0]
	assert.Equal(t, []string{"10.0.0.5"}, ep.Addresses)
	assert.Equal(t, "sm-pod", ep.TargetRef.Name)
	assert.Equal(t, "node1", *ep.NodeName)

	require.Len(t, slice.Ports, 1)
	assert.Equal(t, int32(2049), *slice.Ports[0].Port)
	assert.Equal(t, corev1.ProtocolTCP, *slice.Ports[0].Protocol)
}
