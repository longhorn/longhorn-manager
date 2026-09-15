package types

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/labels"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func spreadTestPod(name string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", Labels: labels},
	}
}

func antiAffinityTerm(topologyKey string, selector *metav1.LabelSelector) corev1.PodAffinityTerm {
	return corev1.PodAffinityTerm{LabelSelector: selector, TopologyKey: topologyKey}
}

func withRequiredAntiAffinity(pod *corev1.Pod, terms ...corev1.PodAffinityTerm) *corev1.Pod {
	pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: terms,
	}}
	return pod
}

func expectedSelector(namespace, instance string, matchLabels map[string]string, extra ...metav1.LabelSelectorRequirement) metav1.LabelSelector {
	labels := map[string]string{PodSpreadLabelKeyNamespace: namespace}
	for k, v := range matchLabels {
		labels[k] = v
	}
	requirements := append([]metav1.LabelSelectorRequirement{}, extra...)
	requirements = append(requirements, metav1.LabelSelectorRequirement{
		Key: PodSpreadLabelKeyInstance, Operator: metav1.LabelSelectorOpNotIn, Values: []string{instance},
	})
	return metav1.LabelSelector{MatchLabels: labels, MatchExpressions: requirements}
}

func TestDeriveVolumeAntiAffinityFromPod(t *testing.T) {
	appSelector := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "kafka"}}
	podLabels := map[string]string{"app": "kafka", "pod-template-hash": "abc"}

	t.Run("required hostname anti-affinity projects labels and selector", func(t *testing.T) {
		pod := withRequiredAntiAffinity(spreadTestPod("kafka-0", podLabels), antiAffinityTerm(corev1.LabelHostname, appSelector))
		got := DeriveVolumeAntiAffinityFromPod(pod)
		want := &longhorn.VolumeAntiAffinity{
			Labels:    map[string]string{PodSpreadLabelKeyNamespace: "prod", PodSpreadLabelKeyInstance: "kafka-0", "app": "kafka"},
			Selectors: []metav1.LabelSelector{expectedSelector("prod", "kafka-0", map[string]string{"app": "kafka"})},
		}
		assertAntiAffinityEqual(t, got, want)
	})

	t.Run("preferred term and topology spread constraint are both sources", func(t *testing.T) {
		pod := spreadTestPod("kafka-0", podLabels)
		pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
			PreferredDuringSchedulingIgnoredDuringExecution: []corev1.WeightedPodAffinityTerm{{
				Weight: 100, PodAffinityTerm: antiAffinityTerm(corev1.LabelHostname, appSelector),
			}},
		}}
		pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
			MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.ScheduleAnyway, LabelSelector: appSelector,
		}}
		got := DeriveVolumeAntiAffinityFromPod(pod)
		// The same selector declared twice carries one meaning.
		if got == nil || len(got.Selectors) != 1 {
			t.Fatalf("expected 1 deduplicated selector, got %+v", got)
		}
	})

	t.Run("distinct selectors from both sources are all kept", func(t *testing.T) {
		pod := spreadTestPod("kafka-0", podLabels)
		pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{antiAffinityTerm(corev1.LabelHostname, appSelector)},
		}}
		pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
			MaxSkew: 3, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{{
				Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{"kafka"},
			}}},
		}}
		got := DeriveVolumeAntiAffinityFromPod(pod)
		// DoNotSchedule and maxSkew are not read: the constraint is inherited as a soft preference like any other.
		if got == nil || len(got.Selectors) != 2 {
			t.Fatalf("expected 2 selectors, got %+v", got)
		}
	})

	t.Run("topology spread constraint matchLabelKeys are folded into the selector", func(t *testing.T) {
		revisionLabels := map[string]string{"app": "kafka", "pod-template-hash": "abc123"}
		pod := spreadTestPod("kafka-0", revisionLabels)
		pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
			MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.ScheduleAnyway,
			LabelSelector: appSelector, MatchLabelKeys: []string{"pod-template-hash"},
		}}
		got := DeriveVolumeAntiAffinityFromPod(pod)
		if got == nil || len(got.Selectors) != 1 {
			t.Fatalf("expected 1 selector, got %+v", got)
		}
		if got.Labels["pod-template-hash"] != "abc123" {
			t.Fatalf("expected the matchLabelKeys key to be projected, got %+v", got.Labels)
		}
		selector, err := metav1.LabelSelectorAsSelector(&got.Selectors[0])
		if err != nil {
			t.Fatal(err)
		}
		sibling := map[string]string{PodSpreadLabelKeyNamespace: pod.Namespace, PodSpreadLabelKeyInstance: "kafka-1", "app": "kafka", "pod-template-hash": "abc123"}
		otherRevision := map[string]string{PodSpreadLabelKeyNamespace: pod.Namespace, PodSpreadLabelKeyInstance: "kafka-1", "app": "kafka", "pod-template-hash": "def456"}
		if !selector.Matches(labels.Set(sibling)) || selector.Matches(labels.Set(otherRevision)) {
			t.Fatalf("expected the selector to match only the same revision, got %v", selector)
		}
	})

	t.Run("zone-keyed declarations are not inherited", func(t *testing.T) {
		pod := withRequiredAntiAffinity(spreadTestPod("kafka-0", podLabels), antiAffinityTerm(corev1.LabelTopologyZone, appSelector))
		pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{
			MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone, WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: appSelector,
		}}
		if got := DeriveVolumeAntiAffinityFromPod(pod); got != nil {
			t.Fatalf("expected nil, got %+v", got)
		}
	})

	t.Run("an explicit namespaces list including the pod's own is projected as an In match", func(t *testing.T) {
		term := antiAffinityTerm(corev1.LabelHostname, appSelector)
		term.Namespaces = []string{"staging", "prod"}
		pod := withRequiredAntiAffinity(spreadTestPod("kafka-0", podLabels), term)
		got := DeriveVolumeAntiAffinityFromPod(pod)
		if got == nil || len(got.Selectors) != 1 {
			t.Fatalf("expected 1 selector, got %+v", got)
		}
		selector, err := metav1.LabelSelectorAsSelector(&got.Selectors[0])
		if err != nil {
			t.Fatal(err)
		}
		stagingSibling := map[string]string{PodSpreadLabelKeyNamespace: "staging", PodSpreadLabelKeyInstance: "kafka-1", "app": "kafka"}
		otherNamespace := map[string]string{PodSpreadLabelKeyNamespace: "dev", PodSpreadLabelKeyInstance: "kafka-1", "app": "kafka"}
		if !selector.Matches(labels.Set(stagingSibling)) || selector.Matches(labels.Set(otherNamespace)) {
			t.Fatalf("expected the selector to cover exactly the listed namespaces, got %v", selector)
		}
	})

	t.Run("terms that do not match the pod itself or leave its namespace are skipped", func(t *testing.T) {
		otherApp := &metav1.LabelSelector{MatchLabels: map[string]string{"app": "zookeeper"}}
		crossNamespace := antiAffinityTerm(corev1.LabelHostname, appSelector)
		crossNamespace.Namespaces = []string{"other"}
		namespaceSelector := antiAffinityTerm(corev1.LabelHostname, appSelector)
		namespaceSelector.NamespaceSelector = &metav1.LabelSelector{}
		pod := withRequiredAntiAffinity(spreadTestPod("kafka-0", podLabels),
			antiAffinityTerm(corev1.LabelHostname, otherApp), crossNamespace, namespaceSelector)
		if got := DeriveVolumeAntiAffinityFromPod(pod); got != nil {
			t.Fatalf("expected nil, got %+v", got)
		}
	})

	t.Run("explicit own namespace in the term is accepted", func(t *testing.T) {
		term := antiAffinityTerm(corev1.LabelHostname, appSelector)
		term.Namespaces = []string{"prod"}
		pod := withRequiredAntiAffinity(spreadTestPod("kafka-0", podLabels), term)
		if got := DeriveVolumeAntiAffinityFromPod(pod); got == nil || len(got.Selectors) != 1 {
			t.Fatalf("expected 1 selector, got %+v", got)
		}
	})

	t.Run("matchLabelKeys and mismatchLabelKeys fold into the selector and project their keys", func(t *testing.T) {
		term := antiAffinityTerm(corev1.LabelHostname, appSelector)
		term.MatchLabelKeys = []string{"pod-template-hash", "absent"}
		term.MismatchLabelKeys = []string{"tenant"}
		labels := map[string]string{"app": "kafka", "pod-template-hash": "abc", "tenant": "a"}
		pod := withRequiredAntiAffinity(spreadTestPod("kafka-0", labels), term)
		got := DeriveVolumeAntiAffinityFromPod(pod)
		want := &longhorn.VolumeAntiAffinity{
			Labels: map[string]string{
				PodSpreadLabelKeyNamespace: "prod", PodSpreadLabelKeyInstance: "kafka-0",
				"app": "kafka", "pod-template-hash": "abc", "tenant": "a",
			},
			Selectors: []metav1.LabelSelector{expectedSelector("prod", "kafka-0", map[string]string{"app": "kafka"},
				metav1.LabelSelectorRequirement{Key: "pod-template-hash", Operator: metav1.LabelSelectorOpIn, Values: []string{"abc"}},
				metav1.LabelSelectorRequirement{Key: "tenant", Operator: metav1.LabelSelectorOpNotIn, Values: []string{"a"}},
			)},
		}
		assertAntiAffinityEqual(t, got, want)
	})

	t.Run("selector keys missing from the pod labels are not projected", func(t *testing.T) {
		selector := &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{"kafka"}},
			{Key: "canary", Operator: metav1.LabelSelectorOpDoesNotExist},
		}}
		pod := withRequiredAntiAffinity(spreadTestPod("kafka-0", podLabels), antiAffinityTerm(corev1.LabelHostname, selector))
		got := DeriveVolumeAntiAffinityFromPod(pod)
		if _, ok := got.Labels["canary"]; ok {
			t.Fatalf("absent key must not be projected: %+v", got.Labels)
		}
	})

	t.Run("long pod names are shortened deterministically", func(t *testing.T) {
		name := strings.Repeat("a", 70)
		pod := withRequiredAntiAffinity(spreadTestPod(name, podLabels), antiAffinityTerm(corev1.LabelHostname, appSelector))
		got := DeriveVolumeAntiAffinityFromPod(pod)
		instance := got.Labels[PodSpreadLabelKeyInstance]
		if len(instance) != 63 || !strings.HasPrefix(instance, strings.Repeat("a", 54)+"-") {
			t.Fatalf("unexpected instance %q", instance)
		}
		if again := DeriveVolumeAntiAffinityFromPod(pod); again.Labels[PodSpreadLabelKeyInstance] != instance {
			t.Fatalf("instance is not deterministic")
		}
		if got.Selectors[0].MatchExpressions[0].Values[0] != instance {
			t.Fatalf("selector must exclude the shortened instance")
		}
	})

	t.Run("pod without spread declarations yields nil", func(t *testing.T) {
		if got := DeriveVolumeAntiAffinityFromPod(spreadTestPod("kafka-0", podLabels)); got != nil {
			t.Fatalf("expected nil, got %+v", got)
		}
	})
}

func TestValidateVolumeAntiAffinity(t *testing.T) {
	if err := ValidateVolumeAntiAffinity(nil); err != nil {
		t.Fatalf("nil must be valid: %v", err)
	}
	valid := &longhorn.VolumeAntiAffinity{
		Labels:    map[string]string{"app": "kafka"},
		Selectors: []metav1.LabelSelector{{MatchLabels: map[string]string{"app": "kafka"}}},
	}
	if err := ValidateVolumeAntiAffinity(valid); err != nil {
		t.Fatalf("expected valid: %v", err)
	}
	badSelector := &longhorn.VolumeAntiAffinity{Selectors: []metav1.LabelSelector{{
		MatchExpressions: []metav1.LabelSelectorRequirement{{Key: "app", Operator: metav1.LabelSelectorOpIn}},
	}}}
	if err := ValidateVolumeAntiAffinity(badSelector); err == nil {
		t.Fatalf("In without values must be rejected")
	}
	badLabel := &longhorn.VolumeAntiAffinity{Labels: map[string]string{"app": "not valid!"}}
	if err := ValidateVolumeAntiAffinity(badLabel); err == nil {
		t.Fatalf("invalid label value must be rejected")
	}
}

func assertAntiAffinityEqual(t *testing.T, got, want *longhorn.VolumeAntiAffinity) {
	t.Helper()
	if got == nil {
		t.Fatalf("expected %+v, got nil", want)
	}
	if len(got.Labels) != len(want.Labels) {
		t.Fatalf("labels: got %v, want %v", got.Labels, want.Labels)
	}
	for k, v := range want.Labels {
		if got.Labels[k] != v {
			t.Fatalf("labels: got %v, want %v", got.Labels, want.Labels)
		}
	}
	if len(got.Selectors) != len(want.Selectors) {
		t.Fatalf("selectors: got %d, want %d", len(got.Selectors), len(want.Selectors))
	}
	for i := range want.Selectors {
		gotSelector, err := metav1.LabelSelectorAsSelector(&got.Selectors[i])
		if err != nil {
			t.Fatalf("selector %d unparsable: %v", i, err)
		}
		wantSelector, _ := metav1.LabelSelectorAsSelector(&want.Selectors[i])
		if gotSelector.String() != wantSelector.String() {
			t.Fatalf("selector %d: got %v, want %v", i, gotSelector, wantSelector)
		}
	}
	if got.PendingInheritance {
		t.Fatalf("derived anti-affinity must not be pending")
	}
}
