package types

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const StorageClassParameterVolumeAntiAffinity = "volumeAntiAffinity"

func ValidateVolumeAntiAffinity(value longhorn.VolumeAntiAffinity) error {
	switch value {
	case "", longhorn.VolumeAntiAffinityEnabled, longhorn.VolumeAntiAffinityDisabled:
		return nil
	}
	return fmt.Errorf("invalid volume anti-affinity %q, must be %q or %q", value, longhorn.VolumeAntiAffinityEnabled, longhorn.VolumeAntiAffinityDisabled)
}

// ValidateVolumeAntiAffinityAndReplicaCount rejects volume anti-affinity on
// volumes with more than one replica. Keeping every replica of related volumes
// apart would need many more nodes than the pods need.
func ValidateVolumeAntiAffinityAndReplicaCount(value longhorn.VolumeAntiAffinity, count int) error {
	if value == longhorn.VolumeAntiAffinityEnabled && count != 1 {
		return fmt.Errorf("replica count should be 1 with volume anti-affinity %v", longhorn.VolumeAntiAffinityEnabled)
	}
	return nil
}

func ValidateVolumeAntiAffinityFromPod(fromPod *longhorn.VolumeAntiAffinityFromPod) error {
	if fromPod == nil {
		return nil
	}
	for _, terms := range [][]longhorn.VolumeAntiAffinityTerm{fromPod.RequiredTerms, fromPod.PreferredTerms} {
		for _, term := range terms {
			if _, err := metav1.LabelSelectorAsSelector(term.LabelSelector); err != nil {
				return fmt.Errorf("invalid label selector in volume anti-affinity term: %w", err)
			}
		}
	}
	return nil
}

func IsVolumeAntiAffinityEnabled(v *longhorn.Volume) bool {
	return v.Spec.VolumeAntiAffinity == longhorn.VolumeAntiAffinityEnabled
}

// IsVolumeAntiAffinityPending reports whether replica scheduling waits for the
// terms to be inherited from the pod of the volume.
func IsVolumeAntiAffinityPending(v *longhorn.Volume) bool {
	return IsVolumeAntiAffinityEnabled(v) && v.Spec.VolumeAntiAffinityFromPod != nil &&
		v.Spec.VolumeAntiAffinityFromPod.PendingInheritance
}

// VolumeAntiAffinityTermKey returns the identity of a term. Two terms are the
// same when their namespaces and their normalized label selectors are equal.
// A matchLabels entry is normalized to a single-value In requirement, so both
// ways of writing the same requirement give the same term.
func VolumeAntiAffinityTermKey(term longhorn.VolumeAntiAffinityTerm) string {
	requirements := sets.New[string]()
	if selector := term.LabelSelector; selector != nil {
		for key, value := range selector.MatchLabels {
			requirements.Insert(requirementKey(key, metav1.LabelSelectorOpIn, []string{value}))
		}
		for _, expression := range selector.MatchExpressions {
			requirements.Insert(requirementKey(expression.Key, expression.Operator, expression.Values))
		}
	}
	return strings.Join(sets.List(sets.New(term.Namespaces...)), ",") + "|" + strings.Join(sets.List(requirements), ";")
}

// VolumeAntiAffinityFromPodEqual compares the terms by identity, so a
// round trip through the API that drops empty fields does not count as a
// change.
func VolumeAntiAffinityFromPodEqual(a, b *longhorn.VolumeAntiAffinityFromPod) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Pod == b.Pod && a.PendingInheritance == b.PendingInheritance &&
		volumeAntiAffinityTermsEqual(a.RequiredTerms, b.RequiredTerms) &&
		volumeAntiAffinityTermsEqual(a.PreferredTerms, b.PreferredTerms)
}

func volumeAntiAffinityTermsEqual(a, b []longhorn.VolumeAntiAffinityTerm) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if VolumeAntiAffinityTermKey(a[i]) != VolumeAntiAffinityTermKey(b[i]) {
			return false
		}
	}
	return true
}

func requirementKey(key string, operator metav1.LabelSelectorOperator, values []string) string {
	return key + " " + string(operator) + " " + strings.Join(sets.List(sets.New(values...)), ",")
}

// InheritVolumeAntiAffinityFromPod converts the node-level spreading rules of
// the pod into volume anti-affinity terms. Required pod anti-affinity becomes a
// required term, and preferred pod anti-affinity and topology spread
// constraints become preferred terms. It returns nil when no rule is
// inherited, together with the reason for each rule that is not inherited.
// Zone and region rules are not reported when the topology requirement of the
// volume already keeps the volume in the zone or region of the pod.
func InheritVolumeAntiAffinityFromPod(pod *corev1.Pod, topology []longhorn.VolumeTopologyTerm) (*longhorn.VolumeAntiAffinityFromPod, []string) {
	inheritor := &volumeAntiAffinityInheritor{
		pod:  pod,
		keys: sets.New[string](),
	}
	for _, term := range topology {
		if term.Zone != "" {
			inheritor.zoneHandled = true
		}
		if term.Zone != "" || term.Region != "" {
			inheritor.regionHandled = true
		}
	}

	if affinity := pod.Spec.Affinity; affinity != nil && affinity.PodAntiAffinity != nil {
		for i, term := range affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			inheritor.add(fmt.Sprintf("spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution[%d]", i),
				term.TopologyKey, term.LabelSelector, term.NamespaceSelector, term.Namespaces, term.MatchLabelKeys, term.MismatchLabelKeys, true)
		}
		for i, weighted := range affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			term := weighted.PodAffinityTerm
			inheritor.add(fmt.Sprintf("spec.affinity.podAntiAffinity.preferredDuringSchedulingIgnoredDuringExecution[%d]", i),
				term.TopologyKey, term.LabelSelector, term.NamespaceSelector, term.Namespaces, term.MatchLabelKeys, term.MismatchLabelKeys, false)
		}
	}
	for i, constraint := range pod.Spec.TopologySpreadConstraints {
		inheritor.add(fmt.Sprintf("spec.topologySpreadConstraints[%d]", i),
			constraint.TopologyKey, constraint.LabelSelector, nil, nil, constraint.MatchLabelKeys, nil, false)
	}

	if len(inheritor.required) == 0 && len(inheritor.preferred) == 0 {
		return nil, inheritor.notInherited
	}
	return &longhorn.VolumeAntiAffinityFromPod{
		Pod:            pod.Namespace + "/" + pod.Name,
		RequiredTerms:  inheritor.required,
		PreferredTerms: inheritor.preferred,
	}, inheritor.notInherited
}

type volumeAntiAffinityInheritor struct {
	pod           *corev1.Pod
	zoneHandled   bool
	regionHandled bool

	keys         sets.Set[string]
	required     []longhorn.VolumeAntiAffinityTerm
	preferred    []longhorn.VolumeAntiAffinityTerm
	notInherited []string
}

// add inherits one rule. Required rules are added before preferred ones, so a
// term declared with both strengths is kept as required only.
func (i *volumeAntiAffinityInheritor) add(source, topologyKey string, selector, namespaceSelector *metav1.LabelSelector,
	namespaces, matchLabelKeys, mismatchLabelKeys []string, required bool) {
	// A rule without a label selector selects no pod.
	if selector == nil {
		return
	}

	switch topologyKey {
	case corev1.LabelHostname:
	case KubernetesTopologyZoneLabelKey:
		if !i.zoneHandled {
			i.skip(source, "zone rules are not inherited; the volumeTopology parameter zonal keeps a volume in the zone of its pod")
		}
		return
	case KubernetesTopologyRegionLabelKey:
		if !i.regionHandled {
			i.skip(source, "region rules are not inherited; the volumeTopology parameter regional keeps a volume in the region of its pod")
		}
		return
	default:
		i.skip(source, fmt.Sprintf("topology key %v is not inherited, only %v is", topologyKey, corev1.LabelHostname))
		return
	}

	if namespaceSelector != nil {
		i.skip(source, "rules with a namespace selector are not inherited")
		return
	}

	if len(namespaces) == 0 {
		namespaces = []string{i.pod.Namespace}
	}
	term := longhorn.VolumeAntiAffinityTerm{
		Namespaces:    sets.List(sets.New(namespaces...)),
		LabelSelector: selector.DeepCopy(),
	}
	for _, key := range matchLabelKeys {
		if value, ok := i.pod.Labels[key]; ok {
			addRequirement(term.LabelSelector, key, metav1.LabelSelectorOpIn, value)
		}
	}
	for _, key := range mismatchLabelKeys {
		if value, ok := i.pod.Labels[key]; ok {
			addRequirement(term.LabelSelector, key, metav1.LabelSelectorOpNotIn, value)
		}
	}

	// A volume avoids the volumes that carry the same term, so only a rule that
	// spreads the pods declaring it can be inherited. A rule that keeps the pod
	// away from other pods would relate the volumes of the declaring pods to each
	// other instead.
	labelSelector, err := metav1.LabelSelectorAsSelector(term.LabelSelector)
	if err != nil {
		i.skip(source, fmt.Sprintf("invalid label selector: %v", err))
		return
	}
	if !sets.New(term.Namespaces...).Has(i.pod.Namespace) || !labelSelector.Matches(labels.Set(i.pod.Labels)) {
		i.skip(source, "the rule does not select the pod itself")
		return
	}

	key := VolumeAntiAffinityTermKey(term)
	if i.keys.Has(key) {
		return
	}
	i.keys.Insert(key)
	if required {
		i.required = append(i.required, term)
	} else {
		i.preferred = append(i.preferred, term)
	}
}

func (i *volumeAntiAffinityInheritor) skip(source, reason string) {
	i.notInherited = append(i.notInherited, source+": "+reason)
}

// addRequirement merges a matchLabelKeys or mismatchLabelKeys entry into the
// selector as kube-scheduler does. kube-apiserver may have merged it already
// when the pod was created, so an existing requirement is not added twice.
func addRequirement(selector *metav1.LabelSelector, key string, operator metav1.LabelSelectorOperator, value string) {
	requirement := metav1.LabelSelectorRequirement{Key: key, Operator: operator, Values: []string{value}}
	for _, existing := range selector.MatchExpressions {
		if requirementKey(existing.Key, existing.Operator, existing.Values) == requirementKey(key, operator, requirement.Values) {
			return
		}
	}
	selector.MatchExpressions = append(selector.MatchExpressions, requirement)
}
