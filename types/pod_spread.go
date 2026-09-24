package types

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	metav1validation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	// Identity keys stamped on a volume's anti-affinity labels. The namespace
	// scopes matching to the pod's namespace (all volumes live in one
	// namespace); the instance excludes the volumes of the same pod from each
	// other.
	PodSpreadLabelKeyNamespace = "pod.longhorn.io/namespace"
	PodSpreadLabelKeyInstance  = "pod.longhorn.io/instance"
)

// DeriveVolumeAntiAffinityFromPod projects the pod's own node-level spread
// declarations onto a volume: every self-matching inter-pod anti-affinity term
// or topology spread constraint keyed on kubernetes.io/hostname becomes one
// selector, scoped to the pod's namespace and excluding the pod's own volumes.
// Returns nil when the pod declares no such spread.
// spreadTerm is one inherited declaration: the selector and the namespaces it
// looks at (empty means the pod's own namespace).
type spreadTerm struct {
	selector   *metav1.LabelSelector
	namespaces []string
}

func DeriveVolumeAntiAffinityFromPod(pod *corev1.Pod) *longhorn.VolumeAntiAffinity {
	var terms []spreadTerm
	if affinity := pod.Spec.Affinity; affinity != nil && affinity.PodAntiAffinity != nil {
		for i := range affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution {
			if term := selfSpreadTerm(pod, &affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution[i]); term != nil {
				terms = append(terms, *term)
			}
		}
		for i := range affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution {
			if term := selfSpreadTerm(pod, &affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution[i].PodAffinityTerm); term != nil {
				terms = append(terms, *term)
			}
		}
	}
	for i := range pod.Spec.TopologySpreadConstraints {
		constraint := &pod.Spec.TopologySpreadConstraints[i]
		affinityTerm := corev1.PodAffinityTerm{
			LabelSelector:  constraint.LabelSelector,
			TopologyKey:    constraint.TopologyKey,
			MatchLabelKeys: constraint.MatchLabelKeys,
		}
		if term := selfSpreadTerm(pod, &affinityTerm); term != nil {
			terms = append(terms, *term)
		}
	}
	if len(terms) == 0 {
		return nil
	}
	// A pod often declares the same selector twice (anti-affinity and a spread
	// constraint); one copy carries the same meaning.
	seen := map[string]bool{}
	unique := terms[:0]
	for _, term := range terms {
		key := metav1.FormatLabelSelector(term.selector) + "|" + strings.Join(term.namespaces, ",")
		if !seen[key] {
			seen[key] = true
			unique = append(unique, term)
		}
	}
	terms = unique

	instance := podSpreadInstance(pod.Name)
	antiAffinity := &longhorn.VolumeAntiAffinity{
		Labels: map[string]string{
			PodSpreadLabelKeyNamespace: pod.Namespace,
			PodSpreadLabelKeyInstance:  instance,
		},
	}
	for _, term := range terms {
		selector := term.selector
		for _, key := range selectorKeys(selector) {
			if value, ok := pod.Labels[key]; ok {
				antiAffinity.Labels[key] = value
			}
		}
		if len(term.namespaces) == 0 {
			if selector.MatchLabels == nil {
				selector.MatchLabels = map[string]string{}
			}
			selector.MatchLabels[PodSpreadLabelKeyNamespace] = pod.Namespace
		} else {
			selector.MatchExpressions = append(selector.MatchExpressions, metav1.LabelSelectorRequirement{
				Key:      PodSpreadLabelKeyNamespace,
				Operator: metav1.LabelSelectorOpIn,
				Values:   term.namespaces,
			})
		}
		selector.MatchExpressions = append(selector.MatchExpressions, metav1.LabelSelectorRequirement{
			Key:      PodSpreadLabelKeyInstance,
			Operator: metav1.LabelSelectorOpNotIn,
			Values:   []string{instance},
		})
		antiAffinity.Selectors = append(antiAffinity.Selectors, *selector)
	}
	return antiAffinity
}

// selfSpreadTerm returns a copy of the term's selector, with matchLabelKeys and
// mismatchLabelKeys folded in the way kube-scheduler does, when the term spreads
// the pod itself across nodes and includes its own namespace. An explicit
// namespaces list is kept; namespaceSelector cannot be expressed on volumes and
// makes the term ineligible.
func selfSpreadTerm(pod *corev1.Pod, term *corev1.PodAffinityTerm) *spreadTerm {
	if term.TopologyKey != corev1.LabelHostname || term.LabelSelector == nil || term.NamespaceSelector != nil {
		return nil
	}
	if len(term.Namespaces) > 0 && !slicesContains(term.Namespaces, pod.Namespace) {
		return nil
	}
	selector := term.LabelSelector.DeepCopy()
	for _, key := range term.MatchLabelKeys {
		if value, ok := pod.Labels[key]; ok {
			selector.MatchExpressions = append(selector.MatchExpressions, metav1.LabelSelectorRequirement{
				Key: key, Operator: metav1.LabelSelectorOpIn, Values: []string{value},
			})
		}
	}
	parsed, err := metav1.LabelSelectorAsSelector(selector)
	if err != nil || !parsed.Matches(labels.Set(pod.Labels)) {
		return nil
	}
	for _, key := range term.MismatchLabelKeys {
		if value, ok := pod.Labels[key]; ok {
			selector.MatchExpressions = append(selector.MatchExpressions, metav1.LabelSelectorRequirement{
				Key: key, Operator: metav1.LabelSelectorOpNotIn, Values: []string{value},
			})
		}
	}
	namespaces := append([]string(nil), term.Namespaces...)
	sort.Strings(namespaces)
	return &spreadTerm{selector: selector, namespaces: namespaces}
}

func selectorKeys(selector *metav1.LabelSelector) []string {
	keys := make([]string, 0, len(selector.MatchLabels)+len(selector.MatchExpressions))
	for key := range selector.MatchLabels {
		keys = append(keys, key)
	}
	for _, requirement := range selector.MatchExpressions {
		keys = append(keys, requirement.Key)
	}
	return keys
}

// podSpreadInstance fits the pod name into a label value, deterministically so
// that all volumes of one pod carry the same instance.
func podSpreadInstance(podName string) string {
	if len(podName) <= validation.LabelValueMaxLength {
		return podName
	}
	digest := sha256.Sum256([]byte(podName))
	suffix := hex.EncodeToString(digest[:])[:8]
	return fmt.Sprintf("%s-%s", podName[:validation.LabelValueMaxLength-len(suffix)-1], suffix)
}

func slicesContains(list []string, value string) bool {
	for _, item := range list {
		if item == value {
			return true
		}
	}
	return false
}

// ValidateVolumeAntiAffinity checks that the labels and selectors are well
// formed; an unparsable selector would otherwise fail every scheduling attempt.
func ValidateVolumeAntiAffinity(antiAffinity *longhorn.VolumeAntiAffinity) error {
	if antiAffinity == nil {
		return nil
	}
	if errs := metav1validation.ValidateLabels(antiAffinity.Labels, field.NewPath("labels")); len(errs) > 0 {
		return errs.ToAggregate()
	}
	for i := range antiAffinity.Selectors {
		if _, err := metav1.LabelSelectorAsSelector(&antiAffinity.Selectors[i]); err != nil {
			return errors.Wrapf(err, "invalid selectors[%d]", i)
		}
	}
	return nil
}
