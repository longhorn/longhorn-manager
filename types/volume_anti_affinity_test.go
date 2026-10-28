package types

import (
	"strings"

	. "gopkg.in/check.v1"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type VolumeAntiAffinitySuite struct{}

var _ = Suite(&VolumeAntiAffinitySuite{})

func newAntiAffinityPod(podLabels map[string]string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "kafka-0", Labels: podLabels},
		Spec:       corev1.PodSpec{Affinity: &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{}}},
	}
}

func hostnameTerm(selector *metav1.LabelSelector) corev1.PodAffinityTerm {
	return corev1.PodAffinityTerm{TopologyKey: corev1.LabelHostname, LabelSelector: selector}
}

func matchLabels(kv ...string) *metav1.LabelSelector {
	m := map[string]string{}
	for i := 0; i+1 < len(kv); i += 2 {
		m[kv[i]] = kv[i+1]
	}
	return &metav1.LabelSelector{MatchLabels: m}
}

func (s *VolumeAntiAffinitySuite) TestInheritStrength(c *C) {
	pod := newAntiAffinityPod(map[string]string{"app": "kafka", "tier": "data"})
	pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{
		hostnameTerm(matchLabels("app", "kafka")),
	}
	pod.Spec.Affinity.PodAntiAffinity.PreferredDuringSchedulingIgnoredDuringExecution = []corev1.WeightedPodAffinityTerm{
		{Weight: 10, PodAffinityTerm: hostnameTerm(matchLabels("tier", "data"))},
	}
	pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{
		// Same requirement as the required term, written as an expression.
		{MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.DoNotSchedule,
			LabelSelector: &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
				{Key: "app", Operator: metav1.LabelSelectorOpIn, Values: []string{"kafka"}},
			}}},
		{MaxSkew: 1, TopologyKey: corev1.LabelHostname, WhenUnsatisfiable: corev1.ScheduleAnyway,
			LabelSelector: matchLabels("app", "kafka", "tier", "data")},
	}

	result, notInherited := InheritVolumeAntiAffinityFromPod(pod, nil)
	c.Assert(notInherited, HasLen, 0)
	c.Assert(result, NotNil)
	c.Assert(result.Pod, Equals, "prod/kafka-0")
	c.Assert(result.PendingInheritance, Equals, false)
	c.Assert(result.RequiredTerms, DeepEquals, []longhorn.VolumeAntiAffinityTerm{
		{Namespaces: []string{"prod"}, LabelSelector: matchLabels("app", "kafka")},
	})
	c.Assert(result.PreferredTerms, DeepEquals, []longhorn.VolumeAntiAffinityTerm{
		{Namespaces: []string{"prod"}, LabelSelector: matchLabels("tier", "data")},
		{Namespaces: []string{"prod"}, LabelSelector: matchLabels("app", "kafka", "tier", "data")},
	})
}

func (s *VolumeAntiAffinitySuite) TestInheritConditions(c *C) {
	zonal := []longhorn.VolumeTopologyTerm{{Zone: "zone-a", Region: "region-1"}}
	regional := []longhorn.VolumeTopologyTerm{{Region: "region-1"}}

	testCases := map[string]struct {
		term         corev1.PodAffinityTerm
		topology     []longhorn.VolumeTopologyTerm
		inherited    bool
		notInherited string
	}{
		"other application": {
			term:         hostnameTerm(matchLabels("app", "zookeeper")),
			notInherited: "does not select the pod itself",
		},
		"no label selector": {
			term: corev1.PodAffinityTerm{TopologyKey: corev1.LabelHostname},
		},
		"zone without volume topology": {
			term:         corev1.PodAffinityTerm{TopologyKey: KubernetesTopologyZoneLabelKey, LabelSelector: matchLabels("app", "kafka")},
			notInherited: "zone rules are not inherited",
		},
		"zone with zonal volume topology": {
			term:     corev1.PodAffinityTerm{TopologyKey: KubernetesTopologyZoneLabelKey, LabelSelector: matchLabels("app", "kafka")},
			topology: zonal,
		},
		"zone with regional volume topology": {
			term:         corev1.PodAffinityTerm{TopologyKey: KubernetesTopologyZoneLabelKey, LabelSelector: matchLabels("app", "kafka")},
			topology:     regional,
			notInherited: "zone rules are not inherited",
		},
		"region with regional volume topology": {
			term:     corev1.PodAffinityTerm{TopologyKey: KubernetesTopologyRegionLabelKey, LabelSelector: matchLabels("app", "kafka")},
			topology: regional,
		},
		"region with zonal volume topology": {
			term:     corev1.PodAffinityTerm{TopologyKey: KubernetesTopologyRegionLabelKey, LabelSelector: matchLabels("app", "kafka")},
			topology: zonal,
		},
		"region without volume topology": {
			term:         corev1.PodAffinityTerm{TopologyKey: KubernetesTopologyRegionLabelKey, LabelSelector: matchLabels("app", "kafka")},
			notInherited: "region rules are not inherited",
		},
		"custom topology key": {
			term:         corev1.PodAffinityTerm{TopologyKey: "example.com/rack", LabelSelector: matchLabels("app", "kafka")},
			notInherited: "topology key example.com/rack is not inherited",
		},
		"namespace selector": {
			term: corev1.PodAffinityTerm{TopologyKey: corev1.LabelHostname, LabelSelector: matchLabels("app", "kafka"),
				NamespaceSelector: &metav1.LabelSelector{}},
			notInherited: "namespace selector",
		},
		"namespaces without its own": {
			term: corev1.PodAffinityTerm{TopologyKey: corev1.LabelHostname, LabelSelector: matchLabels("app", "kafka"),
				Namespaces: []string{"stg"}},
			notInherited: "does not select the pod itself",
		},
		"empty selector selects every pod": {
			term:      hostnameTerm(&metav1.LabelSelector{}),
			inherited: true,
		},
	}
	for name, tc := range testCases {
		pod := newAntiAffinityPod(map[string]string{"app": "kafka"})
		pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{tc.term}

		result, notInherited := InheritVolumeAntiAffinityFromPod(pod, tc.topology)
		c.Assert(result != nil, Equals, tc.inherited, Commentf("case %v", name))
		if tc.notInherited == "" {
			c.Assert(notInherited, HasLen, 0, Commentf("case %v: %v", name, notInherited))
			continue
		}
		c.Assert(notInherited, HasLen, 1, Commentf("case %v", name))
		c.Assert(strings.HasPrefix(notInherited[0], "spec.affinity.podAntiAffinity.requiredDuringSchedulingIgnoredDuringExecution[0]: "),
			Equals, true, Commentf("case %v: %v", name, notInherited[0]))
		c.Assert(strings.Contains(notInherited[0], tc.notInherited), Equals, true, Commentf("case %v: %v", name, notInherited[0]))
	}
}

func (s *VolumeAntiAffinitySuite) TestInheritNamespaces(c *C) {
	pod := newAntiAffinityPod(map[string]string{"app": "kafka"})
	pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{
		{TopologyKey: corev1.LabelHostname, LabelSelector: matchLabels("app", "kafka"), Namespaces: []string{"stg", "prod", "stg"}},
	}
	pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{
		{MaxSkew: 1, TopologyKey: corev1.LabelHostname, LabelSelector: matchLabels("app", "kafka")},
	}

	result, notInherited := InheritVolumeAntiAffinityFromPod(pod, nil)
	c.Assert(notInherited, HasLen, 0)
	c.Assert(result.RequiredTerms, DeepEquals, []longhorn.VolumeAntiAffinityTerm{
		{Namespaces: []string{"prod", "stg"}, LabelSelector: matchLabels("app", "kafka")},
	})
	// A topology spread constraint applies to the namespace of the pod only,
	// so it is a different term.
	c.Assert(result.PreferredTerms, DeepEquals, []longhorn.VolumeAntiAffinityTerm{
		{Namespaces: []string{"prod"}, LabelSelector: matchLabels("app", "kafka")},
	})
}

func (s *VolumeAntiAffinitySuite) TestInheritMergedLabelKeys(c *C) {
	pod := newAntiAffinityPod(map[string]string{"app": "kafka", "controller-revision-hash": "abc"})
	revision := metav1.LabelSelectorRequirement{Key: "controller-revision-hash", Operator: metav1.LabelSelectorOpIn, Values: []string{"abc"}}

	term := hostnameTerm(matchLabels("app", "kafka"))
	term.MatchLabelKeys = []string{"controller-revision-hash", "missing"}
	pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{term}
	result, notInherited := InheritVolumeAntiAffinityFromPod(pod, nil)
	c.Assert(notInherited, HasLen, 0)
	c.Assert(result.RequiredTerms[0].LabelSelector.MatchExpressions, DeepEquals, []metav1.LabelSelectorRequirement{revision})
	// The pod spec is not modified.
	c.Assert(term.LabelSelector.MatchExpressions, HasLen, 0)

	// kube-apiserver may have merged the key already.
	merged := hostnameTerm(&metav1.LabelSelector{MatchLabels: map[string]string{"app": "kafka"},
		MatchExpressions: []metav1.LabelSelectorRequirement{revision}})
	merged.MatchLabelKeys = []string{"controller-revision-hash"}
	pod.Spec.Affinity.PodAntiAffinity.RequiredDuringSchedulingIgnoredDuringExecution = []corev1.PodAffinityTerm{merged}
	result, _ = InheritVolumeAntiAffinityFromPod(pod, nil)
	c.Assert(result.RequiredTerms[0].LabelSelector.MatchExpressions, DeepEquals, []metav1.LabelSelectorRequirement{revision})

	pod.Spec.Affinity = nil
	pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{
		{MaxSkew: 1, TopologyKey: corev1.LabelHostname, LabelSelector: matchLabels("app", "kafka"),
			MatchLabelKeys: []string{"controller-revision-hash"}},
	}
	result, _ = InheritVolumeAntiAffinityFromPod(pod, nil)
	c.Assert(result.PreferredTerms[0].LabelSelector.MatchExpressions, DeepEquals, []metav1.LabelSelectorRequirement{revision})

	// A mismatch key keeps the pod away from pods with another value, which is
	// a relation between different groups.
	mismatch := hostnameTerm(matchLabels("app", "kafka"))
	mismatch.MismatchLabelKeys = []string{"controller-revision-hash"}
	pod.Spec.TopologySpreadConstraints = nil
	pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{mismatch},
	}}
	result, notInherited = InheritVolumeAntiAffinityFromPod(pod, nil)
	c.Assert(result, IsNil)
	c.Assert(notInherited, HasLen, 1)
	c.Assert(strings.Contains(notInherited[0], "does not select the pod itself"), Equals, true)
}

func (s *VolumeAntiAffinitySuite) TestInheritNothing(c *C) {
	pod := newAntiAffinityPod(map[string]string{"app": "kafka"})
	pod.Spec.Affinity = nil
	result, notInherited := InheritVolumeAntiAffinityFromPod(pod, nil)
	c.Assert(result, IsNil)
	c.Assert(notInherited, HasLen, 0)
}

func (s *VolumeAntiAffinitySuite) TestTermKey(c *C) {
	key := func(namespaces []string, selector *metav1.LabelSelector) string {
		return VolumeAntiAffinityTermKey(longhorn.VolumeAntiAffinityTerm{Namespaces: namespaces, LabelSelector: selector})
	}
	in := func(k string, values ...string) metav1.LabelSelectorRequirement {
		return metav1.LabelSelectorRequirement{Key: k, Operator: metav1.LabelSelectorOpIn, Values: values}
	}
	prod := []string{"prod"}

	c.Assert(key(prod, matchLabels("app", "kafka")), Equals,
		key(prod, &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{in("app", "kafka")}}))
	c.Assert(key(prod, &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{in("app", "a", "b"), in("tier", "x")}}), Equals,
		key(prod, &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{in("tier", "x"), in("app", "b", "a", "b")}}))
	c.Assert(key([]string{"stg", "prod"}, matchLabels("app", "kafka")), Equals, key([]string{"prod", "stg"}, matchLabels("app", "kafka")))

	c.Assert(key(prod, matchLabels("app", "kafka")), Not(Equals), key([]string{"stg"}, matchLabels("app", "kafka")))
	c.Assert(key(prod, matchLabels("app", "kafka")), Not(Equals), key(prod, matchLabels("app", "zookeeper")))
	c.Assert(key(prod, matchLabels("app", "kafka")), Not(Equals),
		key(prod, &metav1.LabelSelector{MatchExpressions: []metav1.LabelSelectorRequirement{
			{Key: "app", Operator: metav1.LabelSelectorOpNotIn, Values: []string{"kafka"}},
		}}))
}

func (s *VolumeAntiAffinitySuite) TestPending(c *C) {
	volume := &longhorn.Volume{Spec: longhorn.VolumeSpec{
		VolumeAntiAffinity:        longhorn.VolumeAntiAffinityEnabled,
		VolumeAntiAffinityFromPod: &longhorn.VolumeAntiAffinityFromPod{PendingInheritance: true},
	}}
	c.Assert(IsVolumeAntiAffinityPending(volume), Equals, true)

	// Disabling the feature releases the hold at once.
	volume.Spec.VolumeAntiAffinity = longhorn.VolumeAntiAffinityDisabled
	c.Assert(IsVolumeAntiAffinityPending(volume), Equals, false)

	volume.Spec.VolumeAntiAffinity = longhorn.VolumeAntiAffinityEnabled
	volume.Spec.VolumeAntiAffinityFromPod.PendingInheritance = false
	c.Assert(IsVolumeAntiAffinityPending(volume), Equals, false)
}

func (s *VolumeAntiAffinitySuite) TestValidate(c *C) {
	for _, value := range []longhorn.VolumeAntiAffinity{"", longhorn.VolumeAntiAffinityEnabled, longhorn.VolumeAntiAffinityDisabled} {
		c.Assert(ValidateVolumeAntiAffinity(value), IsNil)
	}
	c.Assert(ValidateVolumeAntiAffinity("required"), NotNil)
}
