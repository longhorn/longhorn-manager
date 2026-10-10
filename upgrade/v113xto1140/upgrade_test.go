package v113xto1140

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func newV2InstanceManager(name, image string, created time.Time, spec longhorn.V2DataEngineSpec) *longhorn.InstanceManager {
	return &longhorn.InstanceManager{
		ObjectMeta: metav1.ObjectMeta{Name: name, CreationTimestamp: metav1.NewTime(created)},
		Spec: longhorn.InstanceManagerSpec{
			Image:          image,
			DataEngine:     longhorn.DataEngineTypeV2,
			DataEngineSpec: longhorn.DataEngineSpec{V2: spec},
		},
	}
}

func TestSourceInstanceManager(t *testing.T) {
	now := time.Now()
	older := newV2InstanceManager("older", "image-old", now.Add(-time.Hour), longhorn.V2DataEngineSpec{})
	current := newV2InstanceManager("current", "image-default", now.Add(-2*time.Hour), longhorn.V2DataEngineSpec{})
	newest := newV2InstanceManager("newest", "image-other", now, longhorn.V2DataEngineSpec{})

	assert.Equal(t, current, sourceInstanceManager([]*longhorn.InstanceManager{older, current, newest}, "image-default"))
	assert.Equal(t, newest, sourceInstanceManager([]*longhorn.InstanceManager{older, newest}, "image-default"))
}

func TestMigrateV2DataEngineSpec(t *testing.T) {
	trueValue, cpuMask := true, "0x3"

	tests := map[string]struct {
		spec     longhorn.V2DataEngineSpec
		existing *longhorn.NodeV2DataEngineResources
		expected *longhorn.NodeDataEngineResources
	}{
		"leaves the node untouched without overrides": {},
		"copies both overrides and normalizes the CPU mask": {
			spec:     longhorn.V2DataEngineSpec{CPUMask: "0-1", CPUIsolationEnabled: longhorn.TrueValue},
			expected: &longhorn.NodeDataEngineResources{V2: &longhorn.NodeV2DataEngineResources{CPUMask: &cpuMask, CPUIsolationEnabled: &trueValue}},
		},
		"skips an invalid CPU mask": {
			spec: longhorn.V2DataEngineSpec{CPUMask: "invalid"},
		},
		"keeps values already on the node": {
			spec:     longhorn.V2DataEngineSpec{CPUMask: "0x1", CPUIsolationEnabled: longhorn.FalseValue},
			existing: &longhorn.NodeV2DataEngineResources{CPUMask: &cpuMask, CPUIsolationEnabled: &trueValue},
			expected: &longhorn.NodeDataEngineResources{V2: &longhorn.NodeV2DataEngineResources{CPUMask: &cpuMask, CPUIsolationEnabled: &trueValue}},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			node := &longhorn.Node{}
			if test.existing != nil {
				node.Spec.DataEngineResources = &longhorn.NodeDataEngineResources{V2: test.existing}
			}
			migrateV2DataEngineSpec(node, newV2InstanceManager("im", "image", time.Now(), test.spec))
			assert.Equal(t, test.expected, node.Spec.DataEngineResources)
		})
	}
}
