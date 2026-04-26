package engineapi

import (
	"testing"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestGetObjInfoForVolumeExpand(t *testing.T) {
	engine := &longhorn.Engine{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vol-e-0",
		},
		Spec: longhorn.EngineSpec{
			InstanceSpec: longhorn.InstanceSpec{
				DataEngine: longhorn.DataEngineTypeV2,
				VolumeName: "vol",
			},
		},
	}

	var obj DataEngineObject = engine
	if obj.GetDataEngine() != string(longhorn.DataEngineTypeV2) || obj.GetEngineName() != "vol-e-0" || obj.GetEngineFrontendName() != "" || obj.GetVolumeName() != "vol" {
		t.Fatalf("Engine DataEngineObject = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
			obj.GetDataEngine(), obj.GetEngineName(), obj.GetEngineFrontendName(), obj.GetVolumeName(),
			string(longhorn.DataEngineTypeV2), "vol-e-0", "", "vol")
	}

	engineFrontend := &longhorn.EngineFrontend{
		ObjectMeta: metav1.ObjectMeta{
			Name: "vol-ef-0",
		},
		Spec: longhorn.EngineFrontendSpec{
			InstanceSpec: longhorn.InstanceSpec{
				DataEngine: longhorn.DataEngineTypeV2,
				VolumeName: "vol",
			},
			EngineName: "vol-e-0",
		},
	}

	obj = engineFrontend
	if obj.GetDataEngine() != string(longhorn.DataEngineTypeV2) || obj.GetEngineName() != "vol-e-0" || obj.GetEngineFrontendName() != "vol-ef-0" || obj.GetVolumeName() != "vol" {
		t.Fatalf("EngineFrontend DataEngineObject = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
			obj.GetDataEngine(), obj.GetEngineName(), obj.GetEngineFrontendName(), obj.GetVolumeName(),
			string(longhorn.DataEngineTypeV2), "vol-e-0", "vol-ef-0", "vol")
	}
}

func TestGetEngineFrontendInstanceSize(t *testing.T) {
	engineFrontend := &longhorn.EngineFrontend{
		Spec: longhorn.EngineFrontendSpec{
			InstanceSpec: longhorn.InstanceSpec{
				VolumeSize: 100,
			},
			Size: 50,
		},
	}

	if got := getEngineFrontendInstanceSize(engineFrontend); got != 50 {
		t.Fatalf("EngineFrontend instance size = %d, want %d", got, 50)
	}

	engineFrontend.Spec.Size = 0
	if got := getEngineFrontendInstanceSize(engineFrontend); got != 100 {
		t.Fatalf("EngineFrontend instance fallback size = %d, want %d", got, 100)
	}
}

func TestGetVolumeFrontendServiceObject(t *testing.T) {
	engine := &longhorn.Engine{
		Spec: longhorn.EngineSpec{
			InstanceSpec: longhorn.InstanceSpec{
				DataEngine: longhorn.DataEngineTypeV2,
			},
		},
	}
	engineFrontend := &longhorn.EngineFrontend{}

	if got := getVolumeFrontendServiceObject(engine, engineFrontend); got != engineFrontend {
		t.Fatalf("v2 VolumeFrontendGet service object = %#v, want engine frontend", got)
	}

	engine.Spec.DataEngine = longhorn.DataEngineTypeV1
	if got := getVolumeFrontendServiceObject(engine, engineFrontend); got != engine {
		t.Fatalf("v1 VolumeFrontendGet service object = %#v, want engine", got)
	}

	engine.Spec.DataEngine = longhorn.DataEngineTypeV2
	if got := getVolumeFrontendServiceObject(engine, nil); got != engine {
		t.Fatalf("nil frontend VolumeFrontendGet service object = %#v, want engine", got)
	}
}
