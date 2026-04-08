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

	dataEngine, engineName, engineFrontendName, volumeName, err := (&Proxy{}).GetObjInfo(engine)
	if err != nil {
		t.Fatalf("GetObjInfo(engine) returned error: %v", err)
	}
	if dataEngine != string(longhorn.DataEngineTypeV2) || engineName != "vol-e-0" || engineFrontendName != "" || volumeName != "vol" {
		t.Fatalf("GetObjInfo(engine) = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
			dataEngine, engineName, engineFrontendName, volumeName,
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

	dataEngine, engineName, engineFrontendName, volumeName, err = (&Proxy{}).GetObjInfo(engineFrontend)
	if err != nil {
		t.Fatalf("GetObjInfo(engineFrontend) returned error: %v", err)
	}
	if dataEngine != string(longhorn.DataEngineTypeV2) || engineName != "vol-e-0" || engineFrontendName != "vol-ef-0" || volumeName != "vol" {
		t.Fatalf("GetObjInfo(engineFrontend) = (%q, %q, %q, %q), want (%q, %q, %q, %q)",
			dataEngine, engineName, engineFrontendName, volumeName,
			string(longhorn.DataEngineTypeV2), "vol-e-0", "vol-ef-0", "vol")
	}
}
