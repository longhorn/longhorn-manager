package instancemanagerupgrade

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestInstanceManagerUpgradeValidatorUpdateSpecImmutable(t *testing.T) {
	validator := &instanceManagerUpgradeValidator{}
	oldIMU := &longhorn.InstanceManagerUpgrade{
		ObjectMeta: metav1.ObjectMeta{Name: "imu"},
		Spec: longhorn.InstanceManagerUpgradeSpec{
			NodeID:      "node-1",
			TargetImage: "image-1",
		},
	}

	tests := map[string]struct {
		mutate  func(*longhorn.InstanceManagerUpgrade)
		wantErr bool
	}{
		"allows unchanged spec": {
			mutate: func(imu *longhorn.InstanceManagerUpgrade) {
				imu.Status.State = longhorn.InstanceManagerUpgradeStatePending
			},
		},
		"rejects node change": {
			mutate: func(imu *longhorn.InstanceManagerUpgrade) {
				imu.Spec.NodeID = "node-2"
			},
			wantErr: true,
		},
		"rejects target image change": {
			mutate: func(imu *longhorn.InstanceManagerUpgrade) {
				imu.Spec.TargetImage = "image-2"
			},
			wantErr: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			newIMU := oldIMU.DeepCopy()
			test.mutate(newIMU)
			err := validator.Update(nil, oldIMU, newIMU)
			if test.wantErr && err == nil {
				t.Fatal("expected validation error")
			}
			if !test.wantErr && err != nil {
				t.Fatalf("unexpected validation error: %v", err)
			}
		})
	}
}
