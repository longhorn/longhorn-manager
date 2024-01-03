package common

import (
	"testing"

	"github.com/stretchr/testify/assert"
	v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestIsRemovingLonghornFinalizer(t *testing.T) {
	assert := assert.New(t)
	now := v1.Now()

	tests := map[string]struct {
		oldObj  runtime.Object
		newObj  runtime.Object
		want    bool
		wantErr bool
	}{
		"notBeingDeleted": {
			oldObj:  &longhorn.Node{},
			newObj:  &longhorn.Node{},
			want:    false,
			wantErr: false,
		},
		"lessThanOneFinalizerRemoved": {
			oldObj: &longhorn.Node{
				ObjectMeta: v1.ObjectMeta{
					DeletionTimestamp: &now,
					Finalizers:        []string{longhornFinalizerKey, "otherFinalizer"},
				},
			},
			newObj: &longhorn.Node{
				ObjectMeta: v1.ObjectMeta{
					DeletionTimestamp: &now,
					Finalizers:        []string{longhornFinalizerKey, "otherFinalizer"},
				},
			},
			want:    false,
			wantErr: false,
		},
		"moreThanOneFinalizerRemoved": {
			oldObj: &longhorn.Node{
				ObjectMeta: v1.ObjectMeta{
					DeletionTimestamp: &now,
					Finalizers:        []string{longhornFinalizerKey, "otherFinalizer"},
				},
			},
			newObj: &longhorn.Node{
				ObjectMeta: v1.ObjectMeta{
					DeletionTimestamp: &now,
				},
			},
			want:    false,
			wantErr: false,
		},
		"noLonghornFinalizerInOld": {
			oldObj: &longhorn.Node{
				ObjectMeta: v1.ObjectMeta{
					DeletionTimestamp: &now,
					Finalizers:        []string{"otherFinalizer"},
				},
			},
			newObj: &longhorn.Node{
				ObjectMeta: v1.ObjectMeta{
					DeletionTimestamp: &now,
					Finalizers:        []string{},
				},
			},
			want:    false,
			wantErr: false,
		},
		"longhornFinalizerInNew": {
			oldObj: &longhorn.Node{
				ObjectMeta: v1.ObjectMeta{
					DeletionTimestamp: &now,
					Finalizers:        []string{longhornFinalizerKey, "otherFinalizer"},
				},
			},
			newObj: &longhorn.Node{
				ObjectMeta: v1.ObjectMeta{
					DeletionTimestamp: &now,
					Finalizers:        []string{longhornFinalizerKey},
				},
			},
			want:    false,
			wantErr: false,
		},
		"correctConditions": {
			oldObj: &longhorn.Node{
				ObjectMeta: v1.ObjectMeta{
					DeletionTimestamp: &now,
					Finalizers:        []string{longhornFinalizerKey, "otherFinalizer"},
				},
			},
			newObj: &longhorn.Node{
				ObjectMeta: v1.ObjectMeta{
					DeletionTimestamp: &now,
					Finalizers:        []string{"otherFinalizer"},
				},
			},
			want:    true,
			wantErr: false,
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := IsRemovingLonghornFinalizer(tc.oldObj, tc.newObj)
			assert.Equal(got, tc.want)
			if tc.wantErr {
				assert.Error(err)
			} else {
				assert.NoError(err)
			}
		})
	}
}
