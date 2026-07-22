package volume

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const tib = int64(1) << 40

func TestCheckECExpansionCeiling(t *testing.T) {
	testCases := []struct {
		name         string
		creationSize int64
		newSize      int64
		wantErr      bool
	}{
		{
			name:         "unknown creation size fails open",
			creationSize: 0,
			newSize:      100 * tib,
			wantErr:      false,
		},
		{
			name:         "exactly 10x is allowed",
			creationSize: tib,
			newSize:      10 * tib,
			wantErr:      false,
		},
		{
			name:         "one byte past 10x is rejected",
			creationSize: tib,
			newSize:      10*tib + 1,
			wantErr:      true,
		},
		{
			name:         "below ceiling is allowed",
			creationSize: tib,
			newSize:      2 * tib,
			wantErr:      false,
		},
		{
			name:         "growth past the creation cap is allowed within 10x",
			creationSize: 200 * tib,
			newSize:      2000 * tib, // > EcLvstoreMaxCreationSize; the cap is creation-only
			wantErr:      false,
		},
		{
			name:         "overflow guard rejects absurd creation size",
			creationSize: int64(^uint64(0) >> 1), // math.MaxInt64
			newSize:      tib,
			wantErr:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkECExpansionCeiling("test-vol", tc.creationSize, tc.newSize)
			if (err != nil) != tc.wantErr {
				t.Errorf("checkECExpansionCeiling(creationSize=%v, newSize=%v) error = %v, wantErr %v",
					tc.creationSize, tc.newSize, err, tc.wantErr)
			}
		})
	}
}

func TestECCreationSizeCap(t *testing.T) {
	// The cap comes from the vendored EcLvstoreMaxCreationSize: SPDK rejects
	// lvstore creation when the metadata page count exceeds UINT32_MAX, which
	// at the pinned ratio is (MaxUint32 / 10) * 4 MiB, roughly 1.6 PiB. Pin
	// its magnitude to catch a silently changed constant.
	maxCreationSize := int64(spdktypes.EcLvstoreMaxCreationSize)
	const pib = tib << 10
	if maxCreationSize <= pib || maxCreationSize >= 2*pib {
		t.Errorf("EcLvstoreMaxCreationSize = %v, expected within (1 PiB, 2 PiB)", maxCreationSize)
	}

	// The maximum volume size depends on the EC geometry because the cap is
	// checked against the EC bdev usable size, not the raw volume size.
	const k, stripSizeKB = 4, 64
	maxVolumeSize := spdktypes.MaxECVolumeSizeForCreation(k, stripSizeKB)
	if maxVolumeSize >= maxCreationSize {
		t.Errorf("MaxECVolumeSizeForCreation = %v, expected below EcLvstoreMaxCreationSize %v (metadata margin)", maxVolumeSize, maxCreationSize)
	}

	testCases := []struct {
		name       string
		layoutType longhorn.VolumeDataLayoutType
		size       int64
		wantErr    bool
	}{
		{
			name:       "sharded volume at the maximum is allowed",
			layoutType: longhorn.VolumeDataLayoutTypeSharded,
			size:       maxVolumeSize,
			wantErr:    false,
		},
		{
			name:       "sharded volume one byte over the maximum is rejected",
			layoutType: longhorn.VolumeDataLayoutTypeSharded,
			size:       maxVolumeSize + 1,
			wantErr:    true,
		},
		{
			name:       "non-sharded volume over the maximum is allowed",
			layoutType: longhorn.VolumeDataLayoutTypeReplicated,
			size:       maxVolumeSize + 1,
			wantErr:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			volume := &longhorn.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: "test-vol"},
				Spec: longhorn.VolumeSpec{
					Size: tc.size,
					DataLayout: longhorn.VolumeDataLayout{
						Type:        tc.layoutType,
						DataChunks:  k,
						StripSizeKB: stripSizeKB,
					},
				},
			}
			err := checkECCreationSizeCap(volume, tc.size)
			if (err != nil) != tc.wantErr {
				t.Errorf("checkECCreationSizeCap(type=%v, size=%v) error = %v, wantErr %v",
					tc.layoutType, tc.size, err, tc.wantErr)
			}
		})
	}
}
