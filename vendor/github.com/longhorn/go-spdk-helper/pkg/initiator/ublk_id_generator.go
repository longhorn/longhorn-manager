package initiator

import (
	"fmt"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

// IDGenerator tracks only the last ID returned (which is never 0).
// Valid range for returned IDs is [1..MaxUblkId].
type IDGenerator struct {
	lastCandidate int32
}

// GetAvailableID returns an ID in [1..65535] that is NOT in inUseUblkDeviceList.
// We do a circular search, skipping ID=0 entirely.
// This function is not thread safe. Caller need to have their own locking mechanism
func (gen *IDGenerator) GetAvailableID(inUseUblkDeviceList []spdktypes.UblkDevice) (int32, error) {
	// Make a set for quick membership checks
	inUsedMap := make(map[int32]struct{}, len(inUseUblkDeviceList))
	for _, ublkDevice := range inUseUblkDeviceList {
		inUsedMap[ublkDevice.ID] = struct{}{}
	}

	// We'll try up to MaxUblkId times, because 0 is excluded
	for i := int32(1); i <= MaxUblkId; i++ {
		candidate := (gen.lastCandidate + i) & 0xFFFF // same as % 65536, but faster with bitwise
		if candidate == 0 {
			continue
		}

		if _, used := inUsedMap[candidate]; !used {
			// Found an available ID
			gen.lastCandidate = candidate
			return candidate, nil
		}
	}

	// If every ID in [1..MaxUblkId] is used, no ID is available
	return 0, fmt.Errorf("no available ID (1..%v are all in use)", MaxUblkId)
}
