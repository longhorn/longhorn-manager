package initiator

import (
	"fmt"
	"sync"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

type IDGenerator struct {
	mu            sync.Mutex
	lastCandidate int32
}

func (gen *IDGenerator) GetAvailableID(inUseUblkDeviceList []spdktypes.UblkDevice) (int32, error) {
	gen.mu.Lock()
	defer gen.mu.Unlock()

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
