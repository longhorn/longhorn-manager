package client

import (
	"strings"
	"testing"

	"github.com/longhorn/longhorn-manager/types"
)

func TestInvalidDataEngineCapabilityRejectedBeforeClientConfiguration(t *testing.T) {
	for _, nodeLocal := range []bool{false, true} {
		var err error
		if nodeLocal {
			_, err = NewClientsForNodeLocal("/must-not-be-read", types.DataEngineMode("unknown"), t.Context().Done())
		} else {
			_, err = NewClients("/must-not-be-read", true, types.DataEngineMode("unknown"), t.Context().Done())
		}
		if err == nil || !strings.Contains(err.Error(), "unsupported data engine mode") {
			t.Fatalf("node-local=%t: invalid capability reached client setup: %v", nodeLocal, err)
		}
	}
}
