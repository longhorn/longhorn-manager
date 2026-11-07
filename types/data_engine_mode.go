package types

import (
	"fmt"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// DataEngineMode is the process's installation capability, not a mutable engine
// setting. A supported engine still needs its existing enablement setting.
type DataEngineMode string

const (
	DataEngineModeV1Capable DataEngineMode = "v1-capable"
	DataEngineModeV2Only    DataEngineMode = "v2-only"
)

func ParseDataEngineMode(value string) (DataEngineMode, error) {
	switch mode := DataEngineMode(value); mode {
	case "", DataEngineModeV1Capable:
		return DataEngineModeV1Capable, nil
	case DataEngineModeV2Only:
		return mode, nil
	default:
		return "", fmt.Errorf("unsupported data engine mode %q: expected v1-capable or v2-only", value)
	}
}

func (mode DataEngineMode) RequiresISCSI() bool {
	return mode != DataEngineModeV2Only
}

func (mode DataEngineMode) AllowsDataEngine(engine longhorn.DataEngineType) bool {
	switch mode {
	case "", DataEngineModeV1Capable:
		// Preserve legacy empty V1 fields and combined instance managers.
		return engine == "" || engine == longhorn.DataEngineTypeV1 || engine == longhorn.DataEngineTypeV2 || engine == longhorn.DataEngineTypeAll
	case DataEngineModeV2Only:
		return engine == longhorn.DataEngineTypeV2
	default:
		return false
	}
}
