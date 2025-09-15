package types

type BdevDriverSpecificAio struct {
	FileName          string `json:"filename"`
	ReadOnly          bool   `json:"readonly"`
	BlockSizeOverride bool   `json:"block_size_override"`
}

type BdevAioDriverSpecificInfo struct {
	Filename          string `json:"filename"`
	BlockSizeOverride bool   `json:"block_size_override"`
	Readonly          bool   `json:"readonly"`
}

type BdevAioCreateRequest struct {
	Name      string `json:"name"`
	Filename  string `json:"filename"`
	BlockSize uint64 `json:"block_size,omitzero"`
}

type BdevAioDeleteRequest struct {
	Name string `json:"name"`
}
