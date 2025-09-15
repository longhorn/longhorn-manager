package types

type LogSetFlagRequest struct {
	Flag string `json:"flag"`
}

type LogClearFlagRequest struct {
	Flag string `json:"flag"`
}

type LogGetFlagsRequest struct {
}

type LogSetLevelRequest struct {
	Level string `json:"level"`
}

type LogGetLevelRequest struct {
}

type LogSetPrintLevelRequest struct {
	Level string `json:"level"`
}

type LogGetPrintLevelRequest struct {
}
