package app

import (
	"github.com/urfave/cli/v3"

	"github.com/longhorn/longhorn-manager/types"
)

const FlagDataEngineMode = "data-engine-mode"

func dataEngineModeFlag() cli.Flag {
	return &cli.StringFlag{
		Name:             FlagDataEngineMode,
		Value:            string(types.DataEngineModeV1Capable),
		Usage:            "Data engine installation capability: v1-capable or v2-only",
		ValidateDefaults: true,
		Validator: func(value string) error {
			_, err := types.ParseDataEngineMode(value)
			return err
		},
	}
}
