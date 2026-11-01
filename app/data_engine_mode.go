package app

import (
	"fmt"

	"github.com/urfave/cli/v3"

	"github.com/longhorn/longhorn-manager/types"
)

func checkISCSIForDataEngineMode(mode types.DataEngineMode, check func() error) error {
	parsed, err := types.ParseDataEngineMode(string(mode))
	if err != nil {
		return err
	}
	if !parsed.RequiresISCSI() {
		return nil
	}
	if check == nil {
		return fmt.Errorf("V1-capable mode requires an iSCSI host check")
	}
	return check()
}

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
