package util

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/go-spdk-helper/pkg/types"

	commontypes "github.com/longhorn/go-common-libs/types"
	helperutil "github.com/longhorn/go-spdk-helper/pkg/util"
)

func GetDevNameFromBDF(bdf string) (string, error) {
	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return "", errors.Wrap(err, "failed to create executor")
	}

	cmdArgs := []string{"-n", "--nodeps", "--output", "NAME"}
	output, err := ne.Execute(nil, "lsblk", cmdArgs, types.ExecuteTimeout)
	if err != nil {
		return "", errors.Wrap(err, "failed to list block devices")
	}

	devices := strings.Fields(string(output))
	for _, dev := range devices {
		link, err := os.Readlink("/sys/block/" + dev)
		if err != nil {
			logrus.WithError(err).Warnf("Failed to read link for %s", dev)
			continue
		}

		if strings.Contains(link, bdf) {
			return dev, nil
		}
	}

	return "", fmt.Errorf("failed to find device for BDF %s", bdf)
}

type BlockDevice struct {
	Name       string   `json:"name"`
	Path       string   `json:"path"`
	Subsystems []string `json:"subsystems"`
	Maj        int      `json:"maj"`
	Min        int      `json:"min"`
}

type BlockDevices struct {
	BlockDevices []struct {
		Name       string `json:"name"`
		Path       string `json:"path"`
		MajMin     string `json:"maj:min"`
		Subsystems string `json:"subsystems"`
	} `json:"blockdevices"`
}

func GetBlockDevice(devPath string) (BlockDevice, error) {
	ne, err := helperutil.NewExecutor(commontypes.ProcDirectory)
	if err != nil {
		return BlockDevice{}, errors.Wrap(err, "failed to create executor")
	}

	cmdArgs := []string{"-O", "-J", devPath}
	output, err := ne.Execute(nil, "lsblk", cmdArgs, types.ExecuteTimeout)
	if err != nil {
		return BlockDevice{}, errors.Wrap(err, "failed to get disk subsystems")
	}

	var blockDevices BlockDevices
	err = json.Unmarshal([]byte(output), &blockDevices)
	if err != nil {
		return BlockDevice{}, err
	}

	if len(blockDevices.BlockDevices) == 0 {
		return BlockDevice{}, fmt.Errorf("no blockdevices found")
	}

	bd := blockDevices.BlockDevices[0]
	majMinParts := strings.Split(bd.MajMin, ":")
	if len(majMinParts) != 2 {
		return BlockDevice{}, fmt.Errorf("invalid maj:min format")
	}
	maj, err := strconv.Atoi(majMinParts[0])
	if err != nil {
		return BlockDevice{}, err
	}
	min, err := strconv.Atoi(majMinParts[1])
	if err != nil {
		return BlockDevice{}, err
	}

	subsystems := strings.Split(bd.Subsystems, ":")

	return BlockDevice{
		Name:       bd.Name,
		Path:       bd.Path,
		Subsystems: subsystems,
		Maj:        maj,
		Min:        min,
	}, nil
}
