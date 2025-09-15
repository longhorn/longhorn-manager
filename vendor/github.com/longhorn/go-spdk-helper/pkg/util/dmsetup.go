package util

import (
	"fmt"
	"regexp"
	"strings"

	commonns "github.com/longhorn/go-common-libs/ns"

	"github.com/longhorn/go-spdk-helper/pkg/types"
)

const (
	dmsetupBinary = "dmsetup"
)

// DmsetupCreate creates a device mapper device with the given name and table
func DmsetupCreate(dmDeviceName, table string, executor *commonns.Executor) error {
	opts := []string{
		"create", dmDeviceName, "--table", table,
	}
	_, err := executor.Execute(nil, dmsetupBinary, opts, types.ExecuteTimeout)
	return err
}

// DmsetupSuspend suspends the device mapper device with the given name
func DmsetupSuspend(dmDeviceName string, noflush, nolockfs bool, executor *commonns.Executor) error {
	opts := []string{
		"suspend", dmDeviceName,
	}

	if noflush {
		opts = append(opts, "--noflush")
	}

	if nolockfs {
		opts = append(opts, "--nolockfs")
	}

	_, err := executor.Execute(nil, dmsetupBinary, opts, types.ExecuteTimeout)
	return err
}

// DmsetupResume removes the device mapper device with the given name
func DmsetupResume(dmDeviceName string, executor *commonns.Executor) error {
	opts := []string{
		"resume", dmDeviceName,
	}
	_, err := executor.Execute(nil, dmsetupBinary, opts, types.ExecuteTimeout)
	return err
}

// DmsetupReload reloads the table of the device mapper device with the given name and table
func DmsetupReload(dmDeviceName, table string, executor *commonns.Executor) error {
	opts := []string{
		"reload", dmDeviceName, "--table", table,
	}
	_, err := executor.Execute(nil, dmsetupBinary, opts, types.ExecuteTimeout)
	return err
}

// DmsetupRemove removes the device mapper device with the given name
func DmsetupRemove(dmDeviceName string, force, deferred bool, executor *commonns.Executor) error {
	opts := []string{
		"remove", dmDeviceName,
	}
	if force {
		opts = append(opts, "--force")
	}
	if deferred {
		opts = append(opts, "--deferred")
	}
	_, err := executor.Execute(nil, dmsetupBinary, opts, types.ExecuteTimeout)
	return err
}

// DmsetupDeps returns the dependent devices of the device mapper device with the given name
func DmsetupDeps(dmDeviceName string, executor *commonns.Executor) ([]string, error) {
	opts := []string{
		"deps", dmDeviceName, "-o", "devname",
	}

	outputStr, err := executor.Execute(nil, dmsetupBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return nil, err
	}

	return parseDependentDevicesFromString(outputStr), nil
}

func parseDependentDevicesFromString(str string) []string {
	re := regexp.MustCompile(`\(([\w-]+)\)`)
	matches := re.FindAllStringSubmatch(str, -1)

	devices := make([]string, 0, len(matches))

	for _, match := range matches {
		devices = append(devices, match[1])
	}

	return devices
}

type DeviceInfo struct {
	Name            string
	BlockDeviceName string
	TableLive       bool
	TableInactive   bool
	Suspended       bool
	ReadOnly        bool
	Major           uint32
	Minor           uint32
	OpenCount       uint32 // Open reference count
	TargetCount     uint32 // Number of targets in the live table
	EventNumber     uint32 // Last event sequence number (used by wait)
}

// DmsetupInfo returns the information of the device mapper device with the given name
func DmsetupInfo(dmDeviceName string, executor *commonns.Executor) ([]*DeviceInfo, error) {
	opts := []string{
		"info",
		"--columns",
		"--noheadings",
		"-o",
		"name,blkdevname,attr,major,minor,open,segments,events",
		"--separator",
		" ",
		dmDeviceName,
	}

	outputStr, err := executor.Execute(nil, dmsetupBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return nil, err
	}

	var (
		lines   = strings.Split(outputStr, "\n")
		devices = []*DeviceInfo{}
	)

	for _, line := range lines {
		var (
			attr = ""
			info = &DeviceInfo{}
		)

		// Break, if the line is empty or EOF
		if line == "" {
			break
		}

		_, err := fmt.Sscan(line,
			&info.Name,
			&info.BlockDeviceName,
			&attr,
			&info.Major,
			&info.Minor,
			&info.OpenCount,
			&info.TargetCount,
			&info.EventNumber)
		if err != nil {
			continue
		}

		// Parse attributes (see "man 8 dmsetup" for details)
		info.Suspended = strings.Contains(attr, "s")
		info.ReadOnly = strings.Contains(attr, "r")
		info.TableLive = strings.Contains(attr, "L")
		info.TableInactive = strings.Contains(attr, "I")

		devices = append(devices, info)
	}

	return devices, nil
}
