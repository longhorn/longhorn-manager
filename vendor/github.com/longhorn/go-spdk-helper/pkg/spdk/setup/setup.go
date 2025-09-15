package setup

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/pkg/errors"

	commonns "github.com/longhorn/go-common-libs/ns"

	"github.com/longhorn/go-spdk-helper/pkg/types"
)

const (
	spdkSetupPath = "/usr/src/spdk/scripts/setup.sh"
)

func Bind(deviceAddr, deviceDriver string, executor *commonns.Executor) (string, error) {
	if deviceAddr == "" {
		return "", fmt.Errorf("device address is empty")
	}

	envs := []string{
		fmt.Sprintf("%s=%s", "PCI_ALLOWED", deviceAddr),
	}
	if deviceDriver != "" {
		envs = append(envs, fmt.Sprintf("%s=%s", "DRIVER_OVERRIDE", deviceDriver))
	}

	cmdArgs := []string{
		spdkSetupPath,
	}

	outputStr, err := executor.Execute(envs, "bash", cmdArgs, types.ExecuteTimeout)
	if err != nil {
		return "", err
	}

	return outputStr, nil
}

func Unbind(deviceAddr string, executor *commonns.Executor) (string, error) {
	if deviceAddr == "" {
		return "", fmt.Errorf("device address is empty")
	}

	cmdArgs := []string{
		spdkSetupPath,
		"unbind",
		deviceAddr,
	}

	outputStr, err := executor.Execute(nil, "bash", cmdArgs, types.ExecuteTimeout)
	if err != nil {
		return "", err
	}

	return outputStr, nil
}

func GetDiskDriver(deviceAddr string, executor *commonns.Executor) (string, error) {
	if deviceAddr == "" {
		return "", fmt.Errorf("device address is empty")
	}

	cmdArgs := []string{
		spdkSetupPath,
		"disk-driver",
		deviceAddr,
	}

	outputStr, err := executor.Execute(nil, "bash", cmdArgs, types.ExecuteTimeout)
	if err != nil {
		return "", err
	}

	return extractJSON(outputStr)
}

func GetDiskStatus(deviceAddr string, executor *commonns.Executor) (*types.DiskStatus, error) {
	if deviceAddr == "" {
		return nil, fmt.Errorf("device address is empty")
	}

	cmdArgs := []string{
		spdkSetupPath,
		"disk-status",
		deviceAddr,
	}

	outputStr, err := executor.Execute(nil, "bash", cmdArgs, types.ExecuteTimeout)
	if err != nil {
		return nil, err
	}

	jsonsStr, err := extractJSON(outputStr)
	if err != nil {
		return nil, err
	}

	var diskStatus types.DiskStatus
	err = json.Unmarshal([]byte(jsonsStr), &diskStatus)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal disk status for %s", deviceAddr)
	}

	return &diskStatus, nil
}

func extractJSON(outputStr string) (string, error) {
	// Find the first '{' and last '}' characters, assuming valid JSON format
	start := strings.Index(outputStr, "{")
	end := strings.LastIndex(outputStr, "}")
	if start != -1 && end != -1 {
		return outputStr[start : end+1], nil
	}
	return "", fmt.Errorf("failed to extract JSON from output: %s", outputStr)
}
