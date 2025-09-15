package initiator

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	commonns "github.com/longhorn/go-common-libs/ns"

	"github.com/longhorn/go-spdk-helper/pkg/types"
)

const (
	nvmeBinary = "nvme"

	DefaultTransportType = "tcp"

	// Set short ctrlLossTimeoutSec for quick response to the controller loss.
	defaultCtrlLossTmo    = 30
	defaultKeepAliveTmo   = 5
	defaultReconnectDelay = 2
)

type Device struct {
	Subsystem    string
	SubsystemNQN string
	Controllers  []Controller
	Namespaces   []Namespace
}

type DiscoveryPageEntry struct {
	PortID  uint16 `json:"portid"`
	TrsvcID string `json:"trsvcid"`
	Subnqn  string `json:"subnqn"`
	Traddr  string `json:"traddr"`
	SubType string `json:"subtype"`
}

type Controller struct {
	Controller string
	Transport  string
	Address    string
	State      string
}

// Namespace fields use signed integers instead, because the output of buggy nvme-cli 2.x is possibly negative.
type Namespace struct {
	NameSpace    string
	NSID         int32
	UsedBytes    int64
	MaximumLBA   int64
	PhysicalSize int64
	SectorSize   int32
}

type Subsystem struct {
	Name  string `json:"Name,omitempty"`
	NQN   string `json:"NQN,omitempty"`
	Paths []Path `json:"Paths,omitempty"`
}

type Path struct {
	Name      string `json:"Name,omitempty"`
	Transport string `json:"Transport,omitempty"`
	Address   string `json:"Address,omitempty"`
	State     string `json:"State,omitempty"`
}

func cliVersion(executor *commonns.Executor) (major, minor int, err error) {
	opts := []string{
		"--version",
	}
	outputStr, err := executor.Execute(nil, nvmeBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return 0, 0, err
	}

	versionStr := ""
	lines := strings.Split(string(outputStr), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "nvme") {
			versionStr = strings.TrimSpace(line)
			break
		}
	}

	var version string
	for _, s := range strings.Split(versionStr, " ") {
		if strings.Contains(s, ".") {
			version = s
			break
		}
	}
	if version == "" {
		return 0, 0, fmt.Errorf("failed to get version from %s", outputStr)
	}
	versionArr := strings.Split(version, ".")
	if len(versionArr) >= 1 {
		major, _ = strconv.Atoi(versionArr[0])
	}
	if len(versionArr) >= 2 {
		minor, _ = strconv.Atoi(versionArr[1])
	}
	return major, minor, nil
}

func showHostNQN(executor *commonns.Executor) (string, error) {
	opts := []string{
		"--show-hostnqn",
	}

	outputStr, err := executor.Execute(nil, nvmeBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return "", err
	}

	lines := strings.Split(string(outputStr), "\n")
	for _, line := range lines {
		if strings.HasPrefix(line, "nqn.") {
			return strings.TrimSpace(line), nil
		}
	}
	return "", fmt.Errorf("failed to get host NQN from %s", outputStr)
}

func listSubsystems(devicePath string, executor *commonns.Executor) ([]Subsystem, error) {
	major, _, err := cliVersion(executor)
	if err != nil {
		return nil, err
	}

	opts := []string{
		"list-subsys",
		"-o", "json",
	}

	if devicePath != "" {
		opts = append(opts, devicePath)
	}

	outputStr, err := executor.Execute(nil, nvmeBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return nil, err
	}
	jsonStr, err := extractJSONString(outputStr)
	if err != nil {
		return nil, err
	}

	if major == 1 {
		return listSubsystemsV1(jsonStr)
	}
	return listSubsystemsV2(jsonStr)
}

func listSubsystemsV1(jsonStr string) ([]Subsystem, error) {
	output := map[string][]Subsystem{}
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		return nil, err
	}

	return output["Subsystems"], nil
}

type ListSubsystemsV2Output struct {
	HostNQN    string      `json:"HostNQN"`
	HostID     string      `json:"HostID"`
	Subsystems []Subsystem `json:"Subsystems"`
}

func listSubsystemsV2(jsonStr string) ([]Subsystem, error) {
	var output []ListSubsystemsV2Output
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		return nil, err
	}

	subsystems := []Subsystem{}
	for _, o := range output {
		subsystems = append(subsystems, o.Subsystems...)
	}

	return subsystems, nil
}

// CliDevice fields use signed integers instead, because the output of buggy nvme-cli 2.x is possibly negative.
type CliDevice struct {
	NameSpace    int32  `json:"Namespace,omitempty"`
	DevicePath   string `json:"DevicePath,omitempty"`
	Firmware     string `json:"Firmware,omitempty"`
	Index        int32  `json:"Index,omitempty"`
	ModelNumber  string `json:"ModelNumber,omitempty"`
	SerialNumber string `json:"SerialNumber,omitempty"`
	UsedBytes    int64  `json:"UsedBytes,omitempty"`
	MaximumLBA   int64  `json:"MaximumLBA,omitempty"`
	PhysicalSize int64  `json:"PhysicalSize,omitempty"`
	SectorSize   int32  `json:"SectorSize,omitempty"`
}

func listRecognizedNvmeDevices(executor *commonns.Executor) ([]CliDevice, error) {
	opts := []string{
		"list",
		"-o", "json",
	}
	outputStr, err := executor.Execute(nil, nvmeBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return nil, err
	}
	jsonStr, err := extractJSONString(outputStr)
	if err != nil {
		return nil, err
	}
	output := map[string][]CliDevice{}
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		return nil, err
	}

	return output["Devices"], nil
}

func getHostID(executor *commonns.Executor) (string, error) {
	outputStr, err := executor.Execute(nil, "cat", []string{"/etc/nvme/hostid"}, types.ExecuteTimeout)
	if err == nil {
		return strings.TrimSpace(string(outputStr)), nil
	}

	outputStr, err = executor.Execute(nil, "cat", []string{"/sys/class/dmi/id/product_uuid"}, types.ExecuteTimeout)
	if err == nil {
		return strings.TrimSpace(string(outputStr)), nil
	}

	return "", err
}

func discovery(hostID, hostNQN, ip, port string, executor *commonns.Executor) ([]DiscoveryPageEntry, error) {
	opts := []string{
		"discover",
		"-t", DefaultTransportType,
		"-a", ip,
		"-s", port,
		"-o", "json",
	}
	if hostID != "" {
		opts = append(opts, "-I", hostID)
	}
	if hostNQN != "" {
		opts = append(opts, "-q", hostNQN)
	}

	// A valid output is like below:
	// # nvme discover -t tcp -a 10.42.2.20 -s 20011 -o json
	//	{
	//		"device" : "nvme0",
	//		"genctr" : 2,
	//		"records" : [
	//		  {
	//			"trtype" : "tcp",
	//			"adrfam" : "ipv4",
	//			"subtype" : "nvme subsystem",
	//			"treq" : "not required",
	//			"portid" : 0,
	//			"trsvcid" : "20001",
	//			"subnqn" : "nqn.2023-01.io.longhorn.spdk:pvc-81bab972-8e6b-48be-b691-18eaa430a897-r-0881c7b4",
	//			"traddr" : "10.42.2.20",
	//			"sectype" : "none"
	//		  },
	//		  {
	//			"trtype" : "tcp",
	//			"adrfam" : "ipv4",
	//			"subtype" : "nvme subsystem",
	//			"treq" : "not required",
	//			"portid" : 0,
	//			"trsvcid" : "20011",
	//			"subnqn" : "nqn.2023-01.io.longhorn.spdk:pvc-5f94d59d-baec-40e5-9e8b-25b79909d14e-e-49c947f5",
	//			"traddr" : "10.42.2.20",
	//			"sectype" : "none"
	//		  }
	//		]
	//	  }

	// nvme discover does not respect the -s option, so we need to filter the output
	outputStr, err := executor.Execute(nil, nvmeBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return nil, err
	}

	jsonStr, err := extractJSONString(outputStr)
	if err != nil {
		return nil, err
	}

	var output struct {
		Entries []DiscoveryPageEntry `json:"records"`
	}

	err = json.Unmarshal([]byte(jsonStr), &output)
	if err != nil {
		return nil, err
	}

	return output.Entries, nil
}

func connect(hostID, hostNQN, nqn, transpotType, ip, port string, executor *commonns.Executor) (string, error) {
	var err error

	opts := []string{
		"connect",
		"-t", transpotType,
		"--nqn", nqn,
		"--ctrl-loss-tmo", strconv.Itoa(defaultCtrlLossTmo),
		"--keep-alive-tmo", strconv.Itoa(defaultKeepAliveTmo),
		"--reconnect-delay", strconv.Itoa(defaultReconnectDelay),
		"-o", "json",
	}

	if hostID != "" {
		opts = append(opts, "-I", hostID)
	}
	if hostNQN != "" {
		opts = append(opts, "-q", hostNQN)
	}
	if ip != "" {
		opts = append(opts, "-a", ip)
	}
	if port != "" {
		opts = append(opts, "-s", port)
	}

	// The output example:
	// {
	//  "device" : "nvme0"
	// }
	outputStr, err := executor.Execute(nil, nvmeBinary, opts, types.ExecuteTimeout)
	if err != nil {
		return "", err
	}

	jsonStr, err := extractJSONString(outputStr)
	if err != nil {
		return "", err
	}

	output := map[string]string{}
	if err := json.Unmarshal([]byte(jsonStr), &output); err != nil {
		return "", err
	}

	return output["device"], nil
}

func disconnect(nqn string, executor *commonns.Executor) error {
	opts := []string{
		"disconnect",
		"--nqn", nqn,
	}

	// The output example:
	// NQN:nqn.2023-01.io.spdk:raid01 disconnected 1 controller(s)
	//
	// And trying to disconnect a non-existing target would return exit code 0
	_, err := executor.Execute(nil, nvmeBinary, opts, types.ExecuteTimeout)
	return err
}

func extractJSONString(str string) (string, error) {
	startIndex := strings.Index(str, "{")
	startIndexBracket := strings.Index(str, "[")

	if startIndex == -1 && startIndexBracket == -1 {
		return "", fmt.Errorf("invalid JSON string")
	}

	if startIndex != -1 && (startIndexBracket == -1 || startIndex < startIndexBracket) {
		endIndex := strings.LastIndex(str, "}")
		if endIndex == -1 {
			return "", fmt.Errorf("invalid JSON string")
		}
		return str[startIndex : endIndex+1], nil
	} else if startIndexBracket != -1 {
		endIndex := strings.LastIndex(str, "]")
		if endIndex == -1 {
			return "", fmt.Errorf("invalid JSON string")
		}
		return str[startIndexBracket : endIndex+1], nil
	}

	return "", fmt.Errorf("invalid JSON string")
}

// GetIPAndPortFromControllerAddress returns the IP and port from the controller address
// Input can be either "traddr=10.42.2.18 trsvcid=20006" or "traddr=10.42.2.18,trsvcid=20006"
func GetIPAndPortFromControllerAddress(address string) (string, string) {
	var traddr, trsvcid string

	parts := strings.FieldsFunc(address, func(r rune) bool {
		return r == ',' || r == ' '
	})

	for _, part := range parts {
		keyVal := strings.Split(part, "=")
		if len(keyVal) == 2 {
			key := strings.TrimSpace(keyVal[0])
			value := strings.TrimSpace(keyVal[1])
			switch key {
			case "traddr":
				traddr = value
			case "trsvcid":
				trsvcid = value
			}
		}
	}

	return traddr, trsvcid
}

func flush(devicePath, namespaceID string, executor *commonns.Executor) (string, error) {

	opts := []string{
		"flush",
		devicePath,
		"-o", "json",
	}

	if namespaceID != "" {
		opts = append(opts, "-n", namespaceID)
	}

	return executor.Execute(nil, nvmeBinary, opts, types.ExecuteTimeout)
}
