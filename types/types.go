package types

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/longhorn/longhorn-manager/util"
)

const (
	LonghornKindNode            = "Node"
	LonghornKindVolume          = "Volume"
	LonghornKindEngineImage     = "EngineImage"
	LonghornKindInstanceManager = "InstanceManager"

	CRDAPIVersionV1alpha1 = "longhorn.rancher.io/v1alpha1"
	CRDAPIVersionV1beta1  = "longhorn.io/v1beta1"
	CurrentCRDAPIVersion  = CRDAPIVersionV1beta1
)

const (
	DefaultAPIPort = 9500

	EngineBinaryDirectoryInContainer = "/engine-binaries/"
	EngineBinaryDirectoryOnHost      = "/var/lib/longhorn/engine-binaries/"
	ReplicaHostPrefix                = "/host"
	EngineBinaryName                 = "longhorn"

	LonghornNodeKey = "longhornnode"
	LonghornDiskKey = "longhorndisk"

	NodeCreateDefaultDiskLabelKey             = "node.longhorn.io/create-default-disk"
	NodeCreateDefaultDiskLabelValueTrue       = "true"
	NodeCreateDefaultDiskLabelValueConfig     = "config"
	KubeNodeDefaultDiskConfigAnnotationKey    = "node.longhorn.io/default-disks-config"
	KubeNodeDefaultNodeTagConfigAnnotationKey = "node.longhorn.io/default-node-tags"

	BaseImageLabel        = "ranchervm-base-image"
	KubernetesStatusLabel = "KubernetesStatus"
	KubernetesReplicaSet  = "ReplicaSet"
	KubernetesStatefulSet = "StatefulSet"
	RecurringJobLabel     = "RecurringJob"

	LonghornLabelKeyPrefix = "longhorn.io"

	LonghornLabelEngineImage          = "engine-image"
	LonghornLabelInstanceManager      = "instance-manager"
	LonghornLabelNode                 = "node"
	LonghornLabelInstanceManagerType  = "instance-manager-type"
	LonghornLabelInstanceManagerImage = "instance-manager-image"
	LonghornLabelVolume               = "longhornvolume"

	KubernetesFailureDomainRegionLabelKey = "failure-domain.beta.kubernetes.io/region"
	KubernetesFailureDomainZoneLabelKey   = "failure-domain.beta.kubernetes.io/zone"
	KubernetesTopologyRegionLabelKey      = "topology.kubernetes.io/region"
	KubernetesTopologyZoneLabelKey        = "topology.kubernetes.io/zone"

	LonghornDriverName = "driver.longhorn.io"

	DeprecatedProvisionerName = "rancher.io/longhorn"
	DepracatedDriverName      = "io.rancher.longhorn"
)

const (
	CSIMinVersion                = "v1.14.0"
	CSIVolumeExpansionMinVersion = "v1.16.0"
	CSISnapshotterMinVersion     = "v1.17.0"

	KubernetesTopologyLabelsVersion = "v1.17.0"
)

type ReplicaMode string

const (
	ReplicaModeRW  = ReplicaMode("RW")
	ReplicaModeWO  = ReplicaMode("WO")
	ReplicaModeERR = ReplicaMode("ERR")

	EnvNodeName       = "NODE_NAME"
	EnvPodNamespace   = "POD_NAMESPACE"
	EnvPodIP          = "POD_IP"
	EnvServiceAccount = "SERVICE_ACCOUNT"

	AWSAccessKey = "AWS_ACCESS_KEY_ID"
	AWSSecretKey = "AWS_SECRET_ACCESS_KEY"
	AWSEndPoint  = "AWS_ENDPOINTS"
	AWSCert      = "AWS_CERT"

	HTTPSProxy = "HTTPS_PROXY"
	HTTPProxy  = "HTTP_PROXY"
	NOProxy    = "NO_PROXY"

	VirtualHostedStyle = "VIRTUAL_HOSTED_STYLE"

	OptionFromBackup          = "fromBackup"
	OptionNumberOfReplicas    = "numberOfReplicas"
	OptionStaleReplicaTimeout = "staleReplicaTimeout"
	OptionBaseImage           = "baseImage"
	OptionFrontend            = "frontend"
	OptionDiskSelector        = "diskSelector"
	OptionNodeSelector        = "nodeSelector"

	// DefaultStaleReplicaTimeout in minutes. 48h by default
	DefaultStaleReplicaTimeout = "2880"

	ImageChecksumNameLength = 8
)

type NotFoundError struct {
	Name string
}

func (e *NotFoundError) Error() string {
	return fmt.Sprintf("cannot find %v", e.Name)
}

const (
	engineSuffix    = "-e"
	replicaSuffix   = "-r"
	recurringSuffix = "-c"

	// MaximumJobNameSize is calculated using
	// 1. NameMaximumLength is 40
	// 2. Recurring suffix is 2
	// 3. Maximum kubernetes name length is 63
	// 4. cronjob pod suffix is 11
	// 5. Dash and buffer for 2
	MaximumJobNameSize = 8

	engineImagePrefix          = "ei-"
	instanceManagerImagePrefix = "imi-"

	diskNamePrefix = "disk-"

	instanceManagerPrefix = "instance-manager-"
	engineManagerPrefix   = instanceManagerPrefix + "e-"
	replicaManagerPrefix  = instanceManagerPrefix + "r-"
)

func GenerateEngineNameForVolume(vName string) string {
	return vName + engineSuffix + "-" + util.RandomID()
}

func GenerateReplicaNameForVolume(vName string) string {
	return vName + replicaSuffix + "-" + util.RandomID()
}

func GetCronJobNameForVolumeAndJob(vName, job string) string {
	return vName + "-" + job + recurringSuffix
}

func GetAPIServerAddressFromIP(ip string) string {
	return net.JoinHostPort(ip, strconv.Itoa(DefaultAPIPort))
}

func GetDefaultManagerURL() string {
	return "http://longhorn-backend:" + strconv.Itoa(DefaultAPIPort) + "/v1"
}

func GetImageCanonicalName(image string) string {
	return strings.Replace(strings.Replace(image, ":", "-", -1), "/", "-", -1)
}

func GetEngineBinaryDirectoryOnHostForImage(image string) string {
	cname := GetImageCanonicalName(image)
	return filepath.Join(EngineBinaryDirectoryOnHost, cname)
}

func GetEngineBinaryDirectoryForEngineManagerContainer(image string) string {
	cname := GetImageCanonicalName(image)
	return filepath.Join(EngineBinaryDirectoryInContainer, cname)
}

func GetEngineBinaryDirectoryForReplicaManagerContainer(image string) string {
	cname := GetImageCanonicalName(image)
	return filepath.Join(filepath.Join(ReplicaHostPrefix, EngineBinaryDirectoryOnHost), cname)
}

func EngineBinaryExistOnHostForImage(image string) bool {
	st, err := os.Stat(filepath.Join(GetEngineBinaryDirectoryOnHostForImage(image), "longhorn"))
	return err == nil && !st.IsDir()
}

var (
	LonghornSystemKey = "longhorn"
)

func GetLonghornLabelKey(name string) string {
	return fmt.Sprintf("%s/%s", LonghornLabelKeyPrefix, name)
}

func GetLonghornLabelComponentKey() string {
	return GetLonghornLabelKey("component")
}

func GetEngineImageLabels(engineImageName string) map[string]string {
	return map[string]string{
		GetLonghornLabelComponentKey():                LonghornLabelEngineImage,
		GetLonghornLabelKey(LonghornLabelEngineImage): engineImageName,
	}
}

func GetInstanceManagerLabels(node, instanceManagerImage string, managerType InstanceManagerType) map[string]string {
	labels := map[string]string{
		GetLonghornLabelComponentKey():                        LonghornLabelInstanceManager,
		GetLonghornLabelKey(LonghornLabelInstanceManagerType): string(managerType),
	}
	if node != "" {
		labels[GetLonghornLabelKey(LonghornLabelNode)] = node
	}
	if instanceManagerImage != "" {
		labels[GetLonghornLabelKey(LonghornLabelInstanceManagerImage)] = GetInstanceManagerImageChecksumName(GetImageCanonicalName(instanceManagerImage))
	}

	return labels
}

func GetInstanceManagerComponentLabel() map[string]string {
	return map[string]string{
		GetLonghornLabelComponentKey(): LonghornLabelInstanceManager,
	}
}

func GetVolumeLabels(volumeName string) map[string]string {
	return map[string]string{
		LonghornLabelVolume: volumeName,
	}
}

func GetRegionAndZone(labels map[string]string, isUsingTopologyLabels bool) (string, string) {
	region := ""
	zone := ""
	if isUsingTopologyLabels {
		if v, ok := labels[KubernetesTopologyRegionLabelKey]; ok {
			region = v
		}
		if v, ok := labels[KubernetesTopologyZoneLabelKey]; ok {
			zone = v
		}
	} else {
		if v, ok := labels[KubernetesFailureDomainRegionLabelKey]; ok {
			region = v
		}
		if v, ok := labels[KubernetesFailureDomainZoneLabelKey]; ok {
			zone = v
		}
	}
	return region, zone
}

func GetEngineImageChecksumName(image string) string {
	return engineImagePrefix + util.GetStringChecksum(strings.TrimSpace(image))[:ImageChecksumNameLength]
}

func GetInstanceManagerImageChecksumName(image string) string {
	return instanceManagerImagePrefix + util.GetStringChecksum(strings.TrimSpace(image))[:ImageChecksumNameLength]
}

func ValidateEngineImageChecksumName(name string) bool {
	matched, _ := regexp.MatchString(fmt.Sprintf("^%s[a-fA-F0-9]{%d}$", engineImagePrefix, ImageChecksumNameLength), name)
	return matched
}

func GetInstanceManagerName(imType InstanceManagerType) (string, error) {
	switch imType {
	case InstanceManagerTypeEngine:
		return engineManagerPrefix + util.RandomID(), nil
	case InstanceManagerTypeReplica:
		return replicaManagerPrefix + util.RandomID(), nil
	}
	return "", fmt.Errorf("cannot generate name for unknown instance manager type %v", imType)
}

func GetInstanceManagerPrefix(imType InstanceManagerType) string {
	switch imType {
	case InstanceManagerTypeEngine:
		return engineManagerPrefix
	case InstanceManagerTypeReplica:
		return replicaManagerPrefix
	}
	return ""
}

func GetReplicaMountedDataPath(dataPath string) string {
	if !strings.HasPrefix(dataPath, ReplicaHostPrefix) {
		return filepath.Join(ReplicaHostPrefix, dataPath)
	}
	return dataPath
}

func ErrorIsNotFound(err error) bool {
	return strings.Contains(err.Error(), "cannot find")
}

func ErrorAlreadyExists(err error) bool {
	return strings.Contains(err.Error(), "already exists")
}

func ValidateReplicaCount(count int) error {
	if count < 1 || count > 20 {
		return fmt.Errorf("replica count value must between 1 to 20")
	}
	return nil
}

func ValidateDataLocality(mode DataLocality) error {
	if mode != DataLocalityDisabled && mode != DataLocalityBestEffort {
		return fmt.Errorf("invalid data locality mode: %v", mode)
	}
	return nil
}

func GetDaemonSetNameFromEngineImageName(engineImageName string) string {
	return "engine-image-" + engineImageName
}

func GetEngineImageNameFromDaemonSetName(dsName string) string {
	return strings.TrimPrefix(dsName, "engine-image-")
}

func LabelsToString(labels map[string]string) string {
	res := ""
	for k, v := range labels {
		res += fmt.Sprintf("%s=%s,", k, v)
	}
	res = strings.TrimSuffix(res, ",")
	return res
}

func GetNodeTagsFromAnnotation(annotation string) ([]string, error) {
	nodeTags, err := UnmarshalToNodeTags(annotation)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal the node tag annotation")
	}
	validNodeTags, err := util.ValidateTags(nodeTags)
	if err != nil {
		return nil, err
	}

	return validNodeTags, nil
}

type DiskInput struct {
	Path string
	DiskSpec
}

// UnmarshalToDiskSpecs input format should be:
// `[{"path":"/mnt/disk1","allowScheduling":false},
//   {"path":"/mnt/disk2","allowScheduling":false,"storageReserved":1024,"tags":["ssd","fast"]}]`
func UnmarshalToDiskSpecs(s string) (ret []DiskInput, err error) {
	if err := json.Unmarshal([]byte(s), &ret); err != nil {
		return nil, err
	}
	return ret, nil
}

// UnmarshalToNodeTags input format should be:
// `["worker1","enabled"]`
func UnmarshalToNodeTags(s string) ([]string, error) {
	var res []string
	if err := json.Unmarshal([]byte(s), &res); err != nil {
		return nil, err
	}
	return res, nil
}
