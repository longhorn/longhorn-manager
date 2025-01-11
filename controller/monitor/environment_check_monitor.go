package monitor

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/jinzhu/copier"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"k8s.io/apimachinery/pkg/util/wait"

	corev1 "k8s.io/api/core/v1"

	lhexec "github.com/longhorn/go-common-libs/exec"
	lhnfs "github.com/longhorn/go-common-libs/nfs"
	lhns "github.com/longhorn/go-common-libs/ns"
	lhsys "github.com/longhorn/go-common-libs/sys"
	lhtypes "github.com/longhorn/go-common-libs/types"
	iscsiutil "github.com/longhorn/go-iscsi-helper/util"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

const (
	EnvironmentCheckMonitorSyncPeriod = 1800 * time.Second

	kernelConfigDir = "/host/boot/"
	systemConfigDir = "/host/etc/"
)

var (
	kernelModules       = map[string]string{"CONFIG_DM_CRYPT": "dm_crypt"}
	kernelModulesV2     = map[string]string{"CONFIG_VFIO_PCI": "vfio_pci", "CONFIG_UIO_PCI_GENERIC": "uio_pci_generic", "CONFIG_NVME_TCP": "nvme_tcp"}
	nfsClientVersions   = map[string]string{"CONFIG_NFS_V4_2": "nfs", "CONFIG_NFS_V4_1": "nfs", "CONFIG_NFS_V4": "nfs"}
	nfsProtocolVersions = map[string]bool{"4.0": true, "4.1": true, "4.2": true}
)

type EnvironmentCheckMonitor struct {
	*baseMonitor

	nodeName string

	collectedDataLock sync.RWMutex
	collectedData     *CollectedEnvironmentCheckInfo

	syncCallback func(key string)
}

type CollectedEnvironmentCheckInfo struct {
	conditions []longhorn.Condition
}

func NewEnvironmentCheckMonitor(logger logrus.FieldLogger, ds *datastore.DataStore, nodeName string, syncCallback func(key string)) (*EnvironmentCheckMonitor, error) {
	ctx, quit := context.WithCancel(context.Background())

	m := &EnvironmentCheckMonitor{
		baseMonitor: newBaseMonitor(ctx, quit, logger, ds, NodeMonitorSyncPeriod),

		nodeName: nodeName,

		collectedDataLock: sync.RWMutex{},
		collectedData:     &CollectedEnvironmentCheckInfo{},

		syncCallback: syncCallback,
	}

	go m.Start()

	return m, nil
}

func (m *EnvironmentCheckMonitor) Start() {
	if err := wait.PollUntilContextCancel(m.ctx, m.syncPeriod, false, func(context.Context) (bool, error) {
		if err := m.run(struct{}{}); err != nil {
			m.logger.WithError(err).Error("Stopped monitoring environment check")
		}
		return false, nil
	}); err != nil {
		m.logger.WithError(err).Error("Failed to start monitoring environment check")
	}
}

func (m *EnvironmentCheckMonitor) Stop() {
	m.quit()
}

func (m *EnvironmentCheckMonitor) RunOnce() error {
	return m.run(struct{}{})
}

func (m *EnvironmentCheckMonitor) UpdateConfiguration(map[string]interface{}) error {
	return nil
}

func (m *EnvironmentCheckMonitor) GetCollectedData() (interface{}, error) {
	m.collectedDataLock.RLock()
	defer m.collectedDataLock.RUnlock()

	data := []longhorn.Condition{}
	if err := copier.CopyWithOption(&data, &m.collectedData.conditions, copier.Option{IgnoreEmpty: true, DeepCopy: true}); err != nil {
		return data, errors.Wrap(err, "failed to copy collected data")
	}

	return data, nil
}

func (m *EnvironmentCheckMonitor) run(value interface{}) error {
	node, err := m.ds.GetNode(m.nodeName)
	if err != nil {
		return errors.Wrapf(err, "failed to get longhorn node %v", m.nodeName)
	}

	collectedData := m.collectEnvironmentCheckData(node)
	if !reflect.DeepEqual(m.collectedData, collectedData) {
		func() {
			m.collectedDataLock.Lock()
			defer m.collectedDataLock.Unlock()
			m.collectedData = collectedData
		}()

		key := node.Namespace + "/" + m.nodeName
		m.syncCallback(key)
	}

	return nil
}

func (m *EnvironmentCheckMonitor) collectEnvironmentCheckData(node *longhorn.Node) *CollectedEnvironmentCheckInfo {
	kubeNode, err := m.ds.GetKubernetesNodeRO(node.Name)
	if err != nil {
		return &CollectedEnvironmentCheckInfo{
			conditions: []longhorn.Condition{},
		}
	}

	return m.environmentCheck(kubeNode)
}

func (m *EnvironmentCheckMonitor) environmentCheck(kubeNode *corev1.Node) *CollectedEnvironmentCheckInfo {
	collectedData := &CollectedEnvironmentCheckInfo{
		conditions: []longhorn.Condition{},
	}

	// Need to find the better way to check if various kernel versions are supported
	namespaces := []lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceNet}
	m.syncPackagesInstalled(kubeNode, namespaces, collectedData)
	m.syncMultipathd(namespaces, collectedData)
	m.syncNFSClientVersion(kubeNode, collectedData)

	isV2DataEngine, err := m.ds.GetSettingAsBool(types.SettingNameV2DataEngine)
	if err != nil {
		m.logger.WithError(err).Debug("Failed to fetch v2-data-engine setting")
		isV2DataEngine = false
	}

	m.checkKernelModulesLoaded(kubeNode, isV2DataEngine, collectedData)

	return collectedData
}

func (m *EnvironmentCheckMonitor) syncPackagesInstalled(kubeNode *corev1.Node, namespaces []lhtypes.Namespace, collectedData *CollectedEnvironmentCheckInfo) {
	osImage := strings.ToLower(kubeNode.Status.NodeInfo.OSImage)
	queryPackagesCmd := ""
	options := []string{}
	packages := []string{}
	pipeFlag := false

	switch {
	case strings.Contains(osImage, "talos"):
		m.syncPackagesInstalledTalosLinux(namespaces, collectedData)
		return
	case strings.Contains(osImage, "ubuntu"):
		fallthrough
	case strings.Contains(osImage, "debian"):
		queryPackagesCmd = "dpkg"
		options = append(options, "-l")
		packages = append(packages, "nfs-common", "open-iscsi", "cryptsetup", "dmsetup")
		pipeFlag = true
	case strings.Contains(osImage, "centos"):
		fallthrough
	case strings.Contains(osImage, "fedora"):
		fallthrough
	case strings.Contains(osImage, "red hat"):
		fallthrough
	case strings.Contains(osImage, "rocky"):
		fallthrough
	case strings.Contains(osImage, "ol"):
		queryPackagesCmd = "rpm"
		options = append(options, "-q")
		packages = append(packages, "nfs-utils", "iscsi-initiator-utils", "cryptsetup", "device-mapper")
	case strings.Contains(osImage, "suse"):
		queryPackagesCmd = "rpm"
		options = append(options, "-q")
		packages = append(packages, "nfs-client", "open-iscsi", "cryptsetup", "device-mapper")
	case strings.Contains(osImage, "arch"):
		queryPackagesCmd = "pacman"
		options = append(options, "-Q")
		packages = append(packages, "nfs-utils", "open-iscsi", "cryptsetup", "device-mapper")
	case strings.Contains(osImage, "gentoo"):
		queryPackagesCmd = "qlist"
		options = append(options, "-I")
		packages = append(packages, "net-fs/nfs-utils", "sys-block/open-iscsi", "sys-fs/cryptsetup", "sys-fs/lvm2")
	default:
		// Skip environment check condition when
		// 1.The OS is unknown to Longhorn
		// 2.The OS that already has all packages installed such as Harvester
		return
	}

	nsexec, err := lhns.NewNamespaceExecutor(lhtypes.ProcessNone, lhtypes.HostProcDirectory, namespaces)
	if err != nil {
		collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeRequiredPackages, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonNamespaceExecutorErr),
			fmt.Sprintf("Failed to get namespace executor: %v", err.Error()))
		return
	}

	notFoundPkgs := []string{}
	for _, pkg := range packages {
		args := options
		if !pipeFlag {
			args = append(args, pkg)
		}
		queryResult, err := nsexec.Execute(nil, queryPackagesCmd, args, lhtypes.ExecuteDefaultTimeout)
		if err != nil {
			m.logger.WithError(err).Debugf("Package %v is not found", pkg)
			notFoundPkgs = append(notFoundPkgs, pkg)
			continue
		}
		if pipeFlag {
			if _, err := lhexec.NewExecutor().ExecuteWithStdinPipe("grep", []string{"-w", pkg}, queryResult, lhtypes.ExecuteDefaultTimeout); err != nil {
				m.logger.WithError(err).Debugf("Package %v is not found", pkg)
				notFoundPkgs = append(notFoundPkgs, pkg)
				continue
			}
		}
	}

	if len(notFoundPkgs) > 0 {
		collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeRequiredPackages, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonPackagesNotInstalled),
			fmt.Sprintf("Missing packages: %v", notFoundPkgs))
		return
	}

	collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeRequiredPackages, longhorn.ConditionStatusTrue, "",
		fmt.Sprintf("All required packages %v are installed", packages))
}

func (m *EnvironmentCheckMonitor) syncPackagesInstalledTalosLinux(namespaces []lhtypes.Namespace, collectedData *CollectedEnvironmentCheckInfo) {
	type validateCommand struct {
		binary string
		args   []string
	}

	packagesIsInstalled := map[string]bool{}

	// Helper function to validate packages within a namespace and update node
	// status if there is an error.
	validatePackages := func(process string, binaryToValidateCommand map[string]validateCommand) (ok bool) {
		nsexec, err := lhns.NewNamespaceExecutor(process, lhtypes.HostProcDirectory, namespaces)
		if err != nil {
			collectedData.conditions = types.SetCondition(
				collectedData.conditions, longhorn.NodeConditionTypeRequiredPackages, longhorn.ConditionStatusFalse,
				string(longhorn.NodeConditionReasonNamespaceExecutorErr), fmt.Sprintf("Failed to get namespace executor: %v", err.Error()),
			)
			return false
		}

		for binary, command := range binaryToValidateCommand {
			_, err := nsexec.Execute(nil, command.binary, command.args, lhtypes.ExecuteDefaultTimeout)
			if err != nil {
				m.logger.WithError(err).Debugf("Package %v is not found", binary)
				packagesIsInstalled[binary] = false
			} else {
				packagesIsInstalled[binary] = true
			}
		}
		return true
	}

	// The validation commands by process.
	hostPackageToValidateCmd := map[string]validateCommand{
		"cryptsetup": {binary: "cryptsetup", args: []string{"--version"}},
		"dmsetup":    {binary: "dmsetup", args: []string{"--version"}},
	}
	kubeletPackageToValidateCmd := map[string]validateCommand{
		"nfs-common": {binary: "dpkg", args: []string{"-s", "nfs-common"}},
	}
	iscsiPackageToValidateCmd := map[string]validateCommand{
		"iscsiadm": {binary: "iscsiadm", args: []string{"--version"}},
	}

	// Check each set of packagesl return immediately if there is an error.
	if !validatePackages(lhtypes.ProcessNone, hostPackageToValidateCmd) ||
		!validatePackages(lhns.GetDefaultProcessName(), kubeletPackageToValidateCmd) ||
		!validatePackages(iscsiutil.ISCSIdProcess, iscsiPackageToValidateCmd) {
		return
	}

	// Organize the installed and not installed packages.
	installedPackages := []string{}
	notInstalledPackages := []string{}
	for binary, isInstalled := range packagesIsInstalled {
		if isInstalled {
			installedPackages = append(installedPackages, binary)
		} else {
			notInstalledPackages = append(notInstalledPackages, binary)
		}
	}

	// Update node condition based on  packages installed status.
	if len(notInstalledPackages) > 0 {
		collectedData.conditions = types.SetCondition(
			collectedData.conditions, longhorn.NodeConditionTypeRequiredPackages, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonPackagesNotInstalled), fmt.Sprintf("Missing packages: %v", notInstalledPackages),
		)
	} else {
		collectedData.conditions = types.SetCondition(
			collectedData.conditions, longhorn.NodeConditionTypeRequiredPackages, longhorn.ConditionStatusTrue,
			"", fmt.Sprintf("All required packages %v are installed", installedPackages),
		)
	}
}

func (m *EnvironmentCheckMonitor) syncMultipathd(namespaces []lhtypes.Namespace, collectedData *CollectedEnvironmentCheckInfo) {
	nsexec, err := lhns.NewNamespaceExecutor(lhtypes.ProcessNone, lhtypes.HostProcDirectory, namespaces)
	if err != nil {
		collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeMultipathd, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonNamespaceExecutorErr),
			fmt.Sprintf("Failed to get namespace executor: %v", err.Error()))
		return
	}
	args := []string{"show", "status"}
	if result, _ := nsexec.Execute(nil, "multipathd", args, lhtypes.ExecuteDefaultTimeout); result != "" {
		collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeMultipathd, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonMultipathdIsRunning),
			"multipathd is running with a known issue that affects Longhorn. See description and solution at https://longhorn.io/kb/troubleshooting-volume-with-multipath")
		return
	}

	collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeMultipathd, longhorn.ConditionStatusTrue, "", "")
}

func (m *EnvironmentCheckMonitor) checkKernelModulesLoaded(kubeNode *corev1.Node, isV2DataEngine bool, collectedData *CollectedEnvironmentCheckInfo) {
	modulesToCheck := make(map[string]string)
	for k, v := range kernelModules {
		modulesToCheck[k] = v
	}

	if isV2DataEngine {
		for k, v := range kernelModulesV2 {
			modulesToCheck[k] = v
		}
	}

	notFoundModulesUsingkmod, err := checkModulesLoadedUsingkmod(modulesToCheck)
	if err != nil {
		collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeKernelModulesLoaded, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonNamespaceExecutorErr),
			fmt.Sprintf("Failed to check kernel modules: %v", err.Error()))
		return
	}

	if len(notFoundModulesUsingkmod) == 0 {
		collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeKernelModulesLoaded, longhorn.ConditionStatusTrue, "",
			fmt.Sprintf("Kernel modules %v are loaded", getModulesConfigsList(modulesToCheck, false)))
		return
	}

	notLoadedModules, err := m.checkModulesLoadedByConfigFile(notFoundModulesUsingkmod, kubeNode.Status.NodeInfo.KernelVersion)
	if err != nil {
		collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeKernelModulesLoaded, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonCheckKernelConfigFailed),
			fmt.Sprintf("Failed to check kernel config file for kernel modules %v: %v", notFoundModulesUsingkmod, err.Error()))
		return
	}

	if len(notLoadedModules) != 0 {
		collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeKernelModulesLoaded, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonKernelModulesNotLoaded),
			fmt.Sprintf("Kernel modules %v are not loaded", notLoadedModules))
		return
	}

	collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeKernelModulesLoaded, longhorn.ConditionStatusTrue, "",
		fmt.Sprintf("Kernel modules %v are loaded", getModulesConfigsList(modulesToCheck, false)))
}

func checkModulesLoadedUsingkmod(modules map[string]string) (map[string]string, error) {
	kmodResult, err := lhexec.NewExecutor().Execute(nil, "kmod", []string{"list"}, lhtypes.ExecuteDefaultTimeout)
	if err != nil {
		return nil, err
	}

	notFoundModules := map[string]string{}
	for config, module := range modules {
		if !strings.Contains(kmodResult, module) {
			notFoundModules[config] = module
		}
	}

	return notFoundModules, nil
}

func (m *EnvironmentCheckMonitor) checkModulesLoadedByConfigFile(modules map[string]string, kernelVersion string) ([]string, error) {
	kernelConfigMap, err := lhsys.GetBootKernelConfigMap(kernelConfigDir, kernelVersion)
	if err != nil {
		if kernelConfigMap, err = lhsys.GetProcKernelConfigMap(lhtypes.HostProcDirectory); err != nil {
			return nil, err
		}
	}

	notLoadedModules := []string{}
	for config, module := range modules {
		moduleEnabled, err := m.checkKernelModuleEnabled(config, module, kernelConfigMap)
		if err != nil {
			return nil, err
		}
		if !moduleEnabled {
			notLoadedModules = append(notLoadedModules, module)
		}
	}

	return notLoadedModules, nil
}

func (m *EnvironmentCheckMonitor) checkNFSMountConfigFile(supported map[string]bool, configFilePathPrefix string) (actualDefaultVer string, isAllowed bool, err error) {
	var nfsVer string
	nfsMajor, nfsMinor, err := lhnfs.GetSystemDefaultNFSVersion(configFilePathPrefix)
	if err == nil {
		nfsVer = fmt.Sprintf("%d.%d", nfsMajor, nfsMinor)
		actualDefaultVer = nfsVer
	} else if errors.Is(err, lhtypes.ErrNotConfigured) {
		m.logger.Debugf("NFS default version is 4 since the nfsmount.conf is absent under %s", configFilePathPrefix)
		nfsVer = "4.0"
		actualDefaultVer = ""
	} else {
		return "", false, errors.Wrap(err, "failed to check NFS default mount configurations")
	}
	return actualDefaultVer, supported[nfsVer], nil
}

func (m *EnvironmentCheckMonitor) checkKernelModuleEnabled(module, kmodName string, kernelConfigMap map[string]string) (bool, error) {
	enabled, exists := kernelConfigMap[module]
	if !exists {
		return false, nil
	}

	switch enabled {
	case "y":
		return true, nil
	case "m":
		kmodResult, err := lhexec.NewExecutor().Execute(nil, "kmod", []string{"list"}, lhtypes.ExecuteDefaultTimeout)
		if err != nil {
			return false, errors.Wrap(err, "Failed to execute command `kmod`")
		}
		if strings.Contains(kmodResult, kmodName) {
			return true, nil
		}
	default:
		m.logger.Debugf("Unknown kernel config value for %v: %v", module, enabled)
	}

	return false, nil
}

func getModulesConfigsList(modulesMap map[string]string, needModules bool) []string {
	modulesConfigs := []string{}
	for mod, config := range modulesMap {
		appendingObj := config
		if needModules {
			appendingObj = mod
		}
		modulesConfigs = append(modulesConfigs, appendingObj)
	}
	return modulesConfigs
}

func (m *EnvironmentCheckMonitor) syncNFSClientVersion(kubeNode *corev1.Node, collectedData *CollectedEnvironmentCheckInfo) {
	notLoadedModules, err := m.checkModulesLoadedByConfigFile(nfsClientVersions, kubeNode.Status.NodeInfo.KernelVersion)
	if err != nil {
		collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeNFSClientInstalled, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonCheckKernelConfigFailed),
			fmt.Sprintf("Failed to check kernel config file for kernel modules %v: %v", nfsClientVersions, err.Error()))
		return
	}

	if len(notLoadedModules) == len(nfsClientVersions) {
		collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeNFSClientInstalled, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonNFSClientIsNotFound),
			fmt.Sprintf("NFS clients %v not found. At least one should be enabled", getModulesConfigsList(nfsClientVersions, true)))
		return
	}

	protocolVer, isAllowed, err := m.checkNFSMountConfigFile(nfsProtocolVersions, systemConfigDir)
	if err != nil {
		collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeNFSClientInstalled, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonNFSClientIsMisconfigured),
			fmt.Sprintf("Failed to check NFS clients default protocol version: %v", err.Error()))
		return
	} else if !isAllowed {
		collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeNFSClientInstalled, longhorn.ConditionStatusFalse,
			string(longhorn.NodeConditionReasonNFSClientIsMisconfigured),
			fmt.Sprintf("NFS clients default protocol version is %v, which is not supported", protocolVer))
		return
	}

	collectedData.conditions = types.SetCondition(collectedData.conditions, longhorn.NodeConditionTypeNFSClientInstalled, longhorn.ConditionStatusTrue, "", "")
}
