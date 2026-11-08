package monitor

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"

	lhns "github.com/longhorn/go-common-libs/ns"
	lhtypes "github.com/longhorn/go-common-libs/types"
	iscsiutil "github.com/longhorn/go-iscsi-helper/util"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
)

func newModeEnvironmentMonitor(t *testing.T, mode types.DataEngineMode, cachedV2 string) *EnvironmentCheckMonitor {
	t.Helper()
	lh, kube := lhfake.NewClientset(), kubefake.NewClientset()
	factories := util.NewInformerFactories("longhorn-system", kube, lh, 0)
	store := datastore.NewDataStoreForNodeLocal("longhorn-system", lh, kube, nil, factories, mode)
	for name, value := range map[types.SettingName]string{types.SettingNameV1DataEngine: "false", types.SettingNameV2DataEngine: cachedV2} {
		if err := factories.LhInformerFactory.Longhorn().V1beta2().Settings().Informer().GetIndexer().Add(&longhorn.Setting{ObjectMeta: metav1.ObjectMeta{Name: string(name), Namespace: "longhorn-system"}, Value: value}); err != nil {
			t.Fatal(err)
		}
	}
	return &EnvironmentCheckMonitor{baseMonitor: &baseMonitor{logger: logrus.New(), ds: store}}
}

func TestV2OnlyLinuxPackageProfilesRetainCommonRequirements(t *testing.T) {
	for _, osImage := range []string{"Ubuntu", "Debian", "CentOS", "Fedora", "Red Hat", "Rocky", "OL", "openSUSE", "Arch", "Gentoo"} {
		for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable, types.DataEngineModeV2Only} {
			probes := requiredPackageProbeExecutables(osImage, mode)
			binaries := map[string]bool{}
			for _, binary := range probes {
				binaries[binary] = true
			}
			if !binaries["mount.nfs"] || !binaries["cryptsetup"] || !binaries["dmsetup"] || binaries["iscsiadm"] != mode.RequiresISCSI() {
				t.Fatalf("OS=%s mode=%s changed common requirements or kept the wrong iSCSI probe: %v", osImage, mode, probes)
			}
		}
	}
	if probes := requiredPackageProbeExecutables("unknown platform", types.DataEngineModeV2Only); probes != nil {
		t.Fatal("unknown OS behavior changed")
	}
}

type recordingPackageExecutor struct {
	run func(string, []string) (string, error)
}

func (e recordingPackageExecutor) Execute(_ []string, binary string, args []string, _ time.Duration) (string, error) {
	return e.run(binary, args)
}

func TestTalosModeOmitsTheEntireISCSIExecutor(t *testing.T) {
	for _, mode := range []types.DataEngineMode{"", types.DataEngineModeV1Capable, types.DataEngineModeV2Only} {
		monitor := newModeEnvironmentMonitor(t, mode, "false")
		processes, binaries := map[string]bool{}, map[string]bool{}
		factory := func(process, directory string, namespaces []lhtypes.Namespace) (environmentPackageExecutor, error) {
			if directory != lhtypes.HostProcDirectory || len(namespaces) != 2 {
				t.Fatal("namespace executor binding changed")
			}
			processes[process] = true
			if process == iscsiutil.ISCSIdProcess {
				return nil, fmt.Errorf("iscsid is absent")
			}
			return recordingPackageExecutor{run: func(binary string, _ []string) (string, error) { binaries[binary] = true; return "", nil }}, nil
		}
		collected := &CollectedEnvironmentCheckInfo{}
		monitor.syncPackagesInstalledTalosLinuxWithExecutor([]lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceNet}, collected, factory)
		condition := types.GetCondition(collected.conditions, longhorn.NodeConditionTypeRequiredPackages)
		if processes[iscsiutil.ISCSIdProcess] != mode.RequiresISCSI() || !processes[lhtypes.ProcessNone] || !processes[lhns.GetDefaultProcessName()] || !binaries["cryptsetup"] || !binaries["dmsetup"] || !binaries["dpkg"] {
			t.Fatalf("mode=%s lost common probes or entered iscsid: processes=%v binaries=%v", mode, processes, binaries)
		}
		want := longhorn.ConditionStatusTrue
		if mode.RequiresISCSI() {
			want = longhorn.ConditionStatusFalse
		}
		if condition.Status != want {
			t.Fatalf("mode=%s lost the required-package condition: %+v", mode, condition)
		}
	}
}

func TestTalosV2OnlyStillReportsCommonPackageFailures(t *testing.T) {
	monitor := newModeEnvironmentMonitor(t, types.DataEngineModeV2Only, "false")
	factory := func(_, _ string, _ []lhtypes.Namespace) (environmentPackageExecutor, error) {
		return recordingPackageExecutor{run: func(binary string, _ []string) (string, error) {
			if binary == "cryptsetup" {
				return "", fmt.Errorf("missing encryption tool")
			}
			return "", nil
		}}, nil
	}
	collected := &CollectedEnvironmentCheckInfo{}
	monitor.syncPackagesInstalledTalosLinuxWithExecutor([]lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceNet}, collected, factory)
	condition := types.GetCondition(collected.conditions, longhorn.NodeConditionTypeRequiredPackages)
	if condition.Status != longhorn.ConditionStatusFalse || !strings.Contains(condition.Message, "cryptsetup") {
		t.Fatalf("V2-only suppressed encryption prerequisites: %+v", condition)
	}
}

func TestV2OnlyRequiresV2HostChecksDespiteStaleOrUnreadableSettings(t *testing.T) {
	for _, value := range []string{"true", "false", "invalid"} {
		for _, mode := range []types.DataEngineMode{types.DataEngineModeV1Capable, types.DataEngineModeV2Only} {
			monitor := newModeEnvironmentMonitor(t, mode, value)
			if got, want := monitor.requiresV2EnvironmentChecks(), mode == types.DataEngineModeV2Only || value == "true"; got != want {
				t.Fatalf("mode=%s cachedV2=%s: V2 host checks=%t want=%t", mode, value, got, want)
			}
		}
	}
}
