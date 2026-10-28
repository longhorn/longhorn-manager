package initiator

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"syscall"

	"github.com/cockroachdb/errors"
)

// sysfsRoot is read directly, not through the executor: the NVMe entries of sysfs are the same in every mount namespace.
var sysfsRoot = "/sys"

var (
	nvmeControllerNamePattern = regexp.MustCompile(`^nvme\d+$`)
	nvmeNamespaceNamePattern  = regexp.MustCompile(`^nvme\d+n\d+$`)
	nvmeHeadNamePattern       = regexp.MustCompile(`^nvme(\d+)n(\d+)$`)
	nvmePathNamePattern       = regexp.MustCompile(`^nvme(\d+)c\d+n(\d+)$`)
)

// listRecognizedNvmeDevicesFromSysfs returns what nvme list does: every namespace device,
// those of a subsystem before those of its controllers. A device without a usable path is
// left out, as nvme-cli fails to identify it.
func listRecognizedNvmeDevicesFromSysfs(sysRoot string) ([]CliDevice, error) {
	subsystems, err := readSubsystemsFromSysfs(sysRoot)
	if err != nil {
		return nil, err
	}
	devices := []CliDevice{}
	for _, s := range subsystems {
		namespaces := slices.Clone(s.heads)
		for _, c := range s.controllers {
			namespaces = append(namespaces, s.owned[c.Controller]...)
		}
		for _, ns := range namespaces {
			if !ns.usable {
				continue
			}
			devices = append(devices, CliDevice{
				NameSpace:    ns.namespace.NSID,
				DevicePath:   "/dev/" + ns.namespace.NameSpace,
				UsedBytes:    ns.namespace.UsedBytes,
				MaximumLBA:   ns.namespace.MaximumLBA,
				PhysicalSize: ns.namespace.PhysicalSize,
				SectorSize:   ns.namespace.SectorSize,
			})
		}
	}
	return devices, nil
}

// listSubsystemsFromSysfs returns what nvme list-subsys does: every subsystem with its
// controllers, or with a device path only its subsystem with the controllers that reach it.
func listSubsystemsFromSysfs(sysRoot, devicePath string) ([]Subsystem, error) {
	if devicePath == "" {
		subsystems, err := readSubsystemsFromSysfs(sysRoot)
		if err != nil {
			return nil, err
		}
		result := []Subsystem{}
		for _, s := range subsystems {
			if len(s.controllers) > 0 {
				result = append(result, s.subsystem(s.controllers))
			}
		}
		return result, nil
	}

	name := filepath.Base(devicePath)
	s, err := readSubsystemOfDeviceFromSysfs(sysRoot, name)
	if err != nil || s == nil {
		return []Subsystem{}, err
	}
	controllers := []Controller{}
	for _, head := range s.heads {
		if head.namespace.NameSpace == name {
			controllers = s.controllersWithPathTo(head)
		}
	}
	for _, c := range s.controllers {
		if slices.ContainsFunc(s.owned[c.Controller], func(ns sysfsNamespace) bool { return ns.namespace.NameSpace == name }) {
			controllers = []Controller{c}
		}
	}
	// nvme-cli leaves out a subsystem without a controller to report.
	if len(controllers) == 0 {
		return []Subsystem{}, nil
	}
	return []Subsystem{s.subsystem(controllers)}, nil
}

// readSubsystemOfDeviceFromSysfs returns nil when no subsystem has the device. The name
// of a head carries its subsystem number and that of a device without native multipath
// its controller number, so the subsystem is found without reading all of them.
func readSubsystemOfDeviceFromSysfs(sysRoot, name string) (*sysfsSubsystem, error) {
	classDir := filepath.Join(sysRoot, "class", "nvme-subsystem")
	if m := nvmeHeadNamePattern.FindStringSubmatch(name); m != nil {
		s, err := readSubsystemDir(classDir, "nvme-subsys"+m[1], "")
		if err != nil {
			return nil, err
		}
		if s != nil && s.hasDevice(name) {
			if s.nqn, err = readSysfsAttribute(filepath.Join(classDir, s.name, "subsysnqn")); err != nil {
				return nil, err
			}
			return s, nil
		}
		nqn, err := readSysfsAttribute(filepath.Join(sysRoot, "class", "nvme", "nvme"+m[1], "subsysnqn"))
		if err == nil {
			if s, err = readSubsystemFromSysfs(sysRoot, nqn); err != nil {
				return nil, err
			}
			if s != nil && s.hasDevice(name) {
				return s, nil
			}
		} else if !isSysfsEntryGone(err) {
			return nil, err
		}
	}

	subsystems, err := readSubsystemsFromSysfs(sysRoot)
	if err != nil {
		return nil, err
	}
	for _, s := range subsystems {
		if s.hasDevice(name) {
			return s, nil
		}
	}
	return nil, nil
}

func readSubsystemsFromSysfs(sysRoot string) ([]*sysfsSubsystem, error) {
	classDir := filepath.Join(sysRoot, "class", "nvme-subsystem")
	entries, err := os.ReadDir(classDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	subsystems := []*sysfsSubsystem{}
	for _, entry := range entries {
		nqn, err := readSysfsAttribute(filepath.Join(classDir, entry.Name(), "subsysnqn"))
		if err != nil {
			if isSysfsEntryGone(err) {
				continue
			}
			return nil, err
		}
		s, err := readSubsystemDir(classDir, entry.Name(), nqn)
		if err != nil {
			return nil, err
		}
		if s != nil {
			subsystems = append(subsystems, s)
		}
	}
	return subsystems, nil
}

type sysfsSubsystem struct {
	name        string
	nqn         string
	controllers []Controller
	// heads are the namespace devices of native multipath, shared by every controller.
	heads []sysfsNamespace
	// owned are the namespace devices without native multipath, by controller name.
	owned map[string][]sysfsNamespace
	// paths are the hidden per-path devices of native multipath, by controller name.
	paths map[string][]sysfsNamespace
}

type sysfsNamespace struct {
	namespace Namespace
	usable    bool
	// pathOrder lists the paths of a head from its multipath directory (Linux 6.15+).
	pathOrder []string
}

// controllersWithPathTo returns the controllers nvme-cli list-subsys reports for a head, all when none has a path.
func (s *sysfsSubsystem) controllersWithPathTo(head sysfsNamespace) []Controller {
	controllers := []Controller{}
	// libnvme follows the multipath directory when the kernel has one.
	for _, pathName := range head.pathOrder {
		for _, c := range s.controllers {
			if slices.ContainsFunc(s.paths[c.Controller], func(p sysfsNamespace) bool { return p.namespace.NameSpace == pathName }) {
				controllers = append(controllers, c)
				break
			}
		}
	}
	if len(controllers) > 0 {
		return controllers
	}

	headMatch := nvmeHeadNamePattern.FindStringSubmatch(head.namespace.NameSpace)
	for _, c := range s.controllers {
		for _, path := range s.paths[c.Controller] {
			pathMatch := nvmePathNamePattern.FindStringSubmatch(path.namespace.NameSpace)
			if headMatch != nil && pathMatch != nil && pathMatch[1] == headMatch[1] && pathMatch[2] == headMatch[2] {
				controllers = append(controllers, c)
				break
			}
		}
	}
	if len(controllers) == 0 {
		return s.controllers
	}
	return controllers
}

func (s *sysfsSubsystem) subsystem(controllers []Controller) Subsystem {
	paths := []Path{}
	for _, c := range controllers {
		paths = append(paths, Path{Name: c.Controller, Transport: c.Transport, Address: c.Address, State: c.State})
	}
	return Subsystem{Name: s.name, NQN: s.nqn, Paths: paths}
}

func (s *sysfsSubsystem) hasDevice(name string) bool {
	isDevice := func(ns sysfsNamespace) bool { return ns.namespace.NameSpace == name }
	if slices.ContainsFunc(s.heads, isDevice) {
		return true
	}
	for _, namespaces := range s.owned {
		if slices.ContainsFunc(namespaces, isDevice) {
			return true
		}
	}
	return false
}

// readSubsystemFromSysfs returns nil when no subsystem has the NQN.
func readSubsystemFromSysfs(sysRoot, nqn string) (*sysfsSubsystem, error) {
	classDir := filepath.Join(sysRoot, "class", "nvme-subsystem")
	entries, err := os.ReadDir(classDir)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	subsystemName := ""
	for _, entry := range entries {
		subsystemNQN, err := readSysfsAttribute(filepath.Join(classDir, entry.Name(), "subsysnqn"))
		if err != nil {
			if isSysfsEntryGone(err) {
				continue
			}
			return nil, err
		}
		if subsystemNQN != nqn {
			continue
		}
		if subsystemName != "" {
			return nil, fmt.Errorf("multiple subsystems %s and %s found for NQN %s", subsystemName, entry.Name(), nqn)
		}
		subsystemName = entry.Name()
	}
	if subsystemName == "" {
		return nil, nil
	}
	return readSubsystemDir(classDir, subsystemName, nqn)
}

// readSubsystemDir returns nil when the subsystem is gone.
func readSubsystemDir(classDir, subsystemName, nqn string) (*sysfsSubsystem, error) {
	subsystemDir := filepath.Join(classDir, subsystemName)
	entries, err := os.ReadDir(subsystemDir)
	if err != nil {
		if isSysfsEntryGone(err) {
			return nil, nil
		}
		return nil, err
	}
	subsystem := &sysfsSubsystem{
		name:  subsystemName,
		nqn:   nqn,
		owned: map[string][]sysfsNamespace{},
		paths: map[string][]sysfsNamespace{},
	}
	heads := []string{}
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case nvmeNamespaceNamePattern.MatchString(name):
			heads = append(heads, name)
		case nvmeControllerNamePattern.MatchString(name):
			controller, namespaces, paths, err := readControllerFromSysfs(filepath.Join(subsystemDir, name), name)
			if err != nil {
				if isSysfsEntryGone(err) {
					continue
				}
				return nil, err
			}
			subsystem.controllers = append(subsystem.controllers, controller)
			subsystem.owned[name] = namespaces
			subsystem.paths[name] = paths
		}
	}
	for _, name := range heads {
		namespace, err := readNamespaceFromSysfs(filepath.Join(subsystemDir, name), name)
		if err != nil {
			if isSysfsEntryGone(err) {
				continue
			}
			return nil, err
		}
		pathOrder, err := readPathOrderFromSysfs(filepath.Join(subsystemDir, name, "multipath"))
		if err != nil {
			return nil, err
		}
		subsystem.heads = append(subsystem.heads, sysfsNamespace{namespace: namespace, usable: subsystem.hasUsablePathTo(name), pathOrder: pathOrder})
	}
	return subsystem, nil
}

// readPathOrderFromSysfs returns nil when the kernel has no multipath directory.
func readPathOrderFromSysfs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if isSysfsEntryGone(err) {
			return nil, nil
		}
		return nil, err
	}
	paths := []string{}
	for _, entry := range entries {
		if nvmePathNamePattern.MatchString(entry.Name()) {
			paths = append(paths, entry.Name())
		}
	}
	return paths, nil
}

func (s *sysfsSubsystem) hasUsablePathTo(head string) bool {
	headMatch := nvmeHeadNamePattern.FindStringSubmatch(head)
	for _, paths := range s.paths {
		for _, path := range paths {
			pathMatch := nvmePathNamePattern.FindStringSubmatch(path.namespace.NameSpace)
			if headMatch != nil && pathMatch != nil && pathMatch[1] == headMatch[1] && pathMatch[2] == headMatch[2] && path.usable {
				return true
			}
		}
	}
	return false
}

// readControllerFromSysfs also returns the namespace devices and the hidden paths under the controller.
func readControllerFromSysfs(controllerDir, name string) (Controller, []sysfsNamespace, []sysfsNamespace, error) {
	controller := Controller{Controller: name}
	for attribute, value := range map[string]*string{
		"state":     &controller.State,
		"transport": &controller.Transport,
	} {
		v, err := readSysfsAttribute(filepath.Join(controllerDir, attribute))
		if err != nil {
			return Controller{}, nil, nil, err
		}
		*value = v
	}
	address, err := readSysfsAttribute(filepath.Join(controllerDir, "address"))
	// A loop controller may have no address, which libnvme accepts.
	if err != nil && (!errors.Is(err, fs.ErrNotExist) || controller.Transport != "loop") {
		return Controller{}, nil, nil, err
	}
	controller.Address = address

	entries, err := os.ReadDir(controllerDir)
	if err != nil {
		return Controller{}, nil, nil, err
	}
	namespaces := []sysfsNamespace{}
	paths := []sysfsNamespace{}
	for _, entry := range entries {
		isNamespace := nvmeNamespaceNamePattern.MatchString(entry.Name())
		if !isNamespace && !nvmePathNamePattern.MatchString(entry.Name()) {
			continue
		}
		dir := filepath.Join(controllerDir, entry.Name())
		namespace := Namespace{NameSpace: entry.Name()}
		var err error
		if isNamespace {
			namespace, err = readNamespaceFromSysfs(dir, entry.Name())
		}
		usable := false
		if err == nil {
			usable, err = isPathUsable(dir, controller.State)
		}
		if err != nil {
			if isSysfsEntryGone(err) {
				continue
			}
			return Controller{}, nil, nil, err
		}
		if isNamespace {
			namespaces = append(namespaces, sysfsNamespace{namespace: namespace, usable: usable})
		} else {
			paths = append(paths, sysfsNamespace{namespace: namespace, usable: usable})
		}
	}
	return controller, namespaces, paths, nil
}

// readNamespaceFromSysfs reads the attributes libnvme takes from sysfs. SPDK targets report
// the namespace as fully used, so the used bytes are the size.
func readNamespaceFromSysfs(dir, name string) (Namespace, error) {
	values := map[string]int64{}
	for _, attribute := range []string{"nsid", "size", "queue/logical_block_size"} {
		v, err := readSysfsAttribute(filepath.Join(dir, attribute))
		if err != nil {
			return Namespace{}, err
		}
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			return Namespace{}, errors.Wrapf(err, "invalid %s %q of namespace %s", attribute, v, name)
		}
		values[attribute] = n
	}
	sectorSize := values["queue/logical_block_size"]
	if sectorSize <= 0 {
		return Namespace{}, fmt.Errorf("invalid logical block size %d of namespace %s", sectorSize, name)
	}
	// size is in 512-byte sectors whatever the logical block size is.
	physicalSize := values["size"] * 512
	return Namespace{
		NameSpace:    name,
		NSID:         int32(values["nsid"]),
		UsedBytes:    physicalSize,
		MaximumLBA:   physicalSize / sectorSize,
		PhysicalSize: physicalSize,
		SectorSize:   int32(sectorSize),
	}, nil
}

// isPathUsable follows the kernel's nvme_path_is_disabled and ANA checks.
func isPathUsable(dir, controllerState string) (bool, error) {
	if _, err := readSysfsAttribute(filepath.Join(dir, "nsid")); err != nil {
		return false, err
	}
	anaState, err := readSysfsAttribute(filepath.Join(dir, "ana_state"))
	if err != nil {
		if !errors.Is(err, fs.ErrNotExist) {
			return false, err
		}
		anaState = ""
	}
	if controllerState != NvmeControllerStateLive && controllerState != NvmeControllerStateDeleting {
		return false, nil
	}
	return anaState == "" || anaState == "optimized" || anaState == "non-optimized", nil
}

// isSysfsEntryGone also covers ENODEV, returned for a kernfs file deactivated after it was opened.
func isSysfsEntryGone(err error) bool {
	return errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ENODEV)
}

func readSysfsAttribute(path string) (string, error) {
	content, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(content)), nil
}
