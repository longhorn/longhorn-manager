package initiator

import (
	"fmt"
	"path/filepath"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	commonns "github.com/longhorn/go-common-libs/ns"

	"github.com/longhorn/go-spdk-helper/pkg/types"
)

// DiscoverTarget discovers a target
func DiscoverTarget(ip, port string, executor *commonns.Executor) (subnqn string, err error) {
	hostID, err := getHostID(executor)
	if err != nil {
		return "", err
	}
	hostNQN, err := showHostNQN(executor)
	if err != nil {
		return "", err
	}

	entries, err := discovery(hostID, hostNQN, ip, port, executor)
	if err != nil {
		return "", err
	}

	for _, entry := range entries {
		if entry.TrsvcID == port {
			return entry.Subnqn, nil
		}
	}

	return "", fmt.Errorf("found empty subnqn after nvme discover for %s:%s", ip, port)
}

// ConnectTarget connects to a target
func ConnectTarget(ip, port, nqn string, executor *commonns.Executor) (controllerName string, err error) {
	// Trying to connect an existing subsystem will error out with exit code 114.
	// Hence, it's better to check the existence first.
	if devices, err := GetDevices(ip, port, nqn, executor); err == nil && len(devices) > 0 {
		return devices[0].Controllers[0].Controller, nil
	}

	hostNQN, err := showHostNQN(executor)
	if err != nil {
		return "", err
	}
	hostID, err := getHostID(executor)
	if err != nil {
		return "", err
	}

	return connect(hostID, hostNQN, nqn, DefaultTransportType, ip, port, executor)
}

// DisconnectTarget disconnects from a target
func DisconnectTarget(nqn string, executor *commonns.Executor) error {
	return disconnect(nqn, executor)
}

// DisconnectController disconnects a single NVMe controller that
// matches the given NQN, IP, and port. This is used to remove an individual
// multipath path without affecting other controllers for the same subsystem.
// It returns nil if no matching controller is found (already disconnected).
func DisconnectController(nqn, ip, port string, executor *commonns.Executor) error {
	subsystems, err := listSubsystems("", executor)
	if err != nil {
		return errors.Wrap(err, "failed to list subsystems for controller disconnect")
	}
	for _, sys := range subsystems {
		if sys.NQN != nqn {
			continue
		}
		for _, path := range sys.Paths {
			controllerIP, controllerPort := GetIPAndPortFromControllerAddress(path.Address)
			if controllerIP == ip && controllerPort == port {
				return disconnectController(path.Name, executor)
			}
		}
	}
	return nil
}

// GetDevices returns all devices
func GetDevices(ip, port, nqn string, executor *commonns.Executor) (devices []Device, err error) {
	defer func() {
		err = errors.Wrapf(err, "failed to get devices for address %s:%s and nqn %s", ip, port, nqn)
	}()

	devices = []Device{}

	nvmeDevices, err := listRecognizedNvmeDevices(executor)
	if err != nil {
		return nil, err
	}
	for _, d := range nvmeDevices {
		subsystems, err := listSubsystems(d.DevicePath, executor)
		if err != nil {
			logrus.WithError(err).Warnf("failed to list subsystems for NVMe device %s", d.DevicePath)
			continue
		}
		if len(subsystems) == 0 {
			return nil, fmt.Errorf("no subsystem found for NVMe device %s", d.DevicePath)
		}
		if len(subsystems) > 1 {
			return nil, fmt.Errorf("multiple subsystems found for NVMe device %s", d.DevicePath)
		}
		sys := subsystems[0]

		// Reconstruct controller list
		controllers := []Controller{}
		for _, path := range sys.Paths {
			controller := Controller{
				Controller: path.Name,
				Transport:  path.Transport,
				Address:    path.Address,
				State:      path.State,
			}
			controllers = append(controllers, controller)
		}

		namespace := Namespace{
			NameSpace:    filepath.Base(d.DevicePath),
			NSID:         d.NameSpace,
			UsedBytes:    d.UsedBytes,
			MaximumLBA:   d.MaximumLBA,
			PhysicalSize: d.PhysicalSize,
			SectorSize:   d.SectorSize,
		}

		device := Device{
			Subsystem:    sys.Name,
			SubsystemNQN: sys.NQN,
			Controllers:  controllers,
			Namespaces:   []Namespace{namespace},
		}

		devices = append(devices, device)
	}

	if nqn == "" {
		return devices, err
	}

	res := []Device{}
	for _, d := range devices {
		match := false
		if d.SubsystemNQN != nqn {
			continue
		}
		for _, c := range d.Controllers {
			controllerIP, controllerPort := GetIPAndPortFromControllerAddress(c.Address)
			if ip != "" && ip != controllerIP {
				continue
			}
			if port != "" && port != controllerPort {
				continue
			}
			match = true
			break
		}
		if len(d.Namespaces) == 0 {
			continue
		}
		if match {
			res = append(res, d)
		}
	}

	if len(res) == 0 {
		// NVMe native multipath fallback: when multiple controllers share one
		// subsystem NQN, the kernel will only create a single namespace block
		// device (e.g. /dev/nvme4n1) under the first controller. Additional
		// controllers (e.g. nvme5) added via `nvme connect` do not get their
		// own block device because the kernel merges them as extra I/O paths.
		//
		// The per-device `nvme list-subsys /dev/nvme4n1` only returns the
		// path for the controller that owns that block device (nvme4), so the
		// primary matching loop above cannot find the new controller. Here we
		// query ALL subsystems without a device path filter to discover every
		// controller, then map back to the existing namespace block device.
		subsystems, err := listSubsystems("", executor)
		if err != nil {
			return nil, err
		}
		for _, sys := range subsystems {
			if sys.NQN != nqn {
				continue
			}
			pathMatch := false
			for _, path := range sys.Paths {
				controllerIP, controllerPort := GetIPAndPortFromControllerAddress(path.Address)
				if ip != "" && ip != controllerIP {
					continue
				}
				if port != "" && port != controllerPort {
					continue
				}
				pathMatch = true
				break
			}
			if !pathMatch {
				continue
			}

			// Found a subsystem with a matching controller. Now find the
			// namespace block device that belongs to this subsystem from
			// the per-device scan we already performed.
			for _, d := range devices {
				if d.SubsystemNQN != nqn {
					continue
				}
				if len(d.Namespaces) == 0 {
					continue
				}
				// Rebuild this device with ALL controllers from the
				// unfiltered subsystem query so the caller can select
				// the correct controller.
				allControllers := []Controller{}
				for _, p := range sys.Paths {
					allControllers = append(allControllers, Controller{
						Controller: p.Name,
						Transport:  p.Transport,
						Address:    p.Address,
						State:      p.State,
					})
				}
				multipathDevice := Device{
					Subsystem:    sys.Name,
					SubsystemNQN: sys.NQN,
					Controllers:  allControllers,
					Namespaces:   d.Namespaces,
				}
				res = append(res, multipathDevice)
				break
			}
		}
	}

	if len(res) == 0 {
		subsystems, err := listSubsystems("", executor)
		if err != nil {
			return nil, err
		}
		for _, sys := range subsystems {
			if sys.NQN != nqn {
				continue
			}
			for _, path := range sys.Paths {
				return nil, fmt.Errorf("subsystem NQN %s path %v address %v is in %s state",
					nqn, path.Name, path.Address, path.State)
			}
		}

		return nil, fmt.Errorf(types.ErrorMessageCannotFindValidNvmeDevice+" with subsystem NQN %s and address %s:%s", nqn, ip, port)
	}
	return res, nil
}

// GetSubsystems returns all devices
func GetSubsystems(executor *commonns.Executor) (subsystems []Subsystem, err error) {
	return listSubsystems("", executor)
}

// Flush commits data and metadata associated with the specified namespace(s) to nonvolatile media.
func Flush(device, namespaceID string, executor *commonns.Executor) (output string, err error) {
	return flush(device, namespaceID, executor)
}
