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
