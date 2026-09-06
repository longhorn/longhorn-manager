package spdk

import (
	"context"

	"github.com/sirupsen/logrus"

	"google.golang.org/protobuf/types/known/emptypb"

	grpccodes "google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/longhorn/types/pkg/generated/spdkrpc"
)

func (s *Server) DiskCreate(ctx context.Context, req *spdkrpc.DiskCreateRequest) (*spdkrpc.Disk, error) {
	s.Lock()
	spdkClient := s.spdkClient

	disk, exists := s.diskMap[req.DiskName]
	if exists {
		if disk.GetState() == DiskStateReady {
			s.Unlock()
			return disk.DiskGet(spdkClient, req.DiskName, req.DiskPath, req.DiskDriver)
		}
		s.Unlock()

		return &spdkrpc.Disk{
			State: string(disk.GetState()),
		}, nil
	}

	disk = NewDisk(req.DiskName, req.DiskUuid, req.DiskPath, req.DiskDriver, req.BlockSize)
	s.diskMap[req.DiskName] = disk
	s.Unlock()

	go func(d *Disk, req *spdkrpc.DiskCreateRequest) {
		// Serialize SPDK disk creation to avoid race conditions in global SPDK subsystems
		// The upper-level DiskCreate() flow remains asynchronous — the gRPC call immediately returns and the creation
		// runs in a background goroutine — but within that goroutine, we serialize the
		// lower-level SPDK operations to ensure controller attach and lvstore operations
		// are performed safely and deterministically without concurrent access issues.

		// It may have issues if multiple disks are being created simultaneously without this lock
		s.diskCreateLock.Lock()
		defer s.diskCreateLock.Unlock()

		// Disabling hotplug is a best-effort guard to improve stability; creation continues even if this call fails.
		// TODO: If virtio-blk requires the same handling, add similar guards to the virtio-blk path.
		if isNvmeDriver(req.DiskDriver, req.DiskPath) {
			_ = setNvmeHotPlug(spdkClient, false)
			defer func() {
				if success := setNvmeHotPlug(spdkClient, true); success {
					s.hotplugActive.Store(true)
				} else {
					s.hotplugActive.Store(false)
				}
			}()
		}

		if err := d.DiskCreate(spdkClient, req.DiskName, req.DiskUuid, req.DiskPath, req.DiskDriver, req.BlockSize); err != nil {
			logrus.WithError(err).Errorf("Failed to create disk %s(%s) path %s", req.DiskName, req.DiskUuid, req.DiskPath)
			return
		}

		logrus.Infof("Disk %v is created, replicas will be discovered by the next monitoring cycle", req.DiskName)
	}(disk, req)

	return &spdkrpc.Disk{
		State: string(disk.GetState()),
	}, nil
}

func (s *Server) DiskDelete(ctx context.Context, req *spdkrpc.DiskDeleteRequest) (ret *emptypb.Empty, err error) {
	s.RLock()
	disk := s.diskMap[req.DiskName]
	spdkClient := s.spdkClient
	s.RUnlock()

	defer func() {
		if err == nil {
			s.Lock()
			delete(s.diskMap, req.DiskName)
			s.Unlock()
		}
	}()

	if disk == nil {
		// If the specified disk does not exist, tlog a warning for visibility.
		logrus.Warnf("Disk %s not found; skipping deletion", req.DiskName)
		return &emptypb.Empty{}, nil
	}

	return disk.DiskDelete(spdkClient, req.DiskName, req.DiskUuid, req.DiskPath, req.DiskDriver)
}

func (s *Server) DiskGet(ctx context.Context, req *spdkrpc.DiskGetRequest) (ret *spdkrpc.Disk, err error) {
	s.RLock()
	disk := s.diskMap[req.DiskName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if disk == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "cannot find disk %v", req.DiskName)
	}

	return disk.DiskGet(spdkClient, req.DiskName, req.DiskPath, req.DiskDriver)
}

func (s *Server) DiskHealthGet(ctx context.Context, req *spdkrpc.DiskHealthGetRequest) (ret *spdkrpc.DiskHealthGetResponse, err error) {
	s.RLock()
	disk := s.diskMap[req.DiskName]
	spdkClient := s.spdkClient
	s.RUnlock()

	if disk == nil {
		return nil, grpcstatus.Errorf(grpccodes.NotFound, "disk %q not found", req.DiskName)
	}

	diskHealth, err := disk.diskHealthGet(spdkClient, req.DiskName, req.DiskDriver)
	if err != nil {
		return nil, err
	}

	return &spdkrpc.DiskHealthGetResponse{
		ModelNumber:                             diskHealth.ModelNumber,
		SerialNumber:                            diskHealth.SerialNumber,
		FirmwareRevision:                        diskHealth.FirmwareRevision,
		Traddr:                                  diskHealth.Traddr,
		CriticalWarning:                         diskHealth.CriticalWarning,
		TemperatureCelsius:                      diskHealth.TemperatureCelsius,
		AvailableSparePercentage:                diskHealth.AvailableSparePercentage,
		AvailableSpareThresholdPercentage:       diskHealth.AvailableSpareThresholdPercentage,
		PercentageUsed:                          diskHealth.PercentageUsed,
		DataUnitsRead:                           diskHealth.DataUnitsRead,
		DataUnitsWritten:                        diskHealth.DataUnitsWritten,
		HostReadCommands:                        diskHealth.HostReadCommands,
		HostWriteCommands:                       diskHealth.HostWriteCommands,
		ControllerBusyTime:                      diskHealth.ControllerBusyTime,
		PowerCycles:                             diskHealth.PowerCycles,
		PowerOnHours:                            diskHealth.PowerOnHours,
		UnsafeShutdowns:                         diskHealth.UnsafeShutdowns,
		MediaErrors:                             diskHealth.MediaErrors,
		NumErrLogEntries:                        diskHealth.NumErrLogEntries,
		WarningTemperatureTimeMinutes:           diskHealth.WarningTemperatureTimeMinutes,
		CriticalCompositeTemperatureTimeMinutes: diskHealth.CriticalCompositeTemperatureTimeMinutes,
	}, nil
}
