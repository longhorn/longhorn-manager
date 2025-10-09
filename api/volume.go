package api

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (s *Server) VolumeList(rw http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to list volume")
	}()

	apiContext := api.GetApiContext(req)

	resp, err := s.volumeList(apiContext)
	if err != nil {
		return err
	}

	apiContext.Write(resp)

	return nil
}

func (s *Server) volumeList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	resp := &client.GenericCollection{}

	volumes, err := s.m.ListSorted()
	if err != nil {
		return nil, err
	}

	for _, v := range volumes {
		controllers, err := s.m.GetEnginesSorted(v.Name)
		if err != nil {
			return nil, err
		}
		replicas, err := s.m.GetReplicasSorted(v.Name)
		if err != nil {
			return nil, err
		}
		backups, err := s.m.ListBackupsForVolumeSorted(v.Name)
		if err != nil {
			return nil, err
		}
		volumeAttachment, err := s.m.GetVolumeAttachment(v.Name)
		if err != nil && !apierrors.IsNotFound(err) {
			return nil, err
		}

		resp.Data = append(resp.Data, toVolumeResource(v, controllers, replicas, backups, volumeAttachment, apiContext))
	}
	resp.ResourceType = "volume"
	resp.CreateTypes = map[string]string{
		"volume": apiContext.UrlBuilder.Collection("volume"),
	}

	return resp, nil
}

func (s *Server) VolumeGet(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	return s.responseWithVolume(rw, req, id, nil)
}

func (s *Server) responseWithVolume(rw http.ResponseWriter, req *http.Request, id string, v *longhorn.Volume) error {
	var err error
	apiContext := api.GetApiContext(req)

	if v == nil {
		if id == "" {
			rw.WriteHeader(http.StatusNotFound)
			return nil
		}
		v, err = s.m.Get(id)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				rw.WriteHeader(http.StatusNotFound)
				return nil
			}
			return errors.Wrap(err, "failed to get volume")
		}
	}

	controllers, err := s.m.GetEnginesSorted(id)
	if err != nil {
		return err
	}
	replicas, err := s.m.GetReplicasSorted(id)
	if err != nil {
		return err
	}
	backups, err := s.m.ListBackupsForVolumeSorted(id)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	volumeAttachment, err := s.m.GetVolumeAttachment(v.Name)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	apiContext.Write(toVolumeResource(v, controllers, replicas, backups, volumeAttachment, apiContext))
	return nil
}

func (s *Server) VolumeCreate(rw http.ResponseWriter, req *http.Request) error {
	var volume Volume
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&volume); err != nil {
		return err
	}

	if volume.Standby {
		if volume.Frontend != "" {
			return fmt.Errorf("failed to set frontend for standby volume: %v", volume.Name)
		}
		if volume.FromBackup == "" {
			return fmt.Errorf("failed to create standby volume %v without field FromBackup", volume.Name)
		}
	} else {
		if volume.Frontend == "" {
			volume.Frontend = longhorn.VolumeFrontendBlockDev
		}
	}

	size, err := util.ConvertSize(volume.Size)
	if err != nil {
		return fmt.Errorf("failed to parse size %v", err)
	}

	// Check DiskSelector.
	diskTags, err := s.m.GetDiskTags()
	if err != nil {
		return errors.Wrap(err, "failed to get all disk tags")
	}
	sort.Strings(diskTags)
	for _, selector := range volume.DiskSelector {
		if index := sort.SearchStrings(diskTags, selector); index >= len(diskTags) || diskTags[index] != selector {
			return fmt.Errorf("specified disk tag %v does not exist", selector)
		}
	}

	// Check NodeSelector.
	nodeTags, err := s.m.GetNodeTags()
	if err != nil {
		return errors.Wrap(err, "failed to get all node tags")
	}
	sort.Strings(nodeTags)
	for _, selector := range volume.NodeSelector {
		if index := sort.SearchStrings(nodeTags, selector); index >= len(nodeTags) || nodeTags[index] != selector {
			return fmt.Errorf("specified node tag %v does not exist", selector)
		}
	}

	snapshotMaxSize, err := util.ConvertSize(volume.SnapshotMaxSize)
	if err != nil {
		return errors.Wrap(err, "failed to parse snapshot max size")
	}

	backupBlockSize, err := util.ConvertSize(volume.BackupBlockSize)
	if err != nil {
		return errors.Wrapf(err, "failed to parse backup block size %v", volume.BackupBlockSize)
	}

	v, err := s.m.Create(volume.Name, &longhorn.VolumeSpec{
		Size:                            size,
		AccessMode:                      volume.AccessMode,
		Migratable:                      volume.Migratable,
		Encrypted:                       volume.Encrypted,
		Frontend:                        volume.Frontend,
		FromBackup:                      volume.FromBackup,
		RestoreVolumeRecurringJob:       volume.RestoreVolumeRecurringJob,
		DataSource:                      volume.DataSource,
		CloneMode:                       volume.CloneMode,
		NumberOfReplicas:                volume.NumberOfReplicas,
		ReplicaAutoBalance:              volume.ReplicaAutoBalance,
		DataLocality:                    volume.DataLocality,
		StaleReplicaTimeout:             volume.StaleReplicaTimeout,
		BackingImage:                    volume.BackingImage,
		Standby:                         volume.Standby,
		RevisionCounterDisabled:         volume.RevisionCounterDisabled,
		DiskSelector:                    volume.DiskSelector,
		NodeSelector:                    volume.NodeSelector,
		SnapshotDataIntegrity:           volume.SnapshotDataIntegrity,
		SnapshotMaxCount:                volume.SnapshotMaxCount,
		SnapshotMaxSize:                 snapshotMaxSize,
		ReplicaRebuildingBandwidthLimit: volume.ReplicaRebuildingBandwidthLimit,
		BackupCompressionMethod:         volume.BackupCompressionMethod,
		BackupBlockSize:                 backupBlockSize,
		UnmapMarkSnapChainRemoved:       volume.UnmapMarkSnapChainRemoved,
		ReplicaSoftAntiAffinity:         volume.ReplicaSoftAntiAffinity,
		ReplicaZoneSoftAntiAffinity:     volume.ReplicaZoneSoftAntiAffinity,
		ReplicaDiskSoftAntiAffinity:     volume.ReplicaDiskSoftAntiAffinity,
		DataEngine:                      volume.DataEngine,
		FreezeFilesystemForSnapshot:     volume.FreezeFilesystemForSnapshot,
		BackupTargetName:                volume.BackupTargetName,
		OfflineRebuilding:               volume.OfflineRebuilding,
	}, volume.RecurringJobSelector)
	if err != nil {
		return errors.Wrap(err, "failed to create volume")
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	if err := s.m.Delete(id); err != nil {
		return errors.Wrap(err, "failed to delete volume")
	}

	return nil
}

func (s *Server) VolumeAttach(rw http.ResponseWriter, req *http.Request) error {
	var input AttachInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}
	id := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.Attach(id, input.HostID, input.DisableFrontend, input.AttachedBy, input.AttacherType, input.AttachmentID)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeDetach(rw http.ResponseWriter, req *http.Request) error {
	var input DetachInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		// HACK: for ui detach requests that don't send a body
		input.AttachmentID = ""
		input.HostID = ""
	}
	id := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.Detach(id, input.AttachmentID, input.HostID, input.ForceDetach)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeSalvage(rw http.ResponseWriter, req *http.Request) error {
	var input SalvageInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read salvageInput")
	}

	id := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.Salvage(id, input.Names)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeRecurringAdd(rw http.ResponseWriter, req *http.Request) error {
	var input VolumeRecurringJobInput
	volName := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read volumeRecurringJobInput")
	}

	_, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.AddVolumeRecurringJob(volName, input.Name, input.IsGroup)
	})
	if err != nil {
		return err
	}
	jobList, err := s.m.ListVolumeRecurringJob(volName)
	if err != nil {
		return err
	}
	api.GetApiContext(req).Write(toVolumeRecurringJobCollection(jobList))
	return nil
}

func (s *Server) VolumeRecurringList(w http.ResponseWriter, req *http.Request) (err error) {
	defer func() {
		err = errors.Wrap(err, "failed to list volume recurring jobs")
	}()

	volName := mux.Vars(req)["name"]

	jobList, err := s.m.ListVolumeRecurringJob(volName)
	if err != nil {
		return err
	}
	api.GetApiContext(req).Write(toVolumeRecurringJobCollection(jobList))
	return nil
}

func (s *Server) VolumeRecurringDelete(rw http.ResponseWriter, req *http.Request) error {
	var input VolumeRecurringJobInput
	volName := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read volumeRecurringJobInput")
	}

	_, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.DeleteVolumeRecurringJob(volName, input.Name, input.IsGroup)
	})
	if err != nil {
		return err
	}
	jobList, err := s.m.ListVolumeRecurringJob(volName)
	if err != nil {
		return err
	}
	api.GetApiContext(req).Write(toVolumeRecurringJobCollection(jobList))
	return nil
}

func (s *Server) VolumeUpdateReplicaCount(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateReplicaCountInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read replicaCount")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateReplicaCount(id, input.ReplicaCount)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateSnapshotDataIntegrity(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateSnapshotDataIntegrityInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read snapshotDataIntegrity")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateSnapshotDataIntegrity(id, input.SnapshotDataIntegrity)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateBackupCompressionMethod(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateBackupCompressionMethodInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read backupCompressionMethod")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateBackupCompressionMethod(id, input.BackupCompressionMethod)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateReplicaAutoBalance(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateReplicaAutoBalanceInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read replicaAutoBalance")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateReplicaAutoBalance(id, longhorn.ReplicaAutoBalance(input.ReplicaAutoBalance))
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateDataLocality(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateDataLocalityInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read dataLocality")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateDataLocality(id, longhorn.DataLocality(input.DataLocality))
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateAccessMode(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateAccessModeInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read access mode")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateAccessMode(id, longhorn.AccessMode(input.AccessMode))
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateUnmapMarkSnapChainRemoved(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateUnmapMarkSnapChainRemovedInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read UnmapMarkSnapChainRemoved input")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateUnmapMarkSnapChainRemoved(id, longhorn.UnmapMarkSnapChainRemoved(input.UnmapMarkSnapChainRemoved))
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateReplicaSoftAntiAffinity(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateReplicaSoftAntiAffinityInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read ReplicaSoftAntiAffinity input")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateReplicaSoftAntiAffinity(id, longhorn.ReplicaSoftAntiAffinity(input.ReplicaSoftAntiAffinity))
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateReplicaZoneSoftAntiAffinity(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateReplicaZoneSoftAntiAffinityInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read ReplicaZoneSoftAntiAffinity input")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateReplicaZoneSoftAntiAffinity(id, longhorn.ReplicaZoneSoftAntiAffinity(input.ReplicaZoneSoftAntiAffinity))
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateReplicaDiskSoftAntiAffinity(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateReplicaDiskSoftAntiAffinityInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read ReplicaDiskSoftAntiAffinity input")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateReplicaDiskSoftAntiAffinity(id, longhorn.ReplicaDiskSoftAntiAffinity(input.ReplicaDiskSoftAntiAffinity))
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeActivate(rw http.ResponseWriter, req *http.Request) error {
	var input ActivateInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.Activate(id, input.Frontend)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeExpand(rw http.ResponseWriter, req *http.Request) error {
	var input ExpandInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	size, err := util.ConvertSize(input.Size)
	if err != nil {
		return fmt.Errorf("failed to parse size %v", err)
	}

	id := mux.Vars(req)["name"]

	vol, err := s.m.Get(id)
	if err != nil {
		return errors.Wrap(err, "failed to get volume")
	}
	if vol.Status.IsStandby {
		return fmt.Errorf("failed to manually expand standby volume %v", vol.Name)
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.Expand(id, size)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeCancelExpansion(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.CancelExpansion(id)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeOfflineRebuilding(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateOfflineRebuildingInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	id := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateOfflineRebuilding(id, longhorn.VolumeOfflineRebuilding(input.OfflineRebuilding))
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeFilesystemTrim(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.TrimFilesystem(id)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) PVCreate(rw http.ResponseWriter, req *http.Request) error {
	var input PVCreateInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read pvCreateInput")
	}

	vol, err := s.m.Get(id)
	if err != nil {
		return errors.Wrap(err, "failed to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("failed to create PV for standby volume %v", vol.Name)
	}

	_, err = util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.PVCreate(id, input.PVName, input.FSType, input.SecretNamespace, input.SecretName, input.StorageClassName)
	})
	if err != nil {
		return err
	}
	return s.responseWithVolume(rw, req, id, nil)
}

func (s *Server) PVCCreate(rw http.ResponseWriter, req *http.Request) error {
	var input PVCCreateInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read pvcCreateInput")
	}

	vol, err := s.m.Get(id)
	if err != nil {
		return errors.Wrap(err, "failed to get volume")
	}

	if vol.Status.IsStandby {
		return fmt.Errorf("failed to create PVC for standby volume %v", vol.Name)
	}

	_, err = util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.PVCCreate(id, input.Namespace, input.PVCName)
	})
	if err != nil {
		return err
	}
	return s.responseWithVolume(rw, req, id, nil)
}

func (s *Server) ReplicaRemove(rw http.ResponseWriter, req *http.Request) error {
	var input ReplicaRemoveInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read replicaRemoveInput")
	}

	id := mux.Vars(req)["name"]

	if err := s.m.DeleteReplica(id, input.Name); err != nil {
		return errors.Wrap(err, "failed to remove replica")
	}

	return s.responseWithVolume(rw, req, id, nil)
}

func (s *Server) EngineUpgrade(rw http.ResponseWriter, req *http.Request) error {
	var input EngineUpgradeInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read engineUpgradeInput")
	}

	id := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.EngineUpgrade(id, input.Image)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}

	return s.responseWithVolume(rw, req, id, v)
}

func (s *Server) VolumeUpdateSnapshotMaxCount(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateSnapshotMaxCount
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read SnapshotMaxCount input")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateSnapshotMaxCount(id, input.SnapshotMaxCount)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateSnapshotMaxSize(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateSnapshotMaxSize
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read SnapshotMaxSize input")
	}

	snapshotMaxSize, err := util.ConvertSize(input.SnapshotMaxSize)
	if err != nil {
		return fmt.Errorf("failed to parse snapshot max size %v", err)
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateSnapshotMaxSize(id, snapshotMaxSize)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateReplicaRebuildingBandwidthLimit(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateReplicaRebuildingBandwidthLimit
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read ReplicaRebuildingBandwidthLimit input")
	}

	replicaRebuildingBandwidthLimit, err := util.ConvertSize(input.ReplicaRebuildingBandwidthLimit)
	if err != nil {
		return fmt.Errorf("failed to parse replica rebuilding bandwidth limit %v", err)
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateReplicaRebuildingBandwidthLimit(id, replicaRebuildingBandwidthLimit)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateFreezeFilesystemForSnapshot(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateFreezeFilesystemForSnapshotInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read FreezeFilesystemForSnapshot input")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateFreezeFilesystemForSnapshot(id, longhorn.FreezeFilesystemForSnapshot(input.FreezeFilesystemForSnapshot))
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}
	return s.responseWithVolume(rw, req, "", v)
}

func (s *Server) VolumeUpdateBackupTargetName(rw http.ResponseWriter, req *http.Request) error {
	var input UpdateBackupTargetInput
	id := mux.Vars(req)["name"]

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return errors.Wrap(err, "failed to read BackupTarget input")
	}

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateVolumeBackupTarget(id, input.BackupTargetName)
	})
	if err != nil {
		return err
	}
	v, ok := obj.(*longhorn.Volume)
	if !ok {
		return fmt.Errorf("failed to convert to volume %v object", id)
	}
	return s.responseWithVolume(rw, req, "", v)
}
