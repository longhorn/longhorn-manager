package api

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/metrics_collector/registry"
)

type HandleFuncWithError func(http.ResponseWriter, *http.Request) error

func HandleError(s *client.Schemas, t HandleFuncWithError) http.Handler {
	return api.ApiHandler(s, http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if err := t(rw, req); err != nil {
			logrus.Warnf("HTTP handling error %v", err)
			apiContext := api.GetApiContext(req)
			apiContext.WriteErr(err)
		}
	}))
}

func NewRouter(s *Server) *mux.Router {
	schemas := NewSchema()
	r := mux.NewRouter().StrictSlash(true)
	f := HandleError

	versionsHandler := api.VersionsHandler(schemas, "v1")
	versionHandler := api.VersionHandler(schemas, "v1")
	r.Methods("GET").Path("/").Handler(versionsHandler)
	r.Methods("GET").Path("/metrics").Handler(registry.Handler())
	r.Methods("GET").Path("/v1").Handler(versionHandler)
	r.Methods("GET").Path("/v1/apiversions").Handler(versionsHandler)
	r.Methods("GET").Path("/v1/apiversions/v1").Handler(versionHandler)
	r.Methods("GET").Path("/v1/schemas").Handler(api.SchemasHandler(schemas))
	r.Methods("GET").Path("/v1/schemas/{id}").Handler(api.SchemaHandler(schemas))

	r.Methods("GET").Path("/v1/settings").Handler(f(schemas, s.SettingList))
	r.Methods("GET").Path("/v1/settings/{name}").Handler(f(schemas, s.SettingGet))
	r.Methods("PUT").Path("/v1/settings/{name}").Handler(f(schemas, s.SettingSet))

	r.Methods("GET").Path("/v1/volumes").Handler(f(schemas, s.VolumeList))
	r.Methods("GET").Path("/v1/volumes/{name}").Handler(f(schemas, s.VolumeGet))
	r.Methods("DELETE").Path("/v1/volumes/{name}").Handler(f(schemas, s.VolumeDelete))
	r.Methods("POST").Path("/v1/volumes").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.VolumeCreate)))
	volumeActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"attach":                          s.VolumeAttach,
		"detach":                          s.VolumeDetach,
		"salvage":                         s.VolumeSalvage,
		"updateDataLocality":              s.VolumeUpdateDataLocality,
		"updateAccessMode":                s.VolumeUpdateAccessMode,
		"updateUnmapMarkSnapChainRemoved": s.VolumeUpdateUnmapMarkSnapChainRemoved,
		"activate":                        s.VolumeActivate,
		"expand":                          s.VolumeExpand,
		"cancelExpansion":                 s.VolumeCancelExpansion,

		"updateReplicaCount":            s.VolumeUpdateReplicaCount,
		"updateReplicaAutoBalance":      s.VolumeUpdateReplicaAutoBalance,
		"updateSnapshotDataIntegrity":   s.VolumeUpdateSnapshotDataIntegrity,
		"updateBackupCompressionMethod": s.VolumeUpdateBackupCompressionMethod,
		"replicaRemove":                 s.ReplicaRemove,

		"engineUpgrade": s.EngineUpgrade,

		"trimFilesystem": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.VolumeFilesystemTrim),

		"snapshotPurge":  s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotPurge),
		"snapshotCreate": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotCreate),
		"snapshotList":   s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotList),
		"snapshotGet":    s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotGet),
		"snapshotDelete": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotDelete),
		"snapshotRevert": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotRevert),
		"snapshotBackup": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotBackup),

		"pvCreate":  s.PVCreate,
		"pvcCreate": s.PVCCreate,

		"recurringJobAdd":    s.VolumeRecurringAdd,
		"recurringJobList":   s.VolumeRecurringList,
		"recurringJobDelete": s.VolumeRecurringDelete,
	}
	for name, action := range volumeActions {
		r.Methods("POST").Path("/v1/volumes/{name}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/backuptargets").Handler(f(schemas, s.BackupTargetList))
	r.Methods("GET").Path("/v1/backupvolumes").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupVolumeList)))
	r.Methods("GET").Path("/v1/backupvolumes/{volName}").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupVolumeGet)))
	r.Methods("DELETE").Path("/v1/backupvolumes/{volName}").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupVolumeDelete)))
	backupActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"backupList":   s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupList),
		"backupGet":    s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupGet),
		"backupDelete": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupDelete),
	}
	for name, action := range backupActions {
		r.Methods("POST").Path("/v1/backupvolumes/{volName}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/nodes").Handler(f(schemas, s.NodeList))
	r.Methods("GET").Path("/v1/nodes/{name}").Handler(f(schemas, s.NodeGet))
	r.Methods("PUT").Path("/v1/nodes/{name}").Handler(f(schemas, s.NodeUpdate))
	r.Methods("DELETE").Path("/v1/nodes/{name}").Handler(f(schemas, s.NodeDelete))
	nodeActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"diskUpdate": s.DiskUpdate,
	}
	for name, action := range nodeActions {
		r.Methods("POST").Path("/v1/nodes/{name}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/engineimages").Handler(f(schemas, s.EngineImageList))
	r.Methods("GET").Path("/v1/engineimages/{name}").Handler(f(schemas, s.EngineImageGet))
	r.Methods("DELETE").Path("/v1/engineimages/{name}").Handler(f(schemas, s.EngineImageDelete))
	r.Methods("POST").Path("/v1/engineimages").Handler(f(schemas, s.EngineImageCreate))

	r.Methods("Get").Path("/v1/events").Handler(f(schemas, s.EventList))

	r.Methods("GET").Path("/v1/disktags").Handler(f(schemas, s.DiskTagList))
	r.Methods("GET").Path("/v1/nodetags").Handler(f(schemas, s.NodeTagList))

	r.Methods("GET").Path("/v1/instancemanagers").Handler(f(schemas, s.InstanceManagerList))
	r.Methods("GET").Path("/v1/instancemanagers/{name}").Handler(f(schemas, s.InstanceManagerGet))

	r.Methods("GET").Path("/v1/backingimages").Handler(f(schemas, s.BackingImageList))
	r.Methods("GET").Path("/v1/backingimages/{name}").Handler(f(schemas, s.BackingImageGet))
	r.Methods("GET").Path("/v1/backingimages/{name}/download").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestForBackingImageDownload, DownloadParametersFromBackingImage(s.m), s.BackingImageProxyFallback)))
	r.Methods("POST").Path("/v1/backingimages").Handler(f(schemas, s.BackingImageCreate))
	r.Methods("DELETE").Path("/v1/backingimages/{name}").Handler(f(schemas, s.BackingImageDelete))
	backingImageActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"backingImageCleanup": s.BackingImageCleanup,

		BackingImageUpload: s.fwd.Handler(s.fwd.HandleProxyRequestForBackingImageUpload, UploadParametersForBackingImage(s.m), s.BackingImageGet),
	}
	for name, action := range backingImageActions {
		r.Methods("POST").Path("/v1/backingimages/{name}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/recurringjobs").Handler(f(schemas, s.RecurringJobList))
	r.Methods("GET").Path("/v1/recurringjobs/{name}").Handler(f(schemas, s.RecurringJobGet))
	r.Methods("DELETE").Path("/v1/recurringjobs/{name}").Handler(f(schemas, s.RecurringJobDelete))
	r.Methods("POST").Path("/v1/recurringjobs").Handler(f(schemas, s.RecurringJobCreate))
	r.Methods("PUT").Path("/v1/recurringjobs/{name}").Handler(f(schemas, s.RecurringJobUpdate))

	r.Methods("GET").Path("/v1/orphans").Handler(f(schemas, s.OrphanList))
	r.Methods("GET").Path("/v1/orphans/{name}").Handler(f(schemas, s.OrphanGet))
	r.Methods("DELETE").Path("/v1/orphans/{name}").Handler(f(schemas, s.OrphanDelete))

	r.Methods("GET").Path("/v1/sharemanagers").Handler(f(schemas, s.ShareManagerList))
	r.Methods("GET").Path("/v1/sharemanagers/{name}").Handler(f(schemas, s.ShareManagerGet))

	r.Methods("POST").Path("/v1/supportbundles").Handler(f(schemas, s.SupportBundleCreate))
	r.Methods("GET").Path("/v1/supportbundles").Handler(f(schemas, s.SupportBundleList))
	r.Methods("GET").Path("/v1/supportbundles/{name}/{bundleName}").Handler(f(schemas,
		s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromNode(s.m)), s.SupportBundleGet)))
	r.Methods("GET").Path("/v1/supportbundles/{name}/{bundleName}/download").Handler(f(schemas,
		s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromNode(s.m)), s.SupportBundleDownload)))
	r.Methods("DELETE").Path("/v1/supportbundles/{name}/{bundleName}").Handler(f(schemas,
		s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromNode(s.m)), s.SupportBundleDelete)))

	r.Methods("POST").Path("/v1/systembackups").Handler(f(schemas, s.SystemBackupCreate))
	r.Methods("GET").Path("/v1/systembackups").Handler(f(schemas, s.SystemBackupList))
	r.Methods("GET").Path("/v1/systembackups/{name}").Handler(f(schemas, s.SystemBackupGet))
	r.Methods("DELETE").Path("/v1/systembackups/{name}").Handler(f(schemas, s.SystemBackupDelete))

	r.Methods("POST").Path("/v1/systemrestores").Handler(f(schemas, s.SystemRestoreCreate))
	r.Methods("GET").Path("/v1/systemrestores").Handler(f(schemas, s.SystemRestoreList))
	r.Methods("GET").Path("/v1/systemrestores/{name}").Handler(f(schemas, s.SystemRestoreGet))
	r.Methods("DELETE").Path("/v1/systemrestores/{name}").Handler(f(schemas, s.SystemRestoreDelete))

	settingListStream := NewStreamHandlerFunc("settings", s.wsc.NewWatcher("setting"), s.settingList)
	r.Path("/v1/ws/settings").Handler(f(schemas, settingListStream))
	r.Path("/v1/ws/{period}/settings").Handler(f(schemas, settingListStream))

	volumeListStream := NewStreamHandlerFunc("volumes", s.wsc.NewWatcher("volume", "engine", "replica", "backup"), s.volumeList)
	r.Path("/v1/ws/volumes").Handler(f(schemas, volumeListStream))
	r.Path("/v1/ws/{period}/volumes").Handler(f(schemas, volumeListStream))

	recurringJobListStream := NewStreamHandlerFunc("recurringjobs", s.wsc.NewWatcher("recurringJob"), s.recurringJobList)
	r.Path("/v1/ws/recurringjobs").Handler(f(schemas, recurringJobListStream))
	r.Path("/v1/ws/{period}/recurringjobs").Handler(f(schemas, recurringJobListStream))

	orphanListStream := NewStreamHandlerFunc("orphans", s.wsc.NewWatcher("orphan"), s.orphanList)
	r.Path("/v1/ws/orphans").Handler(f(schemas, orphanListStream))
	r.Path("/v1/ws/{period}/orphans").Handler(f(schemas, orphanListStream))

	shareManagerListStream := NewStreamHandlerFunc("sharemanagers", s.wsc.NewWatcher("shareManager"), s.shareManagerList)
	r.Path("/v1/ws/sharemanagers").Handler(f(schemas, shareManagerListStream))
	r.Path("/v1/ws/{period}/sharemanagers").Handler(f(schemas, shareManagerListStream))

	nodeListStream := NewStreamHandlerFunc("nodes", s.wsc.NewWatcher("node"), s.nodeList)
	r.Path("/v1/ws/nodes").Handler(f(schemas, nodeListStream))
	r.Path("/v1/ws/{period}/nodes").Handler(f(schemas, nodeListStream))

	engineImageStream := NewStreamHandlerFunc("engineimages", s.wsc.NewWatcher("engineImage"), s.engineImageList)
	r.Path("/v1/ws/engineimages").Handler(f(schemas, engineImageStream))
	r.Path("/v1/ws/{period}/engineimages").Handler(f(schemas, engineImageStream))

	backingImageStream := NewStreamHandlerFunc("backingimages", s.wsc.NewWatcher("backingImage"), s.backingImageList)
	r.Path("/v1/ws/backingimages").Handler(f(schemas, backingImageStream))
	r.Path("/v1/ws/{period}/backingimages").Handler(f(schemas, backingImageStream))

	backupVolumeStream := NewStreamHandlerFunc("backupvolumes", s.wsc.NewWatcher("backupVolume"), s.backupVolumeList)
	r.Path("/v1/ws/backupvolumes").Handler(f(schemas, backupVolumeStream))
	r.Path("/v1/ws/{period}/backupvolumes").Handler(f(schemas, backupVolumeStream))

	// TODO:
	// We haven't found a way to allow passing the volume name as a parameter to filter
	// per-backup volume's backups change thru. WebSocket endpoint. Either by:
	// - `/v1/ws/backups/{volName}`
	// - `/v1/ws/backups?volName=<volName>`
	// - `/v1/ws/backupvolumes/{backupName}`
	// Once we enhance this part, the WebSocket endpoint could only send the updates of specific
	// backup volume changes and decrease the traffic data it sends out.
	backupStream := NewStreamHandlerFunc("backups", s.wsc.NewWatcher("backup"), s.backupListAll)
	r.Path("/v1/ws/backups").Handler(f(schemas, backupStream))
	r.Path("/v1/ws/{period}/backups").Handler(f(schemas, backupStream))

	systemBackupStream := NewStreamHandlerFunc("systembackups", s.wsc.NewWatcher("systemBackup"), s.systemBackupList)
	r.Path("/v1/ws/systembackups").Handler(f(schemas, systemBackupStream))
	r.Path("/v1/ws/{period}/systembackups").Handler(f(schemas, systemBackupStream))

	systemRestoreStream := NewStreamHandlerFunc("systemrestores", s.wsc.NewWatcher("systemRestore"), s.systemRestoreList)
	r.Path("/v1/ws/systemrestores").Handler(f(schemas, systemRestoreStream))
	r.Path("/v1/ws/{period}/systemrestores").Handler(f(schemas, systemRestoreStream))

	eventListStream := NewStreamHandlerFunc("events", s.wsc.NewWatcher("event"), s.eventList)
	r.Path("/v1/ws/events").Handler(f(schemas, eventListStream))
	r.Path("/v1/ws/{period}/events").Handler(f(schemas, eventListStream))

	return r
}
