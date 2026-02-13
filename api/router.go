package api

import (
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/metrics_collector/registry"
)

type HandleFuncWithError func(http.ResponseWriter, *http.Request) error

// SteveHandler is an optional Steve API handler
type SteveHandler interface {
	http.Handler
}

func HandleError(s *client.Schemas, t HandleFuncWithError) http.Handler {
	return api.ApiHandler(s, http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		if err := t(rw, req); err != nil {
			logrus.WithError(err).Warnf("HTTP handling error")

			statusCode := http.StatusInternalServerError
			if datastore.ErrorIsNotFound(err) {
				statusCode = http.StatusNotFound
			}
			writeErr(rw, req, err, statusCode)
		}
	}))
}

func writeErr(rw http.ResponseWriter, req *http.Request, err error, statusCode int) {
	apiContext := api.GetApiContext(req)
	rw.WriteHeader(statusCode)
	writeErr := apiContext.WriteResource(&client.ServerApiError{
		Resource: client.Resource{
			Type: "error",
		},
		Status:  statusCode,
		Code:    http.StatusText(statusCode),
		Message: err.Error(),
	})
	if writeErr != nil {
		logrus.WithError(writeErr).Errorf("Failed to write error %v", err)
	}
}

func NewRouter(s *Server) *mux.Router {
	return NewRouterWithSteve(s, nil)
}

// NewRouterWithSteve creates a new router with optional Steve API handler.
// If steveHandler is provided, Steve API will be available at /v1/steve/* paths.
func NewRouterWithSteve(s *Server, steveHandler SteveHandler) *mux.Router {
	schemas := NewSchema()
	r := mux.NewRouter().StrictSlash(true)
	f := HandleError

	versionsHandler := api.VersionsHandler(schemas, "v1")
	versionHandler := api.VersionHandler(schemas, "v1")
	r.Methods("GET").Path("/").Handler(versionsHandler)
	r.Methods("GET").Path("/metrics").Handler(registry.Handler())

	// Mount Steve API at /v1/longhorn/ if handler is provided
	// Steve API provides endpoints like /v1/longhorn/longhorn.io.volumes
	// This path is under /v1/ so it works with existing Nginx proxy rules
	if steveHandler != nil {
		// User requests /v1/longhorn/longhorn.io.volumes
		// We need to transform it to /v1/longhorn.io.volumes for Steve
		// steveHandler already includes SimplifiedHandler which rewrites response URLs
		r.PathPrefix("/v1/longhorn/").Handler(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			// /v1/longhorn/longhorn.io.volumes -> /v1/longhorn.io.volumes
			originalPath := req.URL.Path
			newPath := "/v1" + strings.TrimPrefix(req.URL.Path, "/v1/longhorn")

			// Clear the mux route variables from Longhorn's router.
			// Steve's mux router needs to set its own variables ({type}, {namespace}, {name})
			// but gorilla/mux stores variables in the request context. If we don't clear them,
			// Steve's router may not properly override the empty variables from Longhorn's
			// PathPrefix route, causing schema lookup to fail and return 404.
			newReq := mux.SetURLVars(req, nil)
			newReq.URL.Path = newPath

			logrus.Debugf("Steve API router: %s %s -> %s", req.Method, originalPath, newPath)
			steveHandler.ServeHTTP(w, newReq)
		}))
		logrus.Info("Steve API mounted at /v1/longhorn/")
	}

	// Apply manager-url middleware to all API routes (including previously registered routes)
	r.Use(ManagerURLMiddleware(s))

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
		"attach":                                s.VolumeAttach,
		"detach":                                s.VolumeDetach,
		"salvage":                               s.VolumeSalvage,
		"updateDataLocality":                    s.VolumeUpdateDataLocality,
		"updateAccessMode":                      s.VolumeUpdateAccessMode,
		"updateUnmapMarkSnapChainRemoved":       s.VolumeUpdateUnmapMarkSnapChainRemoved,
		"updateSnapshotMaxCount":                s.VolumeUpdateSnapshotMaxCount,
		"updateSnapshotMaxSize":                 s.VolumeUpdateSnapshotMaxSize,
		"updateReplicaRebuildingBandwidthLimit": s.VolumeUpdateReplicaRebuildingBandwidthLimit,
		"updateUblkQueueDepth":                  s.VolumeUpdateUblkQueueDepth,
		"updateUblkNumberOfQueue":               s.VolumeUpdateUpdateUblkNumberOfQueue,
		"updateReplicaSoftAntiAffinity":         s.VolumeUpdateReplicaSoftAntiAffinity,
		"updateReplicaZoneSoftAntiAffinity":     s.VolumeUpdateReplicaZoneSoftAntiAffinity,
		"updateReplicaDiskSoftAntiAffinity":     s.VolumeUpdateReplicaDiskSoftAntiAffinity,
		"activate":                              s.VolumeActivate,
		"expand":                                s.VolumeExpand,
		"cancelExpansion":                       s.VolumeCancelExpansion,
		"offlineReplicaRebuilding":              s.VolumeOfflineRebuilding,

		"updateReplicaCount":                s.VolumeUpdateReplicaCount,
		"updateReplicaAutoBalance":          s.VolumeUpdateReplicaAutoBalance,
		"updateRebuildConcurrentSyncLimit":  s.VolumeUpdateRebuildConcurrentSyncLimit,
		"updateSnapshotDataIntegrity":       s.VolumeUpdateSnapshotDataIntegrity,
		"updateBackupCompressionMethod":     s.VolumeUpdateBackupCompressionMethod,
		"updateFreezeFilesystemForSnapshot": s.VolumeUpdateFreezeFilesystemForSnapshot,
		"updateBackupTargetName":            s.VolumeUpdateBackupTargetName,
		"replicaRemove":                     s.ReplicaRemove,

		"engineUpgrade": s.EngineUpgrade,

		"trimFilesystem": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.VolumeFilesystemTrim),

		"snapshotPurge":  s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotPurge),
		"snapshotCreate": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotCreate),
		"snapshotList":   s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotList),
		"snapshotGet":    s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotGet),
		"snapshotDelete": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotDelete),
		"snapshotRevert": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotRevert),
		"snapshotBackup": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromVolume(s.m)), s.SnapshotBackup),

		"snapshotCRCreate": s.SnapshotCRCreate,
		"snapshotCRList":   s.SnapshotCRList,
		"snapshotCRGet":    s.SnapshotCRGet,
		"snapshotCRDelete": s.SnapshotCRDelete,

		"pvCreate":  s.PVCreate,
		"pvcCreate": s.PVCCreate,

		"recurringJobAdd":    s.VolumeRecurringAdd,
		"recurringJobList":   s.VolumeRecurringList,
		"recurringJobDelete": s.VolumeRecurringDelete,
	}
	for name, action := range volumeActions {
		r.Methods("POST").Path("/v1/volumes/{name}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("POST").Path("/v1/backuptargets").Handler(f(schemas, s.BackupTargetCreate))
	r.Methods("GET").Path("/v1/backuptargets/{backupTargetName}").Handler(f(schemas, s.BackupTargetGet))
	r.Methods("GET").Path("/v1/backuptargets").Handler(f(schemas, s.BackupTargetList))
	r.Methods("PUT").Path("/v1/backuptargets").Handler(f(schemas, s.BackupTargetSyncAll))
	r.Methods("DELETE").Path("/v1/backuptargets/{backupTargetName}").Handler(f(schemas, s.BackupTargetDelete))
	backupTargetActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"backupTargetSync":   s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromBackupTarget(s.m)), s.BackupTargetSync),
		"backupTargetUpdate": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(OwnerIDFromBackupTarget(s.m)), s.BackupTargetUpdate),
	}
	for name, action := range backupTargetActions {
		r.Methods("POST").Path("/v1/backuptargets/{backupTargetName}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/backupvolumes").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupVolumeList)))
	r.Methods("PUT").Path("/v1/backupvolumes").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupVolumeSyncAll)))
	r.Methods("GET").Path("/v1/backupvolumes/{backupVolumeName}").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupVolumeGet)))
	r.Methods("DELETE").Path("/v1/backupvolumes/{backupVolumeName}").Handler(f(schemas, s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupVolumeDelete)))
	backupActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"backupList":         s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupListByBackupVolume),
		"backupListByVolume": s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupListByVolume),
		"backupGet":          s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupGet),
		"backupDelete":       s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.BackupDelete),
		"backupVolumeSync":   s.fwd.Handler(s.fwd.HandleProxyRequestByNodeID, s.fwd.GetHTTPAddressByNodeID(NodeHasDefaultEngineImage(s.m)), s.SyncBackupVolume),
	}
	for name, action := range backupActions {
		r.Methods("POST").Path("/v1/backupvolumes/{backupVolumeName}").Queries("action", name).Handler(f(schemas, action))
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
		"backingImageCleanup":      s.BackingImageCleanup,
		BackingImageUpload:         s.fwd.Handler(s.fwd.HandleProxyRequestForBackingImageUpload, UploadParametersForBackingImage(s.m), s.BackingImageGet),
		"backupBackingImageCreate": s.BackupBackingImageCreate,
		"updateMinNumberOfCopies":  s.UpdateMinNumberOfCopies,
	}
	for name, action := range backingImageActions {
		r.Methods("POST").Path("/v1/backingimages/{name}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/backupbackingimages").Handler(f(schemas, s.BackupBackingImageList))
	r.Methods("GET").Path("/v1/backupbackingimages/{name}").Handler(f(schemas, s.BackupBackingImageGet))
	r.Methods("DELETE").Path("/v1/backupbackingimages/{name}").Handler(f(schemas, s.BackupBackingImageDelete))
	backupBackingImageActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"backupBackingImageRestore": s.BackupBackingImageRestore,
	}
	for name, action := range backupBackingImageActions {
		r.Methods("POST").Path("/v1/backupbackingimages/{name}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/recurringjobs").Handler(f(schemas, s.RecurringJobList))
	r.Methods("GET").Path("/v1/recurringjobs/{name}").Handler(f(schemas, s.RecurringJobGet))
	r.Methods("DELETE").Path("/v1/recurringjobs/{name}").Handler(f(schemas, s.RecurringJobDelete))
	r.Methods("POST").Path("/v1/recurringjobs").Handler(f(schemas, s.RecurringJobCreate))
	r.Methods("PUT").Path("/v1/recurringjobs/{name}").Handler(f(schemas, s.RecurringJobUpdate))

	r.Methods("GET").Path("/v1/orphans").Handler(f(schemas, s.OrphanList))
	r.Methods("GET").Path("/v1/orphans/{name}").Handler(f(schemas, s.OrphanGet))
	r.Methods("DELETE").Path("/v1/orphans/{name}").Handler(f(schemas, s.OrphanDelete))

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

	nodeListStream := NewStreamHandlerFunc("nodes", s.wsc.NewWatcher("node"), s.nodeList)
	r.Path("/v1/ws/nodes").Handler(f(schemas, nodeListStream))
	r.Path("/v1/ws/{period}/nodes").Handler(f(schemas, nodeListStream))

	engineImageStream := NewStreamHandlerFunc("engineimages", s.wsc.NewWatcher("engineImage"), s.engineImageList)
	r.Path("/v1/ws/engineimages").Handler(f(schemas, engineImageStream))
	r.Path("/v1/ws/{period}/engineimages").Handler(f(schemas, engineImageStream))

	backingImageStream := NewStreamHandlerFunc("backingimages", s.wsc.NewWatcher("backingImage"), s.backingImageList)
	r.Path("/v1/ws/backingimages").Handler(f(schemas, backingImageStream))
	r.Path("/v1/ws/{period}/backingimages").Handler(f(schemas, backingImageStream))

	backupBackingImageStream := NewStreamHandlerFunc("backupbackingimages", s.wsc.NewWatcher("backupBackingImage"), s.backupBackingImageList)
	r.Path("/v1/ws/backupbackingimages").Handler(f(schemas, backupBackingImageStream))
	r.Path("/v1/ws/{period}/backupbackingimages").Handler(f(schemas, backupBackingImageStream))

	backupVolumeStream := NewStreamHandlerFunc("backupvolumes", s.wsc.NewWatcher("backupVolume"), s.backupVolumeList)
	r.Path("/v1/ws/backupvolumes").Handler(f(schemas, backupVolumeStream))
	r.Path("/v1/ws/{period}/backupvolumes").Handler(f(schemas, backupVolumeStream))

	backupTargetStream := NewStreamHandlerFunc("backuptargets", s.wsc.NewWatcher("backupTarget"), s.backupTargetList)
	r.Path("/v1/ws/backuptargets").Handler(f(schemas, backupTargetStream))
	r.Path("/v1/ws/{period}/backuptargets").Handler(f(schemas, backupTargetStream))

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

	// VolumeAttachment routes
	r.Methods("GET").Path("/v1/volumeattachments").Handler(f(schemas, s.VolumeAttachmentList))
	r.Methods("GET").Path("/v1/volumeattachments/{name}").Handler(f(schemas, s.VolumeAttachmentGet))

	volumeAttachmentListStream := NewStreamHandlerFunc("volumeattachments", s.wsc.NewWatcher("volumeAttachment"), s.volumeAttachmentList)
	r.Path("/v1/ws/volumeattachments").Handler(f(schemas, volumeAttachmentListStream))
	r.Path("/v1/ws/{period}/volumeattachments").Handler(f(schemas, volumeAttachmentListStream))

	return r
}
