package api

import (
	"net/http"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

var (
	RetryCounts   = 5
	RetryInterval = 100 * time.Millisecond
)

type HandleFuncWithError func(http.ResponseWriter, *http.Request) error

func HandleError(s *client.Schemas, t HandleFuncWithError) http.Handler {
	return api.ApiHandler(s, http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
		var err error
		for i := 0; i < RetryCounts; i++ {
			err = t(rw, req)
			if !apierrors.IsConflict(errors.Cause(err)) {
				break
			}
			logrus.Warnf("Retry API call due to conflict")
			time.Sleep(RetryInterval)
		}
		if err != nil {
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
	r.Methods("POST").Path("/v1/volumes").Handler(f(schemas, s.VolumeCreate))

	volumeActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"attach":          s.VolumeAttach,
		"detach":          s.VolumeDetach,
		"salvage":         s.VolumeSalvage,
		"recurringUpdate": s.VolumeRecurringUpdate,

		"replicaRemove": s.ReplicaRemove,
		"engineUpgrade": s.EngineUpgrade,

		"migrationStart":    s.MigrationStart,
		"migrationConfirm":  s.MigrationConfirm,
		"migrationRollback": s.MigrationRollback,

		"snapshotPurge":  s.fwd.Handler(OwnerIDFromVolume(s.m), s.SnapshotPurge),
		"snapshotCreate": s.fwd.Handler(OwnerIDFromVolume(s.m), s.SnapshotCreate),
		"snapshotList":   s.fwd.Handler(OwnerIDFromVolume(s.m), s.SnapshotList),
		"snapshotGet":    s.fwd.Handler(OwnerIDFromVolume(s.m), s.SnapshotGet),
		"snapshotDelete": s.fwd.Handler(OwnerIDFromVolume(s.m), s.SnapshotDelete),
		"snapshotRevert": s.fwd.Handler(OwnerIDFromVolume(s.m), s.SnapshotRevert),
		"snapshotBackup": s.fwd.Handler(OwnerIDFromVolume(s.m), s.SnapshotBackup),
	}
	for name, action := range volumeActions {
		r.Methods("POST").Path("/v1/volumes/{name}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/backupvolumes").Handler(f(schemas, s.BackupVolumeList))
	r.Methods("GET").Path("/v1/backupvolumes/{volName}").Handler(f(schemas, s.BackupVolumeGet))
	backupActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"backupList":   s.BackupList,
		"backupGet":    s.BackupGet,
		"backupDelete": s.BackupDelete,
	}
	for name, action := range backupActions {
		r.Methods("POST").Path("/v1/backupvolumes/{volName}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/nodes").Handler(f(schemas, s.NodeList))
	r.Methods("GET").Path("/v1/nodes/{name}").Handler(f(schemas, s.NodeGet))
	r.Methods("PUT").Path("/v1/nodes/{name}").Handler(f(schemas, s.NodeUpdate))
	nodeActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"diskUpdate": s.fwd.Handler(OwnerIDFromNode(s.m), s.DiskUpdate),
	}
	for name, action := range nodeActions {
		r.Methods("POST").Path("/v1/nodes/{name}").Queries("action", name).Handler(f(schemas, action))
	}

	r.Methods("GET").Path("/v1/engineimages").Handler(f(schemas, s.EngineImageList))
	r.Methods("GET").Path("/v1/engineimages/{name}").Handler(f(schemas, s.EngineImageGet))
	r.Methods("DELETE").Path("/v1/engineimages/{name}").Handler(f(schemas, s.EngineImageDelete))
	r.Methods("POST").Path("/v1/engineimages").Handler(f(schemas, s.EngineImageCreate))

	settingListStream := NewStreamHandlerFunc("settings", s.wsc.NewWatcher("setting"), s.settingList)
	r.Path("/v1/ws/settings").Handler(f(schemas, settingListStream))
	r.Path("/v1/ws/{period}/settings").Handler(f(schemas, settingListStream))

	volumeListStream := NewStreamHandlerFunc("volumes", s.wsc.NewWatcher("volume", "engine", "replica"), s.volumeList)
	r.Path("/v1/ws/volumes").Handler(f(schemas, volumeListStream))
	r.Path("/v1/ws/{period}/volumes").Handler(f(schemas, volumeListStream))

	nodeListStream := NewStreamHandlerFunc("nodes", s.wsc.NewWatcher("node"), s.nodeList)
	r.Path("/v1/ws/nodes").Handler(f(schemas, nodeListStream))
	r.Path("/v1/ws/{period}/nodes").Handler(f(schemas, nodeListStream))

	engineImageStream := NewStreamHandlerFunc("engineimages", s.wsc.NewWatcher("engineImage"), s.engineImageList)
	r.Path("/v1/ws/engineimages").Handler(f(schemas, engineImageStream))
	r.Path("/v1/ws/{period}/engineimages").Handler(f(schemas, engineImageStream))

	return r
}
