package api

import (
	"net/http"

	"github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
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
	r.Methods("GET").Path("/v1").Handler(versionHandler)
	r.Methods("GET").Path("/v1/apiversions").Handler(versionsHandler)
	r.Methods("GET").Path("/v1/apiversions/v1").Handler(versionHandler)
	r.Methods("GET").Path("/v1/schemas").Handler(api.SchemasHandler(schemas))
	r.Methods("GET").Path("/v1/schemas/{id}").Handler(api.SchemaHandler(schemas))

	r.Methods("GET").Path("/v1/settings").Handler(f(schemas, s.SettingsList))
	r.Methods("GET").Path("/v1/settings/{name}").Handler(f(schemas, s.SettingsGet))
	r.Methods("PUT").Path("/v1/settings/{name}").Handler(f(schemas, s.SettingsSet))

	r.Methods("GET").Path("/v1/volumes").Handler(f(schemas, s.VolumeList))
	r.Methods("GET").Path("/v1/volumes/{name}").Handler(f(schemas, s.VolumeGet))
	r.Methods("DELETE").Path("/v1/volumes/{name}").Handler(f(schemas, s.VolumeDelete))
	r.Methods("POST").Path("/v1/volumes").Handler(f(schemas, s.VolumeCreate))

	volumeActions := map[string]func(http.ResponseWriter, *http.Request) error{
		"attach":          s.VolumeAttach,
		"detach":          s.VolumeDetach,
		"salvage":         s.VolumeSalvage,
		"recurringUpdate": s.VolumeRecurringUpdate,

		"snapshotPurge":  s.fwd.Handler(OwnerIDFromVolume(s.ds), s.SnapshotPurge),
		"snapshotCreate": s.fwd.Handler(OwnerIDFromVolume(s.ds), s.SnapshotCreate),
		"snapshotList":   s.fwd.Handler(OwnerIDFromVolume(s.ds), s.SnapshotList),
		"snapshotGet":    s.fwd.Handler(OwnerIDFromVolume(s.ds), s.SnapshotGet),
		"snapshotDelete": s.fwd.Handler(OwnerIDFromVolume(s.ds), s.SnapshotDelete),
		"snapshotRevert": s.fwd.Handler(OwnerIDFromVolume(s.ds), s.SnapshotRevert),
		"snapshotBackup": s.fwd.Handler(OwnerIDFromVolume(s.ds), s.SnapshotBackup),

		"replicaRemove": s.fwd.Handler(OwnerIDFromVolume(s.ds), s.ReplicaRemove),
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

	r.Methods("GET").Path("/v1/hosts").Handler(f(schemas, s.NodeList))
	r.Methods("GET").Path("/v1/hosts/{id}").Handler(f(schemas, s.NodeGet))

	return r
}
