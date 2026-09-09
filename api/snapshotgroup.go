package api

import (
	"fmt"
	"net/http"

	"github.com/cockroachdb/errors"
	"github.com/gorilla/mux"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

// SnapshotGroupAction dispatches POST /v1/snapshotgroups by the action query
// parameter. A POST without an action is the conventional create request;
// unknown actions are rejected so a mistyped action cannot fall through to
// create.
func (s *Server) SnapshotGroupAction(rw http.ResponseWriter, req *http.Request) error {
	switch action := req.URL.Query().Get("action"); action {
	case "":
		return s.SnapshotGroupCreate(rw, req)
	case "preview":
		return s.SnapshotGroupPreview(rw, req)
	default:
		writeErr(rw, req, fmt.Errorf("unknown snapshot group action %q; supported action: preview", action), http.StatusBadRequest)
		return nil
	}
}

func (s *Server) SnapshotGroupList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	sgl, err := s.snapshotGroupList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(sgl)
	return nil
}

func (s *Server) snapshotGroupList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListSnapshotGroupsSorted()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list snapshot groups")
	}
	return toSnapshotGroupCollection(list, apiContext), nil
}

func (s *Server) SnapshotGroupGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]

	snapshotGroup, err := s.m.GetSnapshotGroup(id)
	if err != nil {
		return errors.Wrapf(err, "failed to get snapshot group %v", id)
	}
	apiContext.Write(toSnapshotGroupResource(snapshotGroup, apiContext))
	return nil
}

func (s *Server) SnapshotGroupCreate(rw http.ResponseWriter, req *http.Request) error {
	var input SnapshotGroup
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	obj, err := s.m.CreateSnapshotGroup(input.Name, &longhorn.SnapshotGroupSpec{
		Volumes:         input.Volumes,
		VolumeSelector:  input.VolumeSelector,
		Labels:          input.Labels,
		DeadlineSeconds: input.DeadlineSeconds,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to create snapshot group %v", input.Name)
	}
	apiContext.Write(toSnapshotGroupResource(obj, apiContext))
	return nil
}

// SnapshotGroupPreview previews which volumes a snapshot group spec would
// select, without creating the group. Selection failures are reported in the
// preview body, not as HTTP errors.
func (s *Server) SnapshotGroupPreview(rw http.ResponseWriter, req *http.Request) error {
	var input SnapshotGroupPreviewInput
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	preview := &SnapshotGroupPreviewOutput{
		Resource: client.Resource{
			Type: "snapshotGroupPreviewOutput",
		},
	}
	candidates, err := s.m.ResolveSnapshotGroupMemberCandidates(&longhorn.SnapshotGroupSpec{
		Volumes:        input.Volumes,
		VolumeSelector: input.VolumeSelector,
	})
	if err != nil {
		preview.Error = err.Error()
	}
	for _, candidate := range candidates {
		preview.Members = append(preview.Members, SnapshotGroupPreviewMember{
			VolumeName:        candidate.VolumeName,
			ValidationFailure: candidate.ValidationFailure,
		})
	}
	apiContext.Write(preview)
	return nil
}

func (s *Server) SnapshotGroupDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteSnapshotGroup(id); err != nil {
		return errors.Wrapf(err, "failed to delete snapshot group %v", id)
	}

	return nil
}
