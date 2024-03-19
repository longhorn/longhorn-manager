package api

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func (s *Server) RecurringJobList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	bil, err := s.recurringJobList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(bil)
	return nil
}

func (s *Server) recurringJobList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListRecurringJobsSorted()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list recurring job")
	}
	return toRecurringJobCollection(list, apiContext), nil
}

func (s *Server) RecurringJobGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	id := mux.Vars(req)["name"]

	job, err := s.m.GetRecurringJob(id)
	if err != nil {
		return errors.Wrapf(err, "failed to get recurring job policy '%s'", id)
	}
	apiContext.Write(toRecurringJobResource(job, apiContext))
	return nil
}

func (s *Server) RecurringJobCreate(rw http.ResponseWriter, req *http.Request) error {
	var input RecurringJob
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	obj, err := s.m.CreateRecurringJob(&longhorn.RecurringJobSpec{
		Name:        input.Name,
		Groups:      input.Groups,
		Task:        longhorn.RecurringJobType(input.Task),
		Cron:        input.Cron,
		Retain:      input.Retain,
		Concurrency: input.Concurrency,
		Labels:      input.Labels,
		Parameters:  input.Parameters,
	})
	if err != nil {
		return errors.Wrapf(err, "failed to create recurring job %v", input.Name)
	}
	apiContext.Write(toRecurringJobResource(obj, apiContext))
	return nil
}

func (s *Server) RecurringJobUpdate(rw http.ResponseWriter, req *http.Request) error {
	var input RecurringJob

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	name := mux.Vars(req)["name"]

	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateRecurringJob(longhorn.RecurringJobSpec{
			Name:        name,
			Groups:      input.Groups,
			Task:        longhorn.RecurringJobType(input.Task),
			Cron:        input.Cron,
			Retain:      input.Retain,
			Concurrency: input.Concurrency,
			Labels:      input.Labels,
			Parameters:  input.Parameters,
		})
	})
	if err != nil {
		return err
	}
	job, ok := obj.(*longhorn.RecurringJob)
	if !ok {
		return fmt.Errorf("failed to convert %v to recurring job object", name)
	}

	apiContext.Write(toRecurringJobResource(job, apiContext))
	return nil
}

func (s *Server) RecurringJobDelete(rw http.ResponseWriter, req *http.Request) error {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteRecurringJob(id); err != nil {
		return errors.Wrapf(err, "failed to delete recurring job %v", id)
	}

	return nil
}
