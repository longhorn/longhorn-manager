package api

import (
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/longhorn/longhorn-manager/datastore"
	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"
	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (s *Server) ObjectStoreList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)
	col, err := s.objectStoreList(apiContext)
	if err != nil {
		return err
	}
	apiContext.Write(col)
	return nil
}

func (s *Server) objectStoreList(apiContext *api.ApiContext) (*client.GenericCollection, error) {
	list, err := s.m.ListObjectStoresSorted()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list object stores")
	}

	data := []interface{}{}

	for _, store := range list {
		vol, err := s.m.Get(fmt.Sprintf("pv-%v", store.Name))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get volume for %v", store.Name)
		}
		dpls, err := s.m.ListDeploymentsByLabels(types.GetObjectStoreLabels(store))
		if err != nil || len(dpls) == 0 {
			return nil, errors.Wrapf(err, "failed to get deployment for %v", store.Name)
		} else if len(dpls) > 1 {
			return nil, errors.Wrapf(err, "found multiple deployments for %v", store.Name)
		}
		data = append(data,
			toObjectStoreResource(store,
				vol.Spec.Size,
				vol.Status.ActualSize,
				util.GetImageOfDeploymentContainerWithName(dpls[0], types.ObjectStoreContainerName),
				util.GetImageOfDeploymentContainerWithName(dpls[0], types.ObjectStoreUIContainerName),
			),
		)
	}

	return &client.GenericCollection{
		Data: data,
		Collection: client.Collection{
			ResourceType: "objectStore",
		},
	}, nil
}

func (s *Server) ObjectStoreCreate(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	var input ObjectStoreInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	size, err := resource.ParseQuantity(input.Size)
	if err != nil {
		return err
	}

	logrus.Debugf("Creating Object Store of Size %v", size)
	// find a new name for the secret and create a new secret to seed the
	// credentials of the object store.
	var secretName string = input.Name
	for {
		_, err := s.m.GetSecret(s.m.GetLonghornNamespace(), secretName)
		if err != nil && datastore.ErrorIsNotFound(err) {
			break
		} else if err != nil {
			return errors.Wrapf(err, "API error while searching for secret %v", secretName)
		} else {
			secretName = fmt.Sprintf("%v-%v", input.Name, util.RandomString(6))
		}
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: s.m.GetLonghornNamespace(),
		},
		StringData: map[string]string{
			"RGW_DEFAULT_USER_ACCESS_KEY": input.AccessKey,
			"RGW_DEFAULT_USER_SECRET_KEY": input.SecretKey,
		},
	}

	_, err = s.m.CreateSecret(secret)
	if err != nil {
		return errors.Wrapf(err, "failed to create secret %v", input.Name)
	}

	store := &longhorn.ObjectStore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      input.Name,
			Namespace: s.m.GetLonghornNamespace(),
		},
		Spec: longhorn.ObjectStoreSpec{
			Size: size,
			VolumeParameters: longhorn.ObjectStoreVolumeParameterSpec{
				NumberOfReplicas:            input.NumberOfReplicas,
				ReplicaSoftAntiAffinity:     input.ReplicaSoftAntiAffinity,
				ReplicaZoneSoftAntiAffinity: input.ReplicaZoneSoftAntiAffinity,
				ReplicaDiskSoftAntiAffinity: input.ReplicaDiskSoftAntiAffinity,
				DiskSelector:                input.DiskSelector,
				NodeSelector:                input.NodeSelector,
				DataLocality:                input.DataLocality,
				FromBackup:                  input.FromBackup,
				StaleReplicaTimeout:         input.StaleReplicaTimeout,
				RecurringJobSelector:        input.RecurringJobSelector,
				ReplicaAutoBalance:          input.ReplicaAutoBalance,
				RevisionCounterDisabled:     input.RevisionCounterDisabled,
				UnmapMarkSnapChainRemoved:   input.UnmapMarkSnapChainRemoved,
				BackendStoreDriver:          input.BackendStoreDriver,
			},
			Credentials: corev1.SecretReference{
				Name:      secret.Name,
				Namespace: secret.Namespace,
			},
			Endpoints:   []longhorn.ObjectStoreEndpointSpec{},
			TargetState: "running",
		},
	}

	for i, endpoint := range input.Endpoints {
		endpointspec := longhorn.ObjectStoreEndpointSpec{
			Name:       fmt.Sprintf("endpoint-%v", i),
			DomainName: endpoint.DomainName,
		}

		if endpoint.SecretName != "" {
			endpointspec.TLS = corev1.SecretReference{
				Name:      endpoint.SecretName,
				Namespace: endpoint.SecretNamespace,
			}
		}

		store.Spec.Endpoints = append(store.Spec.Endpoints, endpointspec)
	}

	obj, err := s.m.CreateObjectStore(store)
	if err != nil {
		return errors.Wrapf(err, "failed to create object store %v", input.Name)
	}

	// Have to fake the size information because the actual volume isn't yet
	// provisioned and can't be queried. These values are accurate enough for a
	// start
	resp := toObjectStoreResource(obj, (&size).Value(), 0, input.Image, input.UIImage)
	apiContext.Write(resp)
	return nil
}

func (s *Server) ObjectStoreUpdate(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	var input ObjectStoreInput
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	store, err := s.m.GetObjectStore(input.Name)
	if err != nil {
		return err
	}

	vol, err := s.m.Get(fmt.Sprintf("pv-%v", input.Name))
	if err != nil {
		return err
	}

	if input.Size != "" {
		oldSize, err := util.ConvertSize(store.Spec.Size)
		size, err := util.ConvertSize(input.Size)
		if err != nil {
			return errors.Wrapf(err, "failed to parse size")
		}

		if oldSize > size {
			return fmt.Errorf("new object store size must be larger than the old one: %v", oldSize)
		}

		_, err = util.RetryOnConflictCause(func() (interface{}, error) { return s.m.Expand(vol.Name, size) })
		if err != nil {
			return errors.Wrapf(err, "failed to expand volume")
		}
		store.Spec.Size = resource.MustParse(input.Size)
	}

	if input.TargetState != "" {
		store.Spec.TargetState = input.TargetState
	}

	if input.Image != "" {
		store.Spec.Image = input.Image
	}

	if input.UIImage != "" {
		store.Spec.UIImage = input.UIImage
	}

	obj, err := s.m.UpdateObjectStore(store)
	resp := toObjectStoreResource(obj, vol.Spec.Size, vol.Status.ActualSize, input.Image, input.UIImage)
	apiContext.Write(resp)
	return nil
}

func (s *Server) ObjectStoreDelete(rw http.ResponseWriter, req *http.Request) (err error) {
	id := mux.Vars(req)["name"]
	if err := s.m.DeleteObjectStore(id); err != nil {
		return errors.Wrapf(err, "failed to delete object store %v", id)
	}
	return nil
}
