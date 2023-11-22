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
		dpls, err := s.m.ListDeploymentsByLabels(types.GetObjectStoreLabels(store))
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get deployment for %v", store.Name)
		} else if len(dpls) == 0 {
			// this object store might still be in its creation phase
			data = append(data,
				toObjectStoreResource(store,
					store.Spec.Size.Value(),
					0,
					store.Spec.Image,
					store.Spec.UIImage,
				),
			)
			continue
		} else if len(dpls) > 1 {
			return nil, errors.Wrapf(err, "found multiple deployments for %v", store.Name)
		}

		vol, err := s.m.Get(store.Name)
		if err != nil {
			if datastore.ErrorIsNotFound(err) {
				// this object store might still be in its creation phase
				data = append(data,
					toObjectStoreResource(store,
						store.Spec.Size.Value(),
						0,
						store.Spec.Image,
						store.Spec.UIImage,
					),
				)
				continue
			}
			return nil, errors.Wrapf(err, "failed to get volume for %v", store.Name)
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

	// This whole lot is quite crappy. TODO move it to a separate function or
	// rethink the logic here some more
	labels := types.GetBaseLabelsForSystemManagedComponent()
	labels[types.GetLonghornLabelComponentKey()] = types.LonghornLabelObjectStore
	labels[types.GetLonghornLabelKey(types.LonghornLabelObjectStore)] = input.Name

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      input.Name,
			Namespace: s.m.GetLonghornNamespace(),
			Labels:    labels,
		},
		StringData: map[string]string{
			"RGW_DEFAULT_USER_ACCESS_KEY": input.AccessKey,
			"RGW_DEFAULT_USER_SECRET_KEY": input.SecretKey,
		},
	}

	logrus.Debugf("Creating Object Store of Size %v", size)
	sec, err := s.m.GetSecretRO(s.m.GetLonghornNamespace(), input.Name)
	if err != nil && datastore.ErrorIsNotFound(err) {
		// secret doesn't exist --> create it
		_, err = s.m.CreateSecret(secret)
		if err != nil {
			return errors.Wrapf(err, "failed to create secret %v", input.Name)
		}
	} else if err != nil {
		// api error --> abort
		return errors.Wrapf(err, "API error while searching for secret %v", input.Name)
	} else {
		if sec.Labels == nil {
			// secret was created by user --> error
			return fmt.Errorf("secret %v already exists, please clean up", secret.Name)
		}

		labelObjStore, okLabelObjStore := sec.Labels[types.GetLonghornLabelComponentKey()]
		labelManagedBy, okLabelManagedBy := sec.Labels[types.LonghornLabelManagedBy]
		if okLabelObjStore && labelObjStore == types.LonghornLabelObjectStore &&
			okLabelManagedBy && labelManagedBy == types.ControlPlaneName {
			// secret is managed by longhorn manager --> adopt this secret
			_, err = s.m.UpdateSecret(secret)
			if err != nil {
				return errors.Wrapf(err, "api error while adopting secret %v", input.Name)
			}
		} else {
			// secret was created by user --> error
			return fmt.Errorf("secret %v already exists, please clean up", secret.Name)
		}
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

	vol, err := s.m.Get(input.Name)
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
