package api

import (
	"encoding/base64"
	"fmt"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/pkg/errors"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/rancher/go-rancher/api"
	"github.com/rancher/go-rancher/client"

	"github.com/longhorn/longhorn-manager/util"
)

func (s *Server) SecretList(rw http.ResponseWriter, req *http.Request) (err error) {
	apiContext := api.GetApiContext(req)

	secrets, err := s.secretList()
	if err != nil {
		return err
	}
	apiContext.Write(secrets)
	return nil
}

func (s *Server) secretList() (*client.GenericCollection, error) {
	list, err := s.m.ListSecretsSorted()
	if err != nil {
		return nil, errors.Wrap(err, "failed to list secrets")
	}
	return toSecretCollection(list), nil
}

func (s *Server) SecretGet(rw http.ResponseWriter, req *http.Request) error {
	apiContext := api.GetApiContext(req)

	secretName := mux.Vars(req)["name"]

	secret, err := s.m.GetSecret(secretName)
	if err != nil {
		return errors.Wrapf(err, "failed to get secret '%s'", secretName)
	}
	apiContext.Write(toSecretResource(secret))
	return nil
}

func (s *Server) SecretCreate(rw http.ResponseWriter, req *http.Request) error {
	var input SecretInput
	apiContext := api.GetApiContext(req)

	if err := apiContext.Read(&input); err != nil {
		return err
	}

	secretData := make(map[string][]byte, len(input.Data))
	for key, data := range input.Data {
		decodeData, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			return err
		}
		secretData[key] = decodeData
	}

	obj, err := s.m.CreateSecret(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name: input.Name,
		},
		Data: secretData,
		Type: corev1.SecretType(input.Type),
	})
	if err != nil {
		return errors.Wrapf(err, "failed to create secret %v", input.Name)
	}
	apiContext.Write(toSecretResource(obj))
	return nil
}

func (s *Server) SecretUpdate(rw http.ResponseWriter, req *http.Request) error {
	var input SecretInput

	apiContext := api.GetApiContext(req)
	if err := apiContext.Read(&input); err != nil {
		return err
	}

	secretName := mux.Vars(req)["name"]
	secretData := make(map[string][]byte, len(input.Data))
	for key, data := range input.Data {
		decodeData, err := base64.StdEncoding.DecodeString(data)
		if err != nil {
			return err
		}
		secretData[key] = decodeData
	}
	obj, err := util.RetryOnConflictCause(func() (interface{}, error) {
		return s.m.UpdateSecret(&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{
				Name: secretName,
			},
			Data: secretData,
			Type: corev1.SecretType(input.Type),
		})
	})
	if err != nil {
		return err
	}
	secret, ok := obj.(*corev1.Secret)
	if !ok {
		return fmt.Errorf("failed to convert %v to secret object", secretName)
	}

	apiContext.Write(toSecretResource(secret))
	return nil
}

func (s *Server) SecretDelete(rw http.ResponseWriter, req *http.Request) error {
	secretName := mux.Vars(req)["name"]
	if err := s.m.DeleteSecret(secretName); err != nil {
		return errors.Wrapf(err, "failed to delete secret %v", secretName)
	}

	return nil
}
