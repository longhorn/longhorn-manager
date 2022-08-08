package backend

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
)

type ClientID string
type RevokeFileHandle string

type RecoveryBackend struct {
	Namespace string
	Datastore *datastore.DataStore
}

func newConfigMap(namespace, name, version string) *v1.ConfigMap {
	return &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
		},
		Data: map[string]string{
			version: "{}",
		},
	}
}

func (rb *RecoveryBackend) CreateConfigMap(configMapName, version string) error {
	cm := newConfigMap(rb.Namespace, configMapName, version)

	if _, err := rb.Datastore.CreateConfigMap(cm); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "failed to create a configmap %v", configMapName)
		}

		cm, err := rb.Datastore.GetConfigMap(rb.Namespace, configMapName)
		if err != nil {
			return errors.Wrapf(err, "failed to get the configmap %v", configMapName)
		}

		cm.Data[version] = "{}"
		if _, err = rb.Datastore.UpdateConfigMap(cm); err != nil {
			return errors.Wrapf(err, "failed to update the configmap %v", configMapName)
		}
	}

	return nil
}

func (rb *RecoveryBackend) EndGrace(configMapName, version string) error {
	cm, err := rb.Datastore.GetConfigMap(rb.Namespace, configMapName)
	if err != nil {
		return errors.Wrapf(err, "failed to get the configmap %v", configMapName)
	}

	oldVersion := cm.Annotations["version"]

	// Switch to latest version
	annotations := map[string]string{
		"version": version,
	}
	cm.ObjectMeta.Annotations = annotations

	// Remove old data
	delete(cm.Data, oldVersion)
	_, err = rb.Datastore.UpdateConfigMap(cm)

	return err
}

func (rb *RecoveryBackend) AddClientID(configMapName, version string, clientID ClientID) error {
	logrus.Infof("Add client '%v' in recovery backend %v (version %v)", clientID, configMapName, version)

	cm, err := rb.Datastore.GetConfigMap(rb.Namespace, configMapName)
	if err != nil {
		return errors.Wrapf(err, "failed to get configmap %v", configMapName)
	}

	dataStr := cm.Data[version]
	if dataStr == "" {
		return errors.Wrapf(err, "failed to get data from recovery backend %v (version %v)", configMapName, version)
	}

	data := map[ClientID][]RevokeFileHandle{}
	if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
		return errors.Wrapf(err, "unable to decode data from recovery backend %v (version %v)", configMapName, version)
	}

	if _, ok := data[clientID]; ok {
		logrus.Infof("client %v is existing in recovery backend %v", clientID, configMapName)
		return nil
	}

	data[clientID] = []RevokeFileHandle{}
	dataByte, err := json.Marshal(data)
	if err != nil {
		return errors.Wrapf(err, "unable to encode data (recovery backend=%v, version %v)", configMapName, version)
	}

	cm.Data[version] = string(dataByte)
	_, err = rb.Datastore.UpdateConfigMap(cm)

	return err
}

func (rb *RecoveryBackend) RemoveClientID(configMapName string, clientID ClientID) error {
	logrus.Infof("Remove client '%v' in recovery backend %v", clientID, configMapName)

	cm, err := rb.Datastore.GetConfigMap(rb.Namespace, configMapName)
	if err != nil {
		return errors.Wrapf(err, "failed to get configmap %v", configMapName)
	}

	version, ok := cm.Annotations["version"]
	if !ok {
		return errors.Wrapf(err, "failed to get version from configmap %v", configMapName)
	}

	dataStr := cm.Data[version]
	if dataStr == "" {
		return errors.Wrapf(err, "failed to get data from recovery backend %v (version %v)", configMapName, version)
	}

	data := map[ClientID][]RevokeFileHandle{}
	if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
		return errors.Wrapf(err, "unable to decode data from recovery backend %v (version %v)", configMapName, version)
	}

	delete(data, clientID)

	dataByte, err := json.Marshal(data)
	if err != nil {
		return errors.Wrapf(err, "unable to encode data (recovery backend=%v, version %v)", configMapName, version)
	}

	cm.Data[version] = string(dataByte)
	_, err = rb.Datastore.UpdateConfigMap(cm)

	return err
}

func (rb *RecoveryBackend) ReadClientIDs(configMapName string) ([]string, error) {
	logrus.Infof("Read clients from recovery backend %v", configMapName)

	cm, err := rb.Datastore.GetConfigMap(rb.Namespace, configMapName)
	if err != nil {
		return []string{}, errors.Wrapf(err, "failed to get configmap %v", configMapName)
	}

	version, ok := cm.Annotations["version"]
	if !ok {
		logrus.Infof("Annotation version is not existing in recovery backend %v", configMapName)
		return []string{}, nil
	}

	dataStr := cm.Data[version]
	if dataStr == "" {
		return []string{}, errors.Wrapf(err, "failed to get data from recovery backend %v (version %v)", configMapName, version)
	}

	data := map[ClientID][]RevokeFileHandle{}
	if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
		return []string{}, errors.Wrapf(err, "unable to decode data from recovery backend %v (version %v)", configMapName, version)
	}

	clients := []string{}
	for c := range data {
		clients = append(clients, string(c))
	}

	return clients, nil
}

func (rb *RecoveryBackend) AddRevokeFilehandle(configMapName, version string, clientID ClientID, revokeFilehandle RevokeFileHandle) error {
	logrus.Infof("Add client %v revoke filehandle %v into recovery backend %v (version %v)",
		clientID, revokeFilehandle, configMapName, version)

	cm, err := rb.Datastore.GetConfigMap(rb.Namespace, configMapName)
	if err != nil {
		return errors.Wrapf(err, "failed to get configmap %v", configMapName)
	}

	dataStr := cm.Data[version]
	if dataStr == "" {
		return errors.Wrapf(err, "failed to get data from recovery backend %v (version %v)", configMapName, version)
	}

	data := map[ClientID][]RevokeFileHandle{}
	if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
		return errors.Wrapf(err, "unable to decode data from recovery backend %v (version %v)", configMapName, version)
	}

	if _, ok := data[clientID]; !ok {
		return fmt.Errorf("client %v is not existing in recovery backend %v (version %v)", clientID, configMapName, version)
	}

	revokeFilehandles := data[clientID]
	if len(revokeFilehandles) == 0 {
		revokeFilehandles = []RevokeFileHandle{}
	}
	revokeFilehandles = append(revokeFilehandles, revokeFilehandle)
	data[clientID] = revokeFilehandles

	dataByte, err := json.Marshal(data)
	if err != nil {
		return errors.Wrapf(err, "unable to encode data (recovery backend %v, version %v)", configMapName, version)
	}

	cm.Data[version] = string(dataByte)
	_, err = rb.Datastore.UpdateConfigMap(cm)

	return err
}
