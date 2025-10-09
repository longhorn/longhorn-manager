package backend

import (
	"encoding/json"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"
)

type ClientID string
type RevokeFileHandle string

type RecoveryBackend struct {
	Namespace string
	Datastore *datastore.DataStore
	Logger    logrus.FieldLogger
}

func (rb *RecoveryBackend) newConfigMap(namespace, name, version, smPodName string) (*v1.ConfigMap, error) {
	smPod, err := rb.Datastore.GetPod(smPodName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to get share manager pod %v", name)
	}

	smName, ok := smPod.Labels[types.GetLonghornLabelKey(types.LonghornLabelShareManager)]
	if !ok {
		return nil, errors.Wrapf(err, "failed to get label %v from share manager pod %v",
			types.GetLonghornLabelKey(types.LonghornLabelShareManager), smName)
	}
	return &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    types.GetShareManagerConfigMapLabels(smName),
		},
		Data: map[string]string{
			version: "{}",
		},
	}, nil
}

func (rb *RecoveryBackend) CreateConfigMap(hostname, version string) error {
	configMapName := types.GetConfigMapNameFromHostname(hostname)
	cm, err := rb.newConfigMap(rb.Namespace, configMapName, version, hostname)
	if err != nil {
		return err
	}

	if _, err := rb.Datastore.CreateConfigMap(cm); err != nil {
		if !apierrors.IsAlreadyExists(err) {
			return errors.Wrapf(err, "failed to create a configmap %v", configMapName)
		}

		cm, err := rb.Datastore.GetConfigMapWithoutCache(rb.Namespace, configMapName)
		if err != nil {
			return errors.Wrapf(err, "failed to get the configmap %v", configMapName)
		}

		if cm.Data == nil {
			cm.Data = map[string]string{}
		}

		cm.Data[version] = "{}"
		if _, err = rb.Datastore.UpdateConfigMap(cm); err != nil {
			return errors.Wrapf(err, "failed to update the configmap %v", configMapName)
		}
	}

	return nil
}

func (rb *RecoveryBackend) EndGrace(hostname, version string) error {
	configMapName := types.GetConfigMapNameFromHostname(hostname)

	_, err := util.RetryOnConflictCause(func() (interface{}, error) {
		cm, err := rb.Datastore.GetConfigMapWithoutCache(rb.Namespace, configMapName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get the configmap %v", configMapName)
		}

		oldVersion := cm.Annotations["version"]

		// Switch to latest version
		annotations := map[string]string{
			"version": version,
		}
		cm.Annotations = annotations

		// Remove old data
		delete(cm.Data, oldVersion)
		return rb.Datastore.UpdateConfigMap(cm)
	})

	return err
}

func (rb *RecoveryBackend) AddClientID(hostname, version string, clientID ClientID) error {
	configMapName := types.GetConfigMapNameFromHostname(hostname)

	rb.Logger.Infof("Adding client '%v' in recovery backend %v (version %v)", clientID, configMapName, version)

	_, err := util.RetryOnConflictCause(func() (interface{}, error) {
		cm, err := rb.Datastore.GetConfigMapWithoutCache(rb.Namespace, configMapName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get configmap %v", configMapName)
		}

		dataStr := cm.Data[version]
		if dataStr == "" {
			return nil, errors.Wrapf(err, "failed to get data from recovery backend %v (version %v)", configMapName, version)
		}

		data := map[ClientID][]RevokeFileHandle{}
		if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
			return nil, errors.Wrapf(err, "unable to decode data from recovery backend %v (version %v)", configMapName, version)
		}

		if _, ok := data[clientID]; ok {
			rb.Logger.Infof("Client %v is existing in recovery backend %v", clientID, configMapName)
			return nil, nil
		}

		data[clientID] = []RevokeFileHandle{}
		dataByte, err := json.Marshal(data)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to encode data (recovery backend=%v, version %v)", configMapName, version)
		}

		cm.Data[version] = string(dataByte)
		return rb.Datastore.UpdateConfigMap(cm)
	})

	return err
}

func (rb *RecoveryBackend) RemoveClientID(hostname string, clientID ClientID) error {
	configMapName := types.GetConfigMapNameFromHostname(hostname)
	rb.Logger.Infof("Removing client '%v' in recovery backend %v", clientID, configMapName)

	_, err := util.RetryOnConflictCause(func() (interface{}, error) {
		cm, err := rb.Datastore.GetConfigMapWithoutCache(rb.Namespace, configMapName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get configmap %v", configMapName)
		}

		version, ok := cm.Annotations["version"]
		if !ok {
			return nil, errors.Wrapf(err, "failed to get version from configmap %v", configMapName)
		}

		dataStr := cm.Data[version]
		if dataStr == "" {
			return nil, errors.Wrapf(err, "failed to get data from recovery backend %v (version %v)", configMapName, version)
		}

		data := map[ClientID][]RevokeFileHandle{}
		if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
			return nil, errors.Wrapf(err, "unable to decode data from recovery backend %v (version %v)", configMapName, version)
		}

		delete(data, clientID)

		dataByte, err := json.Marshal(data)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to encode data (recovery backend=%v, version %v)", configMapName, version)
		}

		cm.Data[version] = string(dataByte)
		return rb.Datastore.UpdateConfigMap(cm)
	})

	return err
}

func (rb *RecoveryBackend) ReadClientIDs(hostname string) ([]string, error) {
	configMapName := types.GetConfigMapNameFromHostname(hostname)
	rb.Logger.Infof("Reading clients from recovery backend %v", configMapName)

	cm, err := rb.Datastore.GetConfigMapWithoutCache(rb.Namespace, configMapName)
	if err != nil {
		return []string{}, errors.Wrapf(err, "failed to get configmap %v", configMapName)
	}

	version, ok := cm.Annotations["version"]
	if !ok {
		rb.Logger.Infof("Annotation version is not existing in recovery backend %v", configMapName)
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

func (rb *RecoveryBackend) AddRevokeFilehandle(hostname, version string, clientID ClientID, revokeFilehandle RevokeFileHandle) error {
	configMapName := types.GetConfigMapNameFromHostname(hostname)
	rb.Logger.Infof("Adding client %v revoke filehandle %v into recovery backend %v (version %v)",
		clientID, revokeFilehandle, configMapName, version)

	_, err := util.RetryOnConflictCause(func() (interface{}, error) {

		cm, err := rb.Datastore.GetConfigMapWithoutCache(rb.Namespace, configMapName)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get configmap %v", configMapName)
		}

		dataStr := cm.Data[version]
		if dataStr == "" {
			return nil, errors.Wrapf(err, "failed to get data from recovery backend %v (version %v)", configMapName, version)
		}

		data := map[ClientID][]RevokeFileHandle{}
		if err := json.Unmarshal([]byte(dataStr), &data); err != nil {
			return nil, errors.Wrapf(err, "unable to decode data from recovery backend %v (version %v)", configMapName, version)
		}

		if _, ok := data[clientID]; !ok {
			return nil, fmt.Errorf("client %v is not existing in recovery backend %v (version %v)", clientID, configMapName, version)
		}

		revokeFilehandles := data[clientID]
		if len(revokeFilehandles) == 0 {
			revokeFilehandles = []RevokeFileHandle{}
		}
		revokeFilehandles = append(revokeFilehandles, revokeFilehandle)
		data[clientID] = revokeFilehandles

		dataByte, err := json.Marshal(data)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to encode data (recovery backend %v, version %v)", configMapName, version)
		}

		cm.Data[version] = string(dataByte)
		return rb.Datastore.UpdateConfigMap(cm)
	})
	return err
}
