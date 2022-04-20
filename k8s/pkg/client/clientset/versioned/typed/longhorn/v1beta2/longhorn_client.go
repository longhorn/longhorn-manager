/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Code generated by client-gen. DO NOT EDIT.

package v1beta2

import (
	v1beta2 "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	"github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/scheme"
	rest "k8s.io/client-go/rest"
)

type LonghornV1beta2Interface interface {
	RESTClient() rest.Interface
	BackingImagesGetter
	BackingImageDataSourcesGetter
	BackingImageManagersGetter
	BackupsGetter
	BackupTargetsGetter
	BackupVolumesGetter
	EnginesGetter
	EngineImagesGetter
	InstanceManagersGetter
	NodesGetter
	OrphansGetter
	RecurringJobsGetter
	ReplicasGetter
	SettingsGetter
	ShareManagersGetter
	SnapshotsGetter
	VolumesGetter
}

// LonghornV1beta2Client is used to interact with features provided by the longhorn.io group.
type LonghornV1beta2Client struct {
	restClient rest.Interface
}

func (c *LonghornV1beta2Client) BackingImages(namespace string) BackingImageInterface {
	return newBackingImages(c, namespace)
}

func (c *LonghornV1beta2Client) BackingImageDataSources(namespace string) BackingImageDataSourceInterface {
	return newBackingImageDataSources(c, namespace)
}

func (c *LonghornV1beta2Client) BackingImageManagers(namespace string) BackingImageManagerInterface {
	return newBackingImageManagers(c, namespace)
}

func (c *LonghornV1beta2Client) Backups(namespace string) BackupInterface {
	return newBackups(c, namespace)
}

func (c *LonghornV1beta2Client) BackupTargets(namespace string) BackupTargetInterface {
	return newBackupTargets(c, namespace)
}

func (c *LonghornV1beta2Client) BackupVolumes(namespace string) BackupVolumeInterface {
	return newBackupVolumes(c, namespace)
}

func (c *LonghornV1beta2Client) Engines(namespace string) EngineInterface {
	return newEngines(c, namespace)
}

func (c *LonghornV1beta2Client) EngineImages(namespace string) EngineImageInterface {
	return newEngineImages(c, namespace)
}

func (c *LonghornV1beta2Client) InstanceManagers(namespace string) InstanceManagerInterface {
	return newInstanceManagers(c, namespace)
}

func (c *LonghornV1beta2Client) Nodes(namespace string) NodeInterface {
	return newNodes(c, namespace)
}

func (c *LonghornV1beta2Client) Orphans(namespace string) OrphanInterface {
	return newOrphans(c, namespace)
}

func (c *LonghornV1beta2Client) RecurringJobs(namespace string) RecurringJobInterface {
	return newRecurringJobs(c, namespace)
}

func (c *LonghornV1beta2Client) Replicas(namespace string) ReplicaInterface {
	return newReplicas(c, namespace)
}

func (c *LonghornV1beta2Client) Settings(namespace string) SettingInterface {
	return newSettings(c, namespace)
}

func (c *LonghornV1beta2Client) ShareManagers(namespace string) ShareManagerInterface {
	return newShareManagers(c, namespace)
}

func (c *LonghornV1beta2Client) Snapshots(namespace string) SnapshotInterface {
	return newSnapshots(c, namespace)
}

func (c *LonghornV1beta2Client) Volumes(namespace string) VolumeInterface {
	return newVolumes(c, namespace)
}

// NewForConfig creates a new LonghornV1beta2Client for the given config.
func NewForConfig(c *rest.Config) (*LonghornV1beta2Client, error) {
	config := *c
	if err := setConfigDefaults(&config); err != nil {
		return nil, err
	}
	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, err
	}
	return &LonghornV1beta2Client{client}, nil
}

// NewForConfigOrDie creates a new LonghornV1beta2Client for the given config and
// panics if there is an error in the config.
func NewForConfigOrDie(c *rest.Config) *LonghornV1beta2Client {
	client, err := NewForConfig(c)
	if err != nil {
		panic(err)
	}
	return client
}

// New creates a new LonghornV1beta2Client for the given RESTClient.
func New(c rest.Interface) *LonghornV1beta2Client {
	return &LonghornV1beta2Client{c}
}

func setConfigDefaults(config *rest.Config) error {
	gv := v1beta2.SchemeGroupVersion
	config.GroupVersion = &gv
	config.APIPath = "/apis"
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()

	if config.UserAgent == "" {
		config.UserAgent = rest.DefaultKubernetesUserAgent()
	}

	return nil
}

// RESTClient returns a RESTClient that is used to communicate
// with API server by this client implementation.
func (c *LonghornV1beta2Client) RESTClient() rest.Interface {
	if c == nil {
		return nil
	}
	return c.restClient
}
