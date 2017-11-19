package crdstore

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-manager/types"
	"strconv"
	"github.com/rancher/longhorn-manager/crd/crdops"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/rancher/longhorn-manager/crd/crdtype"
	"github.com/rancher/longhorn-manager/crd/clients"
)

type CRDStore struct {
	Prefix string
	VolumeOperator *crdops.CrdOp
	NodeOperator *crdops.CrdOp
	ReplicasOperator *crdops.CrdOp
	ControllerOperator *crdops.CrdOp
	SettingOperator *crdops.CrdOp
}

const (
	keySettings = "settings"
)

// note
func getClientConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func NewCRDStore(prefix string, kubeconfig string) (*CRDStore, error) {

	config, err := getClientConfig(kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create clientset and create our CRD, this only need to run once
	cs, err := apiextcs.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	return &CRDStore{
		Prefix: prefix,
		VolumeOperator: crdclient.CreateVolumeClient(cs, config),
		NodeOperator: crdclient.CreateNodeClient(cs, config),
		ReplicasOperator: crdclient.CreateReplicaClient(cs, config),
		ControllerOperator: crdclient.CreateControllerClient(cs, config),
		SettingOperator: crdclient.CreateSettingClient(cs, config),
	}, nil
}


func (s *CRDStore) checkNode(node *types.NodeInfo) error {
	if node.ID == "" || node.Name == "" || node.IP == "" {
		return fmt.Errorf("BUG: missing required field %+v", node)
	}
	return nil
}

func (s *CRDStore) CreateNode(node *types.NodeInfo) error {
	if err := s.checkNode(node); err != nil {
		return err
	}

	CRDobj := crdtype.Crdnode{}
	crdtype.LhNode2CRDNode(node, &CRDobj, node.ID)
	var result crdtype.Crdnode
	err := s.NodeOperator.Create(&CRDobj, &result)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			fmt.Printf("ALREADY EXISTS: %#v\n", result)
		}
		return  err
	}

	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}

	logrus.Infof("Add node %v name %v longhorn-manager IP %v", node.ID, node.Name, node.IP)

	node.KVIndex = index
	return  nil
}

func (s *CRDStore) UpdateNode(node *types.NodeInfo) error {

	if err := s.checkNode(node); err != nil {
		return err
	}

	CRDobj := crdtype.Crdnode{}
	CRDobj.ResourceVersion = strconv.FormatUint(node.KVIndex, 10)
	crdtype.LhNode2CRDNode(node, &CRDobj, node.ID)
	var result crdtype.Crdnode
	err := s.NodeOperator.Update(&CRDobj, &result, node.ID)
	if err != nil {
		return err
	}
	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}
	node.KVIndex = index

	return nil
}

func (s *CRDStore) DeleteNode(nodeID string) error {

	err := s.NodeOperator.Delete(nodeID, &meta_v1.DeleteOptions{})
	if err != nil {
		return errors.Wrapf(err, "unable to delete node %v", nodeID)
	}
	return nil
}

func (s *CRDStore) GetNode(key string) (*types.NodeInfo, error) {
	n := types.NodeInfo{}
	var result crdtype.Crdnode
	err := s.NodeOperator.Get(key, &result)

	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "unable to get node")
	}

	crdtype.CRDNode2LhNode(&result, &n)
	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}
	n.KVIndex = index
	return &n, nil
}

func (s *CRDStore) ListNodes() (map[string]*types.NodeInfo, error) {
	nodes := make(map[string]*types.NodeInfo)
	var result crdtype.CrdnodeList
	err := s.NodeOperator.List(meta_v1.ListOptions{}, &result)
	if err != nil {
		return nil, err
	}

	if len(result.Items) <= 0 {
		return nil, nil
	}

	var n = make([]types.NodeInfo, len(result.Items))

	for i, item := range result.Items {
		crdtype.CRDNode2LhNode(&item, &n[i])
		index, err := strconv.ParseUint(item.ResourceVersion, 10, 64)
		if err != nil {
			return nil, err
		}
		n[i].KVIndex = index
		nodes[n[i].ID] = &n[i]

	}
	return nodes, nil
}


func (s *CRDStore) CreateSettings(settings *types.SettingsInfo) error {

	CRDobj := crdtype.Crdsetting{}
	crdtype.LhSetting2CRDSetting(settings, &CRDobj, keySettings)
	var result crdtype.Crdsetting
	err := s.SettingOperator.Create(&CRDobj, &result)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			fmt.Printf("ALREADY EXISTS: %#v\n", result)
		}
		return  err
	}

	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}

	settings.KVIndex = index
	return nil
}


func (s *CRDStore) UpdateSettings(settings *types.SettingsInfo) error {

	CRDobj := crdtype.Crdsetting{}
	CRDobj.ResourceVersion = strconv.FormatUint(settings.KVIndex, 10)
	crdtype.LhSetting2CRDSetting(settings, &CRDobj, keySettings)
	var result crdtype.Crdsetting
	err := s.SettingOperator.Update(&CRDobj, &result, keySettings)
	if err != nil {
		return  err
	}

	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}
	settings.KVIndex = index
	return nil
}

func (s *CRDStore) GetSettings() (*types.SettingsInfo, error) {
	var result crdtype.Crdsetting
	err := s.SettingOperator.Get(keySettings, &result)

	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	settings := &types.SettingsInfo{}
	crdtype.CRDSetting2LhSetting(&result, settings)

	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}

	settings.KVIndex = index

	return settings, nil
}

// Nuclear is test only function, which will wipe all longhorn entries
func (s *CRDStore) Nuclear(nuclearCode string) error {
	if nuclearCode != "nuke key value store" {
		return errors.Errorf("invalid nuclear code!")
	}
	// TODO delete all crd resource
	return nil
}