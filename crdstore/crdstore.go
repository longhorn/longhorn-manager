package crdstore

import (
	"fmt"
	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/crd/client"
	"github.com/rancher/longhorn-manager/crd/crdops"
	"github.com/rancher/longhorn-manager/crd/crdtype"
	"github.com/rancher/longhorn-manager/types"

	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type CRDStore struct {
	Prefix   string
	Operator *crdops.CrdOp
}

const (
	keySettings = "settings"
)

// note
func GetClientConfig(kubeconfig string) (*rest.Config, error) {
	if kubeconfig != "" {
		return clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	return rest.InClusterConfig()
}

func NewCRDStore(prefix string, kubeconfig string) (*CRDStore, error) {

	config, err := GetClientConfig(kubeconfig)
	if err != nil {
		return nil, err
	}

	cliset, err := apiextcs.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	client, err := crdclient.CreateClient(cliset, config)
	if err != nil {
		return nil, err
	}

	return &CRDStore{
		Prefix:   prefix,
		Operator: client,
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

	CRDobj := crdtype.CrdNode{}
	crdtype.LhNode2CRDNode(node, &CRDobj, node.ID)

	var result crdtype.CrdNode
	if err := s.Operator.Create(&CRDobj, crdtype.CrdMap[crdtype.KeyNode].CrdPlural, &result); err != nil {
		if apierrors.IsAlreadyExists(err) {
			fmt.Printf("ALREADY EXISTS: %#v\n", result)
		}
		return err
	}

	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}

	logrus.Infof("Add node %v name %v longhorn-manager IP %v", node.ID, node.Name, node.IP)

	node.KVIndex = index
	return nil
}

func (s *CRDStore) UpdateNode(node *types.NodeInfo) error {

	if err := s.checkNode(node); err != nil {
		return err
	}

	CRDobj := crdtype.CrdNode{}
	CRDobj.ResourceVersion = strconv.FormatUint(node.KVIndex, 10)
	crdtype.LhNode2CRDNode(node, &CRDobj, node.ID)

	var result crdtype.CrdNode
	if err := s.Operator.Update(&CRDobj, crdtype.CrdMap[crdtype.KeyNode].CrdPlural, &result, node.ID); err != nil {
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

	if err := s.Operator.Delete(nodeID, crdtype.CrdMap[crdtype.KeyNode].CrdPlural, &metav1.DeleteOptions{}); err != nil {
		return errors.Wrapf(err, "unable to delete node %v", nodeID)
	}

	return nil
}

func (s *CRDStore) GetNode(key string) (*types.NodeInfo, error) {
	node := types.NodeInfo{}

	var result crdtype.CrdNode
	if err := s.Operator.Get(key, crdtype.CrdMap[crdtype.KeyNode].CrdPlural, &result); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "unable to get node")
	}

	crdtype.CRDNode2LhNode(&result, &node)
	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}

	node.KVIndex = index
	return &node, nil
}

func (s *CRDStore) ListNodes() (map[string]*types.NodeInfo, error) {
	nodeMap := make(map[string]*types.NodeInfo)

	var result crdtype.CrdNodeList
	if err := s.Operator.List(metav1.ListOptions{}, crdtype.CrdMap[crdtype.KeyNode].CrdPlural, &result); err != nil {
		return nil, err
	}

	if len(result.Items) <= 0 {
		return nil, nil
	}

	var nodeList = make([]types.NodeInfo, len(result.Items))

	for i, item := range result.Items {
		crdtype.CRDNode2LhNode(&item, &nodeList[i])
		index, err := strconv.ParseUint(item.ResourceVersion, 10, 64)
		if err != nil {
			return nil, err
		}

		nodeList[i].KVIndex = index
		nodeMap[nodeList[i].ID] = &nodeList[i]
	}
	return nodeMap, nil
}

func (s *CRDStore) CreateSettings(settings *types.SettingsInfo) error {

	CRDobj := crdtype.CrdSetting{}
	crdtype.LhSetting2CRDSetting(settings, &CRDobj, keySettings)

	var result crdtype.CrdSetting
	if err := s.Operator.Create(&CRDobj, crdtype.CrdMap[crdtype.KeySetting].CrdPlural, &result); err != nil {
		if apierrors.IsAlreadyExists(err) {
			fmt.Printf("ALREADY EXISTS: %#v\n", result)
		}
		return err
	}

	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}

	settings.KVIndex = index
	return nil
}

func (s *CRDStore) UpdateSettings(settings *types.SettingsInfo) error {

	CRDobj := crdtype.CrdSetting{}
	CRDobj.ResourceVersion = strconv.FormatUint(settings.KVIndex, 10)
	crdtype.LhSetting2CRDSetting(settings, &CRDobj, keySettings)

	var result crdtype.CrdSetting
	if err := s.Operator.Update(&CRDobj, crdtype.CrdMap[crdtype.KeySetting].CrdPlural, &result, keySettings); err != nil {
		return err
	}

	index, err := strconv.ParseUint(result.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}

	settings.KVIndex = index
	return nil
}

func (s *CRDStore) GetSettings() (*types.SettingsInfo, error) {
	var result crdtype.CrdSetting

	if err := s.Operator.Get(keySettings, crdtype.CrdMap[crdtype.KeySetting].CrdPlural, &result); err != nil {
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
