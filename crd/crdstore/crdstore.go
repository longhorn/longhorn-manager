package crdstore

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/longhorn-manager/types"
	"github.com/rancher/longhorn-manager/crd/types/ntype"
	"strconv"
	"github.com/rancher/longhorn-manager/crd/clients/vclient"
	"github.com/rancher/longhorn-manager/crd/clients/nclient"
	"github.com/rancher/longhorn-manager/crd/clients/rclient"
	"github.com/rancher/longhorn-manager/crd/clients/cclient"
	"github.com/rancher/longhorn-manager/crd/clients/sclient"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	apiextcs "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	meta_v1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"github.com/rancher/longhorn-manager/crd/types/stype"
)

type CRDStore struct {
	Prefix string
	VolumeClient *vclient.Crdclient
	NodeClient *nclient.Crdclient
	ReplicasClient *rclient.Crdclient
	ControllerClient *cclient.Crdclient
	SettingClient *sclient.Crdclient
}

const (
	keySettings = "settings"
)

// return rest config, if path not specified assume in cluster config
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
	//CreateVolumeController(lhbackend.VolumeClient)

	return &CRDStore{
		Prefix: prefix,
		VolumeClient: vclient.CreateVolumeClient(cs, config),
		NodeClient: nclient.CreateNodeClient(cs, config),
		ReplicasClient: rclient.CreateReplicaClient(cs, config),
		ControllerClient: cclient.CreateControllerClient(cs, config),
		SettingClient: sclient.CreateSettingClient(cs, config),
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

	CRDobj := ntype.Crdnode{}
	ntype.LhNode2CRDNode(node, &CRDobj, node.ID)
	result, err := s.NodeClient.Create(&CRDobj)
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

	CRDobj := ntype.Crdnode{}
	CRDobj.ResourceVersion = strconv.FormatUint(node.KVIndex, 10)
	ntype.LhNode2CRDNode(node, &CRDobj, node.ID)
	result, err := s.NodeClient.Update(&CRDobj, node.ID)
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

		err := s.NodeClient.Delete(nodeID, &meta_v1.DeleteOptions{})
		if err != nil {
			return errors.Wrapf(err, "unable to delete node %v", nodeID)
		}
		return nil
}

func (s *CRDStore) GetNode(id string) (*types.NodeInfo, error) {
	n, err := s.getNodeByKey(id)
	if err != nil {
		return nil, errors.Wrap(err, "unable to get node")
	}
	return n, nil
}

func (s *CRDStore) getNodeByKey(key string) (*types.NodeInfo, error) {
	n := types.NodeInfo{}

	r, err := s.NodeClient.Get(key)

	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	ntype.CRDNode2LhNode(r, &n)
	index, err := strconv.ParseUint(r.ResourceVersion, 10, 64)
	if err != nil {
		return nil, err
	}
	n.KVIndex = index
	return &n, nil
}

func (s *CRDStore) ListNodes() (map[string]*types.NodeInfo, error) {
	nodes := make(map[string]*types.NodeInfo)

	r, err := s.NodeClient.List(meta_v1.ListOptions{})
	if err != nil {
		return nil, err
	}

	if len(r.Items) <= 0 {
		return nil, nil
	}

	var n = make([]types.NodeInfo, len(r.Items))

	for i, item := range r.Items {
		ntype.CRDNode2LhNode(&item, &n[i])
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

	CRDobj := stype.Crdsetting{}
	stype.LhSetting2CRDSetting(settings, &CRDobj, keySettings)
	r, err := s.SettingClient.Create(&CRDobj)
	if err != nil {
		if apierrors.IsAlreadyExists(err) {
			fmt.Printf("ALREADY EXISTS: %#v\n", r)
		}
		return  err
	}

	index, err := strconv.ParseUint(r.ResourceVersion, 10, 64)
	if err != nil {
		return err
	}

	settings.KVIndex = index
	return nil
}


func (s *CRDStore) UpdateSettings(settings *types.SettingsInfo) error {

	CRDobj := stype.Crdsetting{}
	CRDobj.ResourceVersion = strconv.FormatUint(settings.KVIndex, 10)
	stype.LhSetting2CRDSetting(settings, &CRDobj, keySettings)
	result, err := s.SettingClient.Update(&CRDobj, keySettings)
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

	r, err := s.SettingClient.Get(keySettings)

	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}

	settings := &types.SettingsInfo{}
	stype.CRDSetting2LhSetting(r, settings)

	fmt.Printf("GET setting string %#v \n\n", settings)
	index, err := strconv.ParseUint(r.ResourceVersion, 10, 64)
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