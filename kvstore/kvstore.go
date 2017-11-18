package kvstore

import (
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/rancher/longhorn-manager/types"
)

type Backend interface {
	Create(key string, obj interface{}) (uint64, error)
	Update(key string, obj interface{}, index uint64) (uint64, error)
	Get(key string, obj interface{}) (uint64, error)
	Delete(key string) error
	Keys(prefix string) ([]string, error)
	IsNotFoundError(err error) bool
}

type KVStore struct {
	Prefix string

	b Backend
}

const (
	keyNodes    = "nodes"
	keySettings = "settings"
)

func NewKVStore(prefix string, backend Backend) (*KVStore, error) {
	if backend == nil {
		return nil, errors.Errorf("invalid empty backend")
	}
	return &KVStore{
		Prefix: prefix,
		b:      backend,
	}, nil
}

func (s *KVStore) key(key string) string {
	// It's not file path, but we use it to deal with '/'
	return filepath.Join(s.Prefix, key)
}

func (s *KVStore) nodeKey(id string) string {
	return filepath.Join(s.key(keyNodes), id)
}

func (s *KVStore) checkNode(node *types.NodeInfo) error {
	if node.ID == "" || node.Name == "" || node.IP == "" {
		return fmt.Errorf("BUG: missing required field %+v", node)
	}
	return nil
}

func (s *KVStore) CreateNode(node *types.NodeInfo) error {
	if err := s.checkNode(node); err != nil {
		return err
	}
	index, err := s.b.Create(s.nodeKey(node.ID), node)
	if err != nil {
		return err
	}
	node.KVIndex = index
	logrus.Infof("Add node %v name %v longhorn-manager IP %v", node.ID, node.Name, node.IP)
	return nil
}

func (s *KVStore) UpdateNode(node *types.NodeInfo) error {
	if err := s.checkNode(node); err != nil {
		return err
	}
	index, err := s.b.Update(s.nodeKey(node.ID), node, node.KVIndex)
	if err != nil {
		return err
	}
	node.KVIndex = index
	return nil
}

func (s *KVStore) DeleteNode(nodeID string) error {
	if err := s.b.Delete(s.nodeKey(nodeID)); err != nil {
		return errors.Wrapf(err, "unable to delete node %v", nodeID)
	}
	return nil
}

func (s *KVStore) GetNode(id string) (*types.NodeInfo, error) {
	node, err := s.getNodeByKey(s.nodeKey(id))
	if err != nil {
		return nil, errors.Wrap(err, "unable to get node")
	}
	return node, nil
}

func (s *KVStore) getNodeByKey(key string) (*types.NodeInfo, error) {
	node := types.NodeInfo{}
	index, err := s.b.Get(key, &node)
	if err != nil {
		if s.b.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	node.KVIndex = index
	return &node, nil
}

func (s *KVStore) ListNodes() (map[string]*types.NodeInfo, error) {
	nodeKeys, err := s.b.Keys(s.key(keyNodes))
	if err != nil {
		return nil, err
	}

	nodes := make(map[string]*types.NodeInfo)
	for _, key := range nodeKeys {
		node, err := s.getNodeByKey(key)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid key %v", key)
		}
		if node != nil {
			nodes[node.ID] = node
		}
	}
	return nodes, nil
}

func (s *KVStore) settingsKey() string {
	return s.key(keySettings)
}

func (s *KVStore) CreateSettings(settings *types.SettingsInfo) error {
	index, err := s.b.Create(s.settingsKey(), settings)
	if err != nil {
		return err
	}
	settings.KVIndex = index
	return nil
}

func (s *KVStore) UpdateSettings(settings *types.SettingsInfo) error {
	index, err := s.b.Update(s.settingsKey(), settings, settings.KVIndex)
	if err != nil {
		return err
	}
	settings.KVIndex = index
	return nil
}

func (s *KVStore) GetSettings() (*types.SettingsInfo, error) {
	settings := &types.SettingsInfo{}
	index, err := s.b.Get(s.settingsKey(), settings)
	if err != nil {
		if s.b.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "unable to get settings")
	}
	settings.KVIndex = index

	return settings, nil
}

// Nuclear is test only function, which will wipe all longhorn entries
func (s *KVStore) Nuclear(nuclearCode string) error {
	if nuclearCode != "nuke key value store" {
		return errors.Errorf("invalid nuclear code!")
	}
	if err := s.b.Delete(s.key("")); err != nil {
		return err
	}
	return nil
}

func getFieldUint64(obj interface{}, field string) (uint64, error) {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		return 0, fmt.Errorf("BUG: Non-pointer was passed in")
	}
	t := reflect.TypeOf(obj).Elem()
	if _, found := t.FieldByName(field); !found {
		return 0, fmt.Errorf("BUG: %v doesn't have required field %v", t, field)
	}
	return reflect.ValueOf(obj).Elem().FieldByName(field).Uint(), nil
}

func setFieldUint64(obj interface{}, field string, value uint64) error {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		return fmt.Errorf("BUG: Non-pointer was passed in")
	}
	t := reflect.TypeOf(obj).Elem()
	if _, found := t.FieldByName(field); !found {
		return fmt.Errorf("BUG: %v doesn't have required field %v", t, field)
	}
	v := reflect.ValueOf(obj).Elem().FieldByName(field)
	if !v.CanSet() {
		return fmt.Errorf("BUG: %v doesn't have setable field %v", t, field)
	}
	v.SetUint(value)
	return nil
}

func UpdateKVIndex(dst, src interface{}) error {
	srcIndex, err := getFieldUint64(src, "KVIndex")
	if err != nil {
		return err
	}
	return setFieldUint64(dst, "KVIndex", srcIndex)
}
