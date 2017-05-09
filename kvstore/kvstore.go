package kvstore

import (
	"fmt"
	"path/filepath"
	"reflect"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"

	"github.com/yasker/lm-rewrite/types"
)

type Backend interface {
	Create(key string, obj interface{}) error
	Update(key string, obj interface{}, index uint64) error
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
	keyHosts    = "hosts"
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

func (s *KVStore) hostKey(id string) string {
	return filepath.Join(s.key(keyHosts), id)
}

func (s *KVStore) CreateHost(host *types.HostInfo) error {
	if err := s.b.Create(s.hostKey(host.UUID), host); err != nil {
		return err
	}
	logrus.Infof("Add host %v name %v longhorn-manager address %v", host.UUID, host.Name, host.Address)
	return nil
}

func (s *KVStore) UpdateHost(host *types.HostInfo) error {
	if err := s.b.Update(s.hostKey(host.UUID), host, host.KVIndex); err != nil {
		return err
	}
	logrus.Infof("Add host %v name %v longhorn-manager address %v", host.UUID, host.Name, host.Address)
	return nil
}

func (s *KVStore) GetHost(id string) (*types.HostInfo, error) {
	host, err := s.getHostByKey(s.hostKey(id))
	if err != nil {
		return nil, errors.Wrap(err, "unable to get host")
	}
	return host, nil
}

func (s *KVStore) getHostByKey(key string) (*types.HostInfo, error) {
	host := types.HostInfo{}
	index, err := s.b.Get(key, &host)
	if err != nil {
		if s.b.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	host.KVIndex = index
	return &host, nil
}

func (s *KVStore) ListHosts() (map[string]*types.HostInfo, error) {
	hostKeys, err := s.b.Keys(s.key(keyHosts))
	if err != nil {
		return nil, err
	}

	hosts := make(map[string]*types.HostInfo)
	for _, key := range hostKeys {
		host, err := s.getHostByKey(key)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid key %v", key)
		}
		if host != nil {
			hosts[host.UUID] = host
		}
	}
	return hosts, nil
}

func (s *KVStore) settingsKey() string {
	return s.key(keySettings)
}

func (s *KVStore) CreateSettings(settings *types.SettingsInfo) error {
	if err := s.b.Create(s.settingsKey(), settings); err != nil {
		return err
	}
	return nil
}

func (s *KVStore) UpdateSettings(settings *types.SettingsInfo) error {
	if err := s.b.Update(s.settingsKey(), settings, settings.KVIndex); err != nil {
		return err
	}
	return nil
}

func (s *KVStore) GetSettings() (*types.SettingsInfo, error) {
	settings := &types.SettingsInfo{}
	index, err := s.b.Get(s.settingsKey(), &settings)
	if err != nil {
		if s.b.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, errors.Wrap(err, "unable to get settings")
	}
	settings.KVIndex = index

	return settings, nil
}

// kuNuclear is test only function, which will wipe all longhorn entries
func (s *KVStore) kvNuclear(nuclearCode string) error {
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
