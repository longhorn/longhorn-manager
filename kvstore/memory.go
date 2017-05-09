package kvstore

import (
	"encoding/json"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"

	"github.com/patrickmn/go-cache"
)

var (
	MemoryKeyNotFoundError = errors.Errorf("key not found")

	Separator = "/"
)

type MemoryBackend struct {
	c *cache.Cache
}

func NewMemoryBackend() (*MemoryBackend, error) {
	c := cache.New(cache.NoExpiration, cache.NoExpiration)
	return &MemoryBackend{
		c: c,
	}, nil
}

func (m *MemoryBackend) Create(key string, obj interface{}) error {
	value, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	return m.c.Add(key, string(value), cache.DefaultExpiration)
}

func (m *MemoryBackend) Set(key string, obj interface{}) error {
	value, err := json.Marshal(obj)
	if err != nil {
		return err
	}
	m.c.SetDefault(key, string(value))
	return nil
}

func (m *MemoryBackend) Get(key string, obj interface{}) error {
	value, exists := m.c.Get(key)
	if !exists {
		return MemoryKeyNotFoundError
	}
	if err := json.Unmarshal([]byte(value.(string)), obj); err != nil {
		return errors.Wrap(err, "fail to unmarshal json")
	}
	return nil
}

func (m *MemoryBackend) Delete(key string) error {
	keys, err := m.Keys(key)
	if err != nil {
		return err
	}
	if keys == nil {
		return nil
	}

	for _, key := range keys {
		m.c.Delete(key)
	}
	return nil
}

func (m *MemoryBackend) Keys(prefix string) ([]string, error) {
	keys := []string{}

	items := m.c.Items()
	for key := range items {
		exists := false
		for _, k := range keys {
			if strings.HasPrefix(key, k) {
				exists = true
				break

			}
		}
		if exists {
			continue
		}

		if !strings.HasPrefix(key, prefix) {
			continue
		}

		if key == prefix {
			keys = append(keys, key)
			continue
		}

		k := ""
		key = strings.TrimLeft(key, "/")
		prefixLevel := strings.Count(prefix, Separator)
		entries := strings.Split(key, Separator)
		for i := 0; i < prefixLevel+1; i++ {
			k = filepath.Join(k, entries[i])
		}
		k = "/" + k
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil, nil
	}
	return keys, nil
}

func (m *MemoryBackend) IsNotFoundError(err error) bool {
	return err == MemoryKeyNotFoundError
}
