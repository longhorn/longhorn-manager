package manager

import (
	"fmt"
	"sync"
)

type MockRPCManager struct {
	callbackChan chan Event
	address      string
	db           *MockRPCDB
}

type MockRPCDB struct {
	mutex          *sync.Mutex
	addressChanMap map[string]chan Event
}

func NewMockRPCDB() *MockRPCDB {
	return &MockRPCDB{
		mutex:          &sync.Mutex{},
		addressChanMap: map[string]chan Event{},
	}
}

func (db *MockRPCDB) SetAddress2Channel(address string, ch chan Event) {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	db.addressChanMap[address] = ch
}

func (db *MockRPCDB) GetAddress2Channel(address string) chan Event {
	db.mutex.Lock()
	defer db.mutex.Unlock()

	return db.addressChanMap[address]
}

func NewMockRPCManager(db *MockRPCDB) *MockRPCManager {
	return &MockRPCManager{
		db: db,
	}
}

func (r *MockRPCManager) StartServer(address string, ch chan Event) error {
	r.address = address
	r.callbackChan = ch
	r.db.SetAddress2Channel(r.address, ch)
	return nil
}

func (r *MockRPCManager) NodeNotify(address string, event *Event) error {
	var ch chan Event
	if address == r.address {
		ch = r.callbackChan
	} else {
		ch = r.db.GetAddress2Channel(address)
		if ch == nil {
			return fmt.Errorf("cannot reach %v", address)
		}
	}
	ch <- *event
	return nil
}
