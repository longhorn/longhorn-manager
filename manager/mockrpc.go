package manager

import (
	"fmt"
	"strconv"
	"sync"
)

type MockRPCManager struct {
	callbackChan chan Event
	ip           string
	port         int
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

func NewMockRPCManager(db *MockRPCDB, ip string, port int) *MockRPCManager {
	return &MockRPCManager{
		db:   db,
		ip:   ip,
		port: port,
	}
}

func (r *MockRPCManager) GetAddress() string {
	return r.ip + ":" + strconv.Itoa(r.port)
}

func (r *MockRPCManager) Start(ch chan Event) error {
	r.callbackChan = ch
	r.db.SetAddress2Channel(r.GetAddress(), ch)
	return nil
}

func (r *MockRPCManager) Stop() {
}

func (r *MockRPCManager) GetPort() int {
	return r.port
}

func (r *MockRPCManager) NodeNotify(address string, event *Event) error {
	var ch chan Event
	if address == r.GetAddress() {
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
