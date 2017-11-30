package manager

import (
	"fmt"
	"strconv"
	"sync"
)

type MockRPCNotifier struct {
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

func NewMockRPCNotifier(db *MockRPCDB, ip string, port int) *MockRPCNotifier {
	return &MockRPCNotifier{
		db:   db,
		ip:   ip,
		port: port,
	}
}

func (r *MockRPCNotifier) GetAddress() string {
	return r.ip + ":" + strconv.Itoa(r.port)
}

func (r *MockRPCNotifier) Start(ch chan Event) error {
	r.callbackChan = ch
	r.db.SetAddress2Channel(r.GetAddress(), ch)
	return nil
}

func (r *MockRPCNotifier) Stop() {
}

func (r *MockRPCNotifier) GetPort() int {
	return r.port
}

func (r *MockRPCNotifier) NodeNotify(address string, event *Event) error {
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
