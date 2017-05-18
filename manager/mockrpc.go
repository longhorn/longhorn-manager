package manager

import (
	"fmt"
)

type MockRPCManager struct {
	callbackChan chan Event
	address      string
}

func NewMockRPCManager() RPCManager {
	return &MockRPCManager{}
}

func (r *MockRPCManager) StartServer(address string) error {
	r.address = address
	return nil
}

func (r *MockRPCManager) SetCallbackChan(ch chan Event) {
	r.callbackChan = ch
}

func (r *MockRPCManager) NodeNotify(address string, event *Event) error {
	if address != r.address {
		return fmt.Errorf("cannot reach %v", address)
	}
	r.callbackChan <- *event
	return nil
}
