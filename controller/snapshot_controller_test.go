package controller

import (
	"fmt"
	"testing"
)

func TestShouldUpdateObject(t *testing.T) {
	var reconcileErr1, reconcileErr2 error
	reconcileErr1 = reconcileError{
		error:              fmt.Errorf("test error message"),
		shouldUpdateObject: true,
	}
	reconcileErr2 = reconcileError{
		error:              fmt.Errorf("test error message"),
		shouldUpdateObject: false,
	}

	if !shouldUpdateObject(reconcileErr1) {
		t.Fatal("reconcileErr1 must be updatable error")
	}
	if shouldUpdateObject(reconcileErr2) {
		t.Fatal("reconcileErr1 must be non-updatable error")
	}

	err := fmt.Errorf("test error message")
	if shouldUpdateObject(err) {
		t.Fatal("reconcileErr1 must be non-updatable error")
	}
}
