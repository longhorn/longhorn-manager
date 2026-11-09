package engineapi

import (
	"math"
	"reflect"
	"testing"

	imapi "github.com/longhorn/longhorn-instance-manager/pkg/api"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestReservationConversionRoundTrip(t *testing.T) {
	state := &imapi.ReservationState{
		Epoch:        3,
		Type:         3,
		HolderKey:    0xAAAA,
		HolderHostID: "0f2a8bc4-1d9e-4b5c-8a7f-3e6d2c1b0a99",
		Registrants: []imapi.ReservationRegistrant{
			{Key: 0xAAAA, HostID: "0f2a8bc4-1d9e-4b5c-8a7f-3e6d2c1b0a99"},
			{Key: math.MaxUint64, HostID: "11111111-2222-3333-4444-555555555555"},
		},
	}

	r, err := instanceReservationToVolumeReservation(state, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	want := &longhorn.VolumeReservation{
		Epoch:        3,
		NodeID:       "node-1",
		Type:         3,
		HolderKey:    "0xaaaa",
		HolderHostID: "0f2a8bc4-1d9e-4b5c-8a7f-3e6d2c1b0a99",
		Registrants: []longhorn.VolumeReservationRegistrant{
			{Key: "0xaaaa", HostID: "0f2a8bc4-1d9e-4b5c-8a7f-3e6d2c1b0a99"},
			{Key: "0xffffffffffffffff", HostID: "11111111-2222-3333-4444-555555555555"},
		},
	}
	if !reflect.DeepEqual(r, want) {
		t.Fatalf("got %+v, want %+v", r, want)
	}

	if got := volumeReservationToInstanceReservation(r, ""); !reflect.DeepEqual(got, state) {
		t.Fatalf("got %+v, want %+v", got, state)
	}
}

func TestReservationConversionEdgeCases(t *testing.T) {
	if r, err := instanceReservationToVolumeReservation(nil, "node-1"); r != nil || err != nil {
		t.Fatalf("got %+v, %v for an unknown state, want nil", r, err)
	}
	if got := volumeReservationToInstanceReservation(nil, "uid-a"); got != nil {
		t.Fatalf("got %+v for a nil state, want nil", got)
	}

	// No holder is an empty key rather than 0x0.
	r, err := instanceReservationToVolumeReservation(&imapi.ReservationState{Epoch: 1}, "node-1")
	if err != nil {
		t.Fatal(err)
	}
	if r.HolderKey != "" || len(r.Registrants) != 0 {
		t.Fatalf("got %+v, want no holder and no registrants", r)
	}

	if _, err := instanceReservationToVolumeReservation(&imapi.ReservationState{Epoch: math.MaxUint64}, "node-1"); err == nil {
		t.Fatal("expected an epoch overflowing int64 to fail")
	}

	// A registrant with a malformed key is skipped instead of failing the engine
	// creation.
	got := volumeReservationToInstanceReservation(&longhorn.VolumeReservation{
		Epoch:     2,
		HolderKey: "0xa",
		Registrants: []longhorn.VolumeReservationRegistrant{
			{Key: "0xa", HostID: "host-a"},
			{Key: "not-a-key", HostID: "host-b"},
		},
	}, "uid-a")
	want := &imapi.ReservationState{
		Epoch:       2,
		HolderKey:   0xa,
		Registrants: []imapi.ReservationRegistrant{{Key: 0xa, HostID: "host-a"}},
		VolumeUID:   "uid-a",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
