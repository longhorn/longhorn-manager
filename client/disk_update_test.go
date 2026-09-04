package client

import (
	"encoding/json"
	"testing"
)

func TestDiskUpdateBlockSizeJSONPresence(t *testing.T) {
	zero := int64(0)
	fiveTwelve := int64(512)
	fourK := int64(4096)
	testCases := map[string]struct {
		blockSize *int64
		present   bool
		value     int64
	}{
		"omitted": {
			blockSize: nil,
		},
		"explicit zero": {
			blockSize: &zero,
			present:   true,
		},
		"512": {
			blockSize: &fiveTwelve,
			present:   true,
			value:     512,
		},
		"4096": {
			blockSize: &fourK,
			present:   true,
			value:     4096,
		},
	}

	for name, testCase := range testCases {
		t.Run(name, func(t *testing.T) {
			data, err := json.Marshal(DiskUpdate{BlockSize: testCase.blockSize})
			if err != nil {
				t.Fatalf("failed to marshal disk update: %v", err)
			}

			var fields map[string]json.RawMessage
			if err := json.Unmarshal(data, &fields); err != nil {
				t.Fatalf("failed to inspect disk update JSON: %v", err)
			}
			rawBlockSize, present := fields["blockSize"]
			if present != testCase.present {
				t.Fatalf("expected blockSize presence %v, got %v in %s", testCase.present, present, data)
			}
			if !present {
				return
			}

			var value int64
			if err := json.Unmarshal(rawBlockSize, &value); err != nil {
				t.Fatalf("failed to decode blockSize: %v", err)
			}
			if value != testCase.value {
				t.Fatalf("expected blockSize %d, got %d", testCase.value, value)
			}

			var roundTrip DiskUpdate
			if err := json.Unmarshal(data, &roundTrip); err != nil {
				t.Fatalf("failed to round-trip disk update: %v", err)
			}
			if roundTrip.BlockSize == nil || *roundTrip.BlockSize != testCase.value {
				t.Fatalf("expected round-trip blockSize %d, got %v", testCase.value, roundTrip.BlockSize)
			}
		})
	}
}
