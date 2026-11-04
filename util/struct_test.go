package util

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

type testField struct {
	Name  StructName
	Value interface{}
}

// buildShuffled builds a StructFields slice of n synthetic fields in
// randomized order, mimicking non-deterministic map iteration order
// (like collectSettings()/collectVolumesInfo()).
// Using n fields gives n! possible distinct orderings.
func buildShuffled(seed int64, n int) StructFields {
	base := make([]testField, n)
	for i := 0; i < n; i++ {
		name := StructName(fmt.Sprintf("Field%d", i))
		// Vary value types to better mimic real heterogeneous data
		// (ints, strings, bools) rather than uniform ints.
		switch i % 3 {
		case 0:
			base[i] = testField{name, i}
		case 1:
			base[i] = testField{name, fmt.Sprintf("value-%d", i)}
		case 2:
			base[i] = testField{name, i%2 == 0}
		}
	}

	r := rand.New(rand.NewSource(seed))
	r.Shuffle(len(base), func(i, j int) {
		base[i], base[j] = base[j], base[i]
	})

	sf := StructFields{}
	for _, f := range base {
		sf.Append(f.Name, f.Value)
	}
	return sf
}

// TestToMap_ContentIsOrderIndependent confirms whether the content
// of resulting map is identical regardless of the order fields
// appended in or not since that order is non-deterministic in production
// (driven by Go map iteration in collectSettings()/collectVolumesInfo()).
//
// This asserts that across many differently-ordered builds of the
// same logical field set, every resulting map is deeply equal.
func TestToMap_ContentIsOrderIndependent(t *testing.T) {
	const trials = 500
	var reference any

	for i := 0; i < trials; i++ {
		sf := buildShuffled(int64(i), 10)
		result := sf.ToMap()

		if reference == nil {
			reference = result
			continue
		}
		if !reflect.DeepEqual(result, reference) {
			t.Fatalf("trial %d: map contents differ based on append order\n got:  %#v\nwant: %#v", i, result, reference)
		}
	}
}

func TestToMap(t *testing.T) {
	testCases := []struct {
		name   string
		fields []testField
		want   map[string]any
	}{
		{
			name: "single field",
			fields: []testField{
				{"NodeCount", 4},
			},
			want: map[string]any{
				"nodeCount": 4,
			},
		},
		{
			name: "mixed type fields",
			fields: []testField{
				{"NodeCount", 4},
				{"DataEngineV2Enabled", true},
				{"BackupTargetType", "s3"},
			},
			want: map[string]any{
				"nodeCount":           4,
				"dataEngineV2Enabled": true,
				"backupTargetType":    "s3",
			},
		},
		{
			name:   "empty fields",
			fields: nil,
			want:   map[string]interface{}{},
		},
		{
			name: "nil and zero values",
			fields: []testField{
				{"NodeCount", 0},
				{"BackupTargetType", ""},
				{"Value", nil},
			},
			want: map[string]any{
				"nodeCount":        0,
				"backupTargetType": "",
				"value":            nil,
			},
		},
		{
			name: "nested map or struct value",
			fields: []testField{
				{"Metadata", map[string]string{"env": "prod"}},
			},
			want: map[string]any{
				"metadata": map[string]string{"env": "prod"},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			sf := StructFields{}
			for _, f := range tc.fields {
				sf.Append(f.Name, f.Value)
			}
			got := sf.ToMap()
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}
