package backingimagemanager

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	jsonpatch "gopkg.in/evanphx/json-patch.v4"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestMutatorStartingAt(t *testing.T) {
	for _, operation := range []string{"create", "update"} {
		for _, startingAt := range []string{"", "2026-10-15T08:00:00Z"} {
			t.Run(operation+"/"+startingAt, func(t *testing.T) {
				bim := &longhorn.BackingImageManager{
					Spec: longhorn.BackingImageManagerSpec{StartingAt: startingAt},
				}
				mutator := NewMutator(nil)
				before := time.Now().UTC().Truncate(time.Second)
				var patches []string
				var err error
				if operation == "create" {
					patches, err = mutator.Create(nil, bim)
				} else {
					patches, err = mutator.Update(nil, bim.DeepCopy(), bim)
				}
				require.NoError(t, err)

				startingAtPatches := 0
				for _, patch := range patches {
					if !strings.Contains(patch, `"/spec/startingAt"`) {
						continue
					}
					startingAtPatches++
					// The generated value is an RFC3339 timestamp, quoted in the patch.
					prefix := `{"op": "add", "path": "/spec/startingAt", "value": "`
					require.True(t, strings.HasPrefix(patch, prefix))
					value := strings.TrimSuffix(strings.TrimPrefix(patch, prefix), `"}`)
					timestamp, err := time.Parse(time.RFC3339, value)
					require.NoError(t, err)
					require.False(t, timestamp.Before(before))
					require.False(t, timestamp.After(time.Now().UTC()))

					decodedPatch, err := jsonpatch.DecodePatch([]byte("[" + patch + "]"))
					require.NoError(t, err)
					for _, document := range []string{`{"spec":{}}`, `{"spec":{"startingAt":""}}`} {
						mutated, err := decodedPatch.Apply([]byte(document))
						require.NoError(t, err, "default both omitted and empty startingAt")
						require.Contains(t, string(mutated), value)
					}
				}
				if startingAt == "" {
					require.Equal(t, 1, startingAtPatches)
				} else {
					require.Zero(t, startingAtPatches, "preserve the requested pod generation")
				}
			})
		}
	}
}
