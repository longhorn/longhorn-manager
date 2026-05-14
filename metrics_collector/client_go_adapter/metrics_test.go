package client_go_adapter

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNormalizeURLLabel(t *testing.T) {
	t.Parallel()

	testCases := map[string]struct {
		input    string
		expected string
	}{
		"removes query and fragment": {
			input:    "https://example.com/apis/longhorn.io/v1beta2/volumes?timeout=9.2&watch=true#ignored",
			expected: "https://example.com/apis/longhorn.io/v1beta2/volumes",
		},
		"removes query from path-only url": {
			input:    "/v1/volumes/volume-1?action=snapshotCreate",
			expected: "/v1/volumes/volume-1",
		},
		"preserves already-normalized kubernetes url": {
			input:    "https://10.0.0.1:6443/api/v1/namespaces/%7Bnamespace%7D/pods/%7Bname%7D?fieldSelector=%7Bvalue%7D",
			expected: "https://10.0.0.1:6443/api/v1/namespaces/%7Bnamespace%7D/pods/%7Bname%7D",
		},
		"no-op when no query or fragment present": {
			input:    "https://10.0.0.1:6443/api/v1/nodes/%7Bname%7D",
			expected: "https://10.0.0.1:6443/api/v1/nodes/%7Bname%7D",
		},
		"removes fragment only": {
			input:    "https://example.com/path#section",
			expected: "https://example.com/path",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			parsedURL, err := url.Parse(tc.input)
			require.NoError(t, err)

			require.Equal(t, tc.expected, normalizeURLLabel(*parsedURL))
		})
	}
}
