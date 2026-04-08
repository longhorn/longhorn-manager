package util

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildTargetAddress(t *testing.T) {
	testCases := []struct {
		name     string
		host     string
		port     int
		expected string
	}{
		{
			name:     "ipv4",
			host:     "10.0.0.1",
			port:     9502,
			expected: "10.0.0.1:9502",
		},
		{
			name:     "ipv6",
			host:     "2001:db8::1",
			port:     9502,
			expected: "[2001:db8::1]:9502",
		},
		{
			name:     "empty host",
			host:     "",
			port:     9502,
			expected: "",
		},
		{
			name:     "zero port",
			host:     "10.0.0.1",
			port:     0,
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, BuildTargetAddress(tc.host, tc.port))
		})
	}
}
