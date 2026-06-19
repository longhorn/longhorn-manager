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

func TestBuildHTTPURL(t *testing.T) {
	testCases := []struct {
		name     string
		host     string
		port     int
		path     string
		expected string
	}{
		{
			name:     "ipv4",
			host:     "10.0.0.1",
			port:     8080,
			path:     "status",
			expected: "http://10.0.0.1:8080/status",
		},
		{
			name:     "ipv6",
			host:     "2001:db8::1",
			port:     8080,
			path:     "bundle",
			expected: "http://[2001:db8::1]:8080/bundle",
		},
		{
			name:     "leading slash path",
			host:     "2001:db8::1",
			port:     8080,
			path:     "/status",
			expected: "http://[2001:db8::1]:8080/status",
		},
		{
			name:     "empty host",
			host:     "",
			port:     8080,
			path:     "status",
			expected: "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, BuildHTTPURL(tc.host, tc.port, tc.path))
		})
	}
}
