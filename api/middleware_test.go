package api

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestInjectForwardedHeaders(t *testing.T) {
	tests := []struct {
		name               string
		managerURL         string
		expectedProto      string
		expectedHost       string
		expectedPort       string
		expectedPortExists bool
	}{
		{
			name:               "https with default port",
			managerURL:         "https://longhorn.example.com",
			expectedProto:      "https",
			expectedHost:       "longhorn.example.com",
			expectedPort:       "",
			expectedPortExists: false,
		},
		{
			name:               "https with explicit default port 443",
			managerURL:         "https://longhorn.example.com:443",
			expectedProto:      "https",
			expectedHost:       "longhorn.example.com",
			expectedPort:       "",
			expectedPortExists: false,
		},
		{
			name:               "http with explicit default port 80",
			managerURL:         "http://longhorn.example.com:80",
			expectedProto:      "http",
			expectedHost:       "longhorn.example.com",
			expectedPort:       "",
			expectedPortExists: false,
		},
		{
			name:               "https with custom port",
			managerURL:         "https://longhorn.example.com:8443",
			expectedProto:      "https",
			expectedHost:       "longhorn.example.com",
			expectedPort:       "8443",
			expectedPortExists: true,
		},
		{
			name:               "http with custom port",
			managerURL:         "http://longhorn.example.com:9500",
			expectedProto:      "http",
			expectedHost:       "longhorn.example.com",
			expectedPort:       "9500",
			expectedPortExists: true,
		},
		{
			name:               "IPv6 address with port",
			managerURL:         "http://[2001:db8::1]:9500",
			expectedProto:      "http",
			expectedHost:       "2001:db8::1",
			expectedPort:       "9500",
			expectedPortExists: true,
		},
		{
			name:               "IPv6 localhost with default port",
			managerURL:         "http://[::1]",
			expectedProto:      "http",
			expectedHost:       "::1",
			expectedPort:       "",
			expectedPortExists: false,
		},
		{
			name:               "URL with trailing slash",
			managerURL:         "https://longhorn.example.com/",
			expectedProto:      "https",
			expectedHost:       "longhorn.example.com",
			expectedPort:       "",
			expectedPortExists: false,
		},
		{
			name:               "http with non-default port 443",
			managerURL:         "http://longhorn.example.com:443",
			expectedProto:      "http",
			expectedHost:       "longhorn.example.com",
			expectedPort:       "443",
			expectedPortExists: true,
		},
		{
			name:               "https with non-default port 80",
			managerURL:         "https://longhorn.example.com:80",
			expectedProto:      "https",
			expectedHost:       "longhorn.example.com",
			expectedPort:       "80",
			expectedPortExists: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "http://example.com/v1/volumes", nil)

			err := injectForwardedHeaders(req, tt.managerURL)
			assert.NoError(t, err)

			assert.Equal(t, tt.expectedProto, req.Header.Get("X-Forwarded-Proto"))
			assert.Equal(t, tt.expectedHost, req.Header.Get("X-Forwarded-Host"))

			if tt.expectedPortExists {
				assert.Equal(t, tt.expectedPort, req.Header.Get("X-Forwarded-Port"))
			} else {
				assert.Empty(t, req.Header.Get("X-Forwarded-Port"), "X-Forwarded-Port should not be set")
			}
		})
	}
}

func TestInjectForwardedHeadersOverride(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/v1/volumes", nil)

	// Set existing headers
	req.Header.Set("X-Forwarded-Proto", "http")
	req.Header.Set("X-Forwarded-Host", "old.example.com")
	req.Header.Set("X-Forwarded-Port", "8080")

	// Inject new headers
	err := injectForwardedHeaders(req, "https://new.example.com:9443")
	assert.NoError(t, err)

	// Verify headers were overridden
	assert.Equal(t, "https", req.Header.Get("X-Forwarded-Proto"))
	assert.Equal(t, "new.example.com", req.Header.Get("X-Forwarded-Host"))
	assert.Equal(t, "9443", req.Header.Get("X-Forwarded-Port"))
}

func TestInjectForwardedHeadersInvalidURL(t *testing.T) {
	req := httptest.NewRequest("GET", "http://example.com/v1/volumes", nil)

	err := injectForwardedHeaders(req, "://invalid")
	assert.Error(t, err)
}
