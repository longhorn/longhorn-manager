package api

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidateManagerURL(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantError bool
	}{
		{
			name:      "empty URL is valid (disabled)",
			input:     "",
			wantError: false,
		},
		{
			name:      "valid https URL",
			input:     "https://longhorn.example.com",
			wantError: false,
		},
		{
			name:      "valid http URL",
			input:     "http://longhorn.example.com",
			wantError: false,
		},
		{
			name:      "valid URL with custom port",
			input:     "https://longhorn.example.com:8443",
			wantError: false,
		},
		{
			name:      "valid URL with default https port",
			input:     "https://longhorn.example.com:443",
			wantError: false,
		},
		{
			name:      "valid URL with default http port",
			input:     "http://longhorn.example.com:80",
			wantError: false,
		},
		{
			name:      "valid IPv6 URL",
			input:     "http://[2001:db8::1]:9500",
			wantError: false,
		},
		{
			name:      "valid IPv6 URL without port",
			input:     "http://[::1]",
			wantError: false,
		},
		{
			name:      "invalid scheme",
			input:     "ftp://longhorn.example.com",
			wantError: true,
		},
		{
			name:      "missing scheme",
			input:     "longhorn.example.com",
			wantError: true,
		},
		{
			name:      "URL with path",
			input:     "https://longhorn.example.com/api",
			wantError: true,
		},
		{
			name:      "URL with query",
			input:     "https://longhorn.example.com?foo=bar",
			wantError: true,
		},
		{
			name:      "URL with fragment",
			input:     "https://longhorn.example.com#section",
			wantError: true,
		},
		{
			name:      "URL with trailing slash",
			input:     "https://longhorn.example.com/",
			wantError: false, // Trailing slash is acceptable
		},
		{
			name:      "malformed URL",
			input:     "://invalid",
			wantError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateManagerURL(tt.input)
			if tt.wantError {
				assert.Error(t, err, "expected error for input: %s", tt.input)
			} else {
				assert.NoError(t, err, "expected no error for input: %s", tt.input)
			}
		})
	}
}

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
