package api

import (
	"net/http"
	"net/url"

	"github.com/sirupsen/logrus"

	"github.com/longhorn/longhorn-manager/types"
)

// ManagerURLMiddleware injects X-Forwarded-* headers based on manager-url setting.
// This middleware reads the manager-url setting and, when configured, overrides
// the X-Forwarded-Proto, X-Forwarded-Host, and X-Forwarded-Port headers to ensure
// API responses (actions and links fields) contain the correct external URL instead
// of internal pod IPs.
// Note: The manager-url value is validated when the setting is updated, so no
// validation is performed here.
func ManagerURLMiddleware(s *Server) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			managerURL, err := s.m.GetSettingValueExisted(types.SettingNameManagerURL)
			if err != nil {
				logrus.WithError(err).Warn("Failed to get manager-url setting, using default behavior")
				next.ServeHTTP(w, r)
				return
			}

			if managerURL == "" {
				// Setting not configured, use default behavior
				next.ServeHTTP(w, r)
				return
			}

			if err := injectForwardedHeaders(r, managerURL); err != nil {
				logrus.WithError(err).Warn("Failed to inject forwarded headers")
				next.ServeHTTP(w, r)
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// injectForwardedHeaders extracts proto, host, and port from the managerURL
// and injects them as X-Forwarded-* headers in the request.
// This overrides any existing X-Forwarded-* headers to ensure consistent behavior.
func injectForwardedHeaders(r *http.Request, managerURL string) error {
	u, err := url.Parse(managerURL)
	if err != nil {
		return err
	}

	// Extract proto
	r.Header.Set("X-Forwarded-Proto", u.Scheme)

	// Extract host and port using url.Hostname() and url.Port()
	// url.Hostname() automatically handles IPv6 addresses in brackets
	host := u.Hostname()
	port := u.Port()

	// Set host (without port)
	r.Header.Set("X-Forwarded-Host", host)

	// Only omit port if it matches the default for the scheme
	if port != "" {
		if (u.Scheme == "http" && port == "80") || (u.Scheme == "https" && port == "443") {
			r.Header.Del("X-Forwarded-Port")
		} else {
			r.Header.Set("X-Forwarded-Port", port)
		}
	} else {
		r.Header.Del("X-Forwarded-Port")
	}

	return nil
}
