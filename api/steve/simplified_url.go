package steve

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strings"

	"github.com/gorilla/mux"
	"github.com/sirupsen/logrus"
)

// resourceMapping maps simplified resource names to full Steve schema IDs
var resourceMapping = map[string]string{
	"volumes":             "longhorn.io.volumes",
	"nodes":               "longhorn.io.nodes",
	"engineimages":        "longhorn.io.engineimages",
	"backingimages":       "longhorn.io.backingimages",
	"backups":             "longhorn.io.backups",
	"backupvolumes":       "longhorn.io.backupvolumes",
	"backuptargets":       "longhorn.io.backuptargets",
	"backupbackingimages": "longhorn.io.backupbackingimages",
	"recurringjobs":       "longhorn.io.recurringjobs",
	"snapshots":           "longhorn.io.snapshots",
	"settings":            "longhorn.io.settings",
	"orphans":             "longhorn.io.orphans",
	"systembackups":       "longhorn.io.systembackups",
	"systemrestores":      "longhorn.io.systemrestores",
	"supportbundles":      "longhorn.io.supportbundles",
	"replicas":            "longhorn.io.replicas",
	"engines":             "longhorn.io.engines",
	"instancemanagers":    "longhorn.io.instancemanagers",
	"sharemanagers":       "longhorn.io.sharemanagers",
	"volumeattachments":   "longhorn.io.volumeattachments",
}

// reverseResourceMapping maps full Steve schema IDs to simplified resource names
var reverseResourceMapping = map[string]string{}

func init() {
	// Build reverse mapping
	for k, v := range resourceMapping {
		reverseResourceMapping[v] = k
	}
}

// SimplifiedPathMiddleware rewrites simplified paths to full Steve paths
// and rewrites response URLs back to simplified format.
//
// Simplified format:  /v1/backuptargets/{name}?action=backupTargetSync
// Full Steve format:  /v1/longhorn.io.backuptargets/longhorn-system/{name}?action=backupTargetSync
func SimplifiedPathMiddleware(namespace string, next http.Handler) http.Handler {
	// Regex to match simplified paths: /v1/{resource}/{name}
	// e.g., /v1/volumes/pvc-xxx or /v1/backuptargets/default
	simplifiedPathRegex := regexp.MustCompile(`^/v1/([a-z]+)(?:/([^/]+))?(.*)$`)

	// Regex to match full Steve paths without namespace: /v1/longhorn.io.{resource}/{name}
	// e.g., /v1/longhorn.io.backuptargets/default
	fullPathWithoutNsRegex := regexp.MustCompile(`^/v1/(longhorn\.io\.[a-z]+)/([^/]+)$`)

	// Regex to match full Steve paths WITH namespace: /v1/longhorn.io.{resource}/{namespace}/{name}
	// e.g., /v1/longhorn.io.backuptargets/longhorn-system/default
	fullPathWithNsRegex := regexp.MustCompile(`^/v1/(longhorn\.io\.[a-z]+)/([^/]+)/([^/]+)(.*)$`)

	// Regex to match collection paths: /v1/longhorn.io.{resource} (no name, no namespace)
	// e.g., /v1/longhorn.io.backuptargets
	collectionPathRegex := regexp.MustCompile(`^/v1/(longhorn\.io\.[a-z]+)$`)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		originalPath := r.URL.Path

		// Debug logging for all requests
		logrus.Infof("[SimplifiedPathMiddleware] Method: %s, Path: %s, Query: %s", r.Method, originalPath, r.URL.RawQuery)

		// Variables to track if we need to set mux vars
		var resourceType, resourceName string

		// Check various path patterns and transform as needed
		// All paths will use response rewriting at the end

		if fullMatches := fullPathWithNsRegex.FindStringSubmatch(originalPath); fullMatches != nil {
			// Path is already in correct format: /v1/longhorn.io.xxx/namespace/name
			// No transformation needed, but extract vars for mux
			resourceType = fullMatches[1]  // e.g., longhorn.io.backuptargets
			resourceName = fullMatches[3]  // e.g., default (third group is name)
			logrus.Debugf("Path already in full format, passing through: %s", originalPath)
		} else if collectionPathRegex.MatchString(originalPath) {
			// Collection path: /v1/longhorn.io.backuptargets - no transformation needed
			// Extract type for mux
			if matches := collectionPathRegex.FindStringSubmatch(originalPath); matches != nil {
				resourceType = matches[1]
			}
			logrus.Debugf("Collection path, passing through: %s", originalPath)
		} else if fullMatches := fullPathWithoutNsRegex.FindStringSubmatch(originalPath); fullMatches != nil {
			// Match full Steve path without namespace and inject namespace
			resourceType = fullMatches[1] // e.g., longhorn.io.backuptargets
			resourceName = fullMatches[2] // e.g., default

			// Inject namespace
			newPath := "/v1/" + resourceType + "/" + namespace + "/" + resourceName
			logrus.Debugf("Rewriting full path (inject namespace): %s -> %s", originalPath, newPath)
			r.URL.Path = newPath
		} else {
			// Try to match simplified path and expand it
			matches := simplifiedPathRegex.FindStringSubmatch(originalPath)
			if matches != nil {
				resource := matches[1] // e.g., volumes, backuptargets
				name := matches[2]     // e.g., pvc-xxx, default (optional)
				rest := matches[3]     // e.g., ?action=attach

				// Check if this is a simplified resource name
				if fullResource, ok := resourceMapping[resource]; ok {
					resourceType = fullResource
					resourceName = name
					var newPath string
					if name != "" {
						// /v1/backuptargets/default -> /v1/longhorn.io.backuptargets/longhorn-system/default
						newPath = "/v1/" + fullResource + "/" + namespace + "/" + name + rest
					} else {
						// /v1/backuptargets -> /v1/longhorn.io.backuptargets
						newPath = "/v1/" + fullResource + rest
					}
					logrus.Debugf("Rewriting simplified path: %s -> %s", originalPath, newPath)
					r.URL.Path = newPath
				}
			}
		}

		// Set mux variables so Steve's MuxURLParser can find them
		// This is needed because we're modifying the path after the original mux routing
		if resourceType != "" {
			vars := map[string]string{
				"type":      resourceType,
				"namespace": namespace,
			}
			if resourceName != "" {
				vars["name"] = resourceName
			}
			// Also check for action in query string
			if action := r.URL.Query().Get("action"); action != "" {
				vars["action"] = action
			}
			r = mux.SetURLVars(r, vars)
			logrus.Debugf("[SimplifiedPathMiddleware] Set mux vars: type=%s, namespace=%s, name=%s", resourceType, namespace, resourceName)
		}

		// Skip response wrapping for WebSocket upgrade requests
		// WebSocket connections need direct access to the underlying connection
		if isWebSocketUpgrade(r) {
			logrus.Infof("[SimplifiedPathMiddleware] WebSocket upgrade detected (Upgrade=%q, Connection=%q), skipping URL rewriting",
				r.Header.Get("Upgrade"), r.Header.Get("Connection"))
			next.ServeHTTP(w, r)
			return
		}

		// Debug: log final path being sent to Steve
		logrus.Infof("[SimplifiedPathMiddleware] Sending to Steve: Method=%s Path=%s", r.Method, r.URL.Path)

		// Use a response wrapper to rewrite URLs in ALL responses
		// The wrapper buffers the response and rewrites URLs when flushed
		wrappedWriter := &urlRewritingResponseWriter{
			ResponseWriter: w,
			namespace:      namespace,
			statusCode:     http.StatusOK, // Default status code
		}
		next.ServeHTTP(wrappedWriter, r)
		// Flush the buffer to send the rewritten response
		wrappedWriter.flushBuffer()
	})
}

// isWebSocketUpgrade checks if the request is a WebSocket upgrade request.
// A valid WebSocket upgrade requires BOTH:
// - Upgrade header set to "websocket"
// - Connection header containing "upgrade"
// Note: Rancher proxy often sets Connection: upgrade for keep-alive, so we must check both.
func isWebSocketUpgrade(r *http.Request) bool {
	return strings.ToLower(r.Header.Get("Upgrade")) == "websocket" &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// urlRewritingResponseWriter wraps http.ResponseWriter to rewrite URLs in response body.
// It buffers the entire response to handle chunked encoding where URLs might be split across chunks.
type urlRewritingResponseWriter struct {
	http.ResponseWriter
	namespace    string
	buffer       []byte
	wroteHeader  bool
	statusCode   int
}

func (w *urlRewritingResponseWriter) WriteHeader(statusCode int) {
	w.statusCode = statusCode
	w.wroteHeader = true
	// Don't call underlying WriteHeader yet - wait until we flush the buffer
}

func (w *urlRewritingResponseWriter) Write(b []byte) (int, error) {
	// Buffer all writes - Steve may send response in multiple chunks
	w.buffer = append(w.buffer, b...)
	logrus.Debugf("[urlRewritingResponseWriter] Write called, buffered %d bytes, total buffer: %d", len(b), len(w.buffer))
	return len(b), nil
}

// Flush rewrites URLs and sends the buffered response
func (w *urlRewritingResponseWriter) Flush() {
	// First, process any buffered content
	if len(w.buffer) > 0 {
		w.flushBuffer()
	}
	// Then call the underlying Flush if available
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *urlRewritingResponseWriter) flushBuffer() {
	if len(w.buffer) == 0 {
		return
	}

	content := string(w.buffer)
	originalContent := content

	logrus.Infof("[urlRewritingResponseWriter] Flushing buffer, content length=%d, namespace=%s", len(w.buffer), w.namespace)

	// Replace paths - convert full Steve format to simplified public format
	// User wants: /v1/backuptargets/default (not /v1/longhorn.io.backuptargets/longhorn-system/default)
	for fullResource, simpleResource := range reverseResourceMapping {
		// 1. Replace paths with namespace and resource name (including query strings like ?action=xxx)
		// e.g., /v1/longhorn.io.backuptargets/longhorn-system/default?action=backupTargetSync
		//    -> /v1/backuptargets/default?action=backupTargetSync
		oldPattern := "/v1/" + fullResource + "/" + w.namespace + "/"
		newPattern := "/v1/" + simpleResource + "/"
		content = strings.ReplaceAll(content, oldPattern, newPattern)

		// 2. Replace collection paths ending with quote (JSON string terminator)
		// e.g., /v1/longhorn.io.backuptargets" -> /v1/backuptargets"
		oldCollection := "/v1/" + fullResource + "\""
		newCollection := "/v1/" + simpleResource + "\""
		content = strings.ReplaceAll(content, oldCollection, newCollection)

		// 3. Replace collection paths ending with } (for createTypes object)
		// e.g., /v1/longhorn.io.backuptargets} -> /v1/backuptargets}
		oldCollectionBrace := "/v1/" + fullResource + "}"
		newCollectionBrace := "/v1/" + simpleResource + "}"
		content = strings.ReplaceAll(content, oldCollectionBrace, newCollectionBrace)
	}

	// Debug log to verify rewriting is happening
	if content != originalContent {
		logrus.Infof("[urlRewritingResponseWriter] URLs rewritten successfully")
	} else {
		logrus.Warnf("[urlRewritingResponseWriter] No URLs were rewritten")
	}

	// Now write the modified content
	// Update Content-Length header since content length may have changed
	w.ResponseWriter.Header().Set("Content-Length", fmt.Sprintf("%d", len(content)))
	w.ResponseWriter.Header().Del("Transfer-Encoding") // Remove chunked encoding

	if w.wroteHeader {
		w.ResponseWriter.WriteHeader(w.statusCode)
	}
	w.ResponseWriter.Write([]byte(content))
	w.buffer = nil
}

// Hijack implements http.Hijacker interface for WebSocket support
func (w *urlRewritingResponseWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hijacker, ok := w.ResponseWriter.(http.Hijacker); ok {
		return hijacker.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}
