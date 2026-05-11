package util

import (
	"net"
	"strconv"
	"strings"
)

// BuildTargetAddress returns a host:port target string or an empty string when
// the target endpoint is incomplete.
func BuildTargetAddress(host string, port int) string {
	if host == "" || port <= 0 {
		return ""
	}

	return net.JoinHostPort(host, strconv.Itoa(port))
}

// BuildHTTPURL returns an HTTP URL with a host:port authority that is valid for
// both IPv4 and IPv6 literal addresses.
func BuildHTTPURL(host string, port int, path string) string {
	targetAddress := BuildTargetAddress(host, port)
	if targetAddress == "" {
		return ""
	}

	return "http://" + targetAddress + "/" + strings.TrimPrefix(path, "/")
}
