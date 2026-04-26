package util

import (
	"net"
	"strconv"
)

// BuildTargetAddress returns a host:port target string or an empty string when
// the target endpoint is incomplete.
func BuildTargetAddress(host string, port int) string {
	if host == "" || port <= 0 {
		return ""
	}

	return net.JoinHostPort(host, strconv.Itoa(port))
}
