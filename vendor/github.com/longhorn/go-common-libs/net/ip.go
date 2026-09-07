package net

import (
	"fmt"
	"net"
	"os"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
)

const (
	EnvPodIP = "POD_IP"

	StorageNetworkInterface = "lhnet1"
)

// GetLocalIPv4fromInterface returns the local IPv4 address.
//
// Deprecated: use GetLocalIPFromInterface instead, which also supports IPv6-only
// interfaces.
func GetLocalIPv4fromInterface(name string) (ip string, err error) {
	return getLocalIPFromInterface(name, true)
}

// GetLocalIPFromInterface returns the local IP address of the given interface.
// IPv4 is preferred, and a global unicast IPv6 address is returned when the
// interface has no IPv4 address.
func GetLocalIPFromInterface(name string) (ip string, err error) {
	return getLocalIPFromInterface(name, false)
}

func getLocalIPFromInterface(name string, ipv4Only bool) (string, error) {
	iface, err := net.InterfaceByName(name)
	if err != nil {
		return "", err
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return "", errors.Wrapf(err, "interface %s doesn't have address", name)
	}

	var ipv6 net.IP
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP == nil {
			continue
		}

		if ipv4 := ipNet.IP.To4(); ipv4 != nil {
			return ipv4.String(), nil
		}

		// Link-local and multicast addresses are not routable between nodes.
		if ipv6 == nil && ipNet.IP.IsGlobalUnicast() {
			ipv6 = ipNet.IP
		}
	}

	if ipv4Only {
		return "", errors.Errorf("interface %s don't have an IPv4 address", name)
	}
	if ipv6 == nil {
		return "", errors.Errorf("interface %s doesn't have an IPv4 or a global unicast IPv6 address", name)
	}

	return ipv6.String(), nil
}

// GetIPForPod returns the IP address for the pod from the storage network first or the cluster network.
func GetIPForPod() (ip string, err error) {
	storageIP, err := GetLocalIPFromInterface(StorageNetworkInterface)
	if err == nil {
		return storageIP, nil
	}

	podIP := os.Getenv(EnvPodIP)
	if _, ifaceErr := net.InterfaceByName(StorageNetworkInterface); ifaceErr == nil {
		// The storage network is attached but unusable, so falling back to the cluster
		// network silently bypasses it. Make that visible instead of hiding it.
		logrus.WithError(err).Warnf("Failed to get IP from %v interface, fallback to use the default pod IP %v",
			StorageNetworkInterface, podIP)
	} else {
		logrus.WithError(err).Tracef("Failed to get IP from %v interface, fallback to use the default pod IP %v",
			StorageNetworkInterface, podIP)
	}

	if podIP == "" {
		return "", fmt.Errorf("can't get a ip from either the specified interface or the environment variable")
	}

	return podIP, nil
}

// IsLoopbackHost checks if the given host is a loopback host.
func IsLoopbackHost(host string) bool {
	if host == "localhost" || host == "127.0.0.1" || host == "0.0.0.0" || host == "::1" || host == "" {
		return true
	}
	// Check for loopback network.
	ips, err := net.LookupIP(host)
	if err != nil {
		return false
	}

	for _, ip := range ips {
		if !ip.IsLoopback() {
			return false
		}
	}

	return true
}

// GetAnyExternalIP returns any external IP address. IPv4 is preferred, and a
// global unicast IPv6 address is returned when no IPv4 address is available.
func GetAnyExternalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

	var ipv6 net.IP
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 {
			continue // interface down
		}

		if iface.Flags&net.FlagLoopback != 0 {
			continue // loopback interface
		}

		addrs, err := iface.Addrs()
		if err != nil {
			return "", err
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			if ip == nil || ip.IsLoopback() {
				continue
			}
			if ipv4 := ip.To4(); ipv4 != nil {
				return ipv4.String(), nil
			}
			if ipv6 == nil && ip.IsGlobalUnicast() {
				ipv6 = ip
			}
		}
	}

	if ipv6 != nil {
		return ipv6.String(), nil
	}

	return "", fmt.Errorf("the current host is probably not connected to the network")
}
