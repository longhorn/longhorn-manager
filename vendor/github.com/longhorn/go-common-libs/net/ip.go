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
func GetLocalIPv4fromInterface(name string) (ip string, err error) {
	iface, err := net.InterfaceByName(name)
	if err != nil {
		return "", err
	}

	addrs, err := iface.Addrs()
	if err != nil {
		return "", errors.Wrapf(err, "interface %s doesn't have address", name)
	}

	var ipv4 net.IP
	for _, addr := range addrs {
		if ipv4 = addr.(*net.IPNet).IP.To4(); ipv4 != nil {
			break
		}
	}
	if ipv4 == nil {
		return "", errors.Errorf("interface %s don't have an IPv4 address", name)
	}

	return ipv4.String(), nil
}

// GetIPForPod returns the IP address for the pod from the storage network first or the cluster network.
func GetIPForPod() (ip string, err error) {
	var storageIP string
	if ip, err := GetLocalIPv4fromInterface(StorageNetworkInterface); err != nil {
		storageIP = os.Getenv(EnvPodIP)
		logrus.WithError(err).Tracef("Failed to get IP from %v interface, fallback to use the default pod IP %v",
			StorageNetworkInterface, storageIP)
	} else {
		storageIP = ip
	}

	if storageIP == "" {
		return "", fmt.Errorf("can't get a ip from either the specified interface or the environment variable")
	}

	return storageIP, nil
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

// GetAnyExternalIP returns any external IP address.
func GetAnyExternalIP() (string, error) {
	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}

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
			ip = ip.To4()
			if ip == nil {
				continue // not an ipv4 address
			}
			return ip.String(), nil
		}
	}

	return "", fmt.Errorf("the current host is probably not connected to the network")
}
