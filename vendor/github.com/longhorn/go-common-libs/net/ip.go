package net

import (
	"fmt"
	"net"
	"os"
	"strings"

	stderrors "errors"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"
)

const (
	EnvPodIP = "POD_IP"

	StorageNetworkInterface = "lhnet1"
)

var errInterfaceNotFound = stderrors.New("interface not found")

// IPFamily identifies an IP address family.
type IPFamily string

const (
	IPFamilyUnspecified IPFamily = ""
	IPFamilyIPv4        IPFamily = "ipv4"
	IPFamilyIPv6        IPFamily = "ipv6"
)

// ParseIPFamily parses an IP family case-insensitively. An empty value returns IPFamilyUnspecified.
func ParseIPFamily(value string) (IPFamily, error) {
	family := IPFamily(strings.ToLower(value))
	switch family {
	case IPFamilyUnspecified, IPFamilyIPv4, IPFamilyIPv6:
		return family, nil
	default:
		return "", fmt.Errorf("invalid IP family %q", value)
	}
}

// ParseIPFamilyFromAddress returns the family of a bare IP address or an
// IP address with a port. Hostnames and malformed addresses are rejected.
func ParseIPFamilyFromAddress(address string) (IPFamily, error) {
	host := address
	ip := net.ParseIP(host)
	if ip == nil {
		var err error
		host, _, err = net.SplitHostPort(address)
		if err != nil {
			return "", fmt.Errorf("invalid IP address %q: %w", address, err)
		}
		ip = net.ParseIP(host)
	}
	if ip == nil {
		return "", fmt.Errorf("invalid IP address %q", address)
	}
	if ip.To4() != nil {
		return IPFamilyIPv4, nil
	}
	return IPFamilyIPv6, nil
}

// getInterfaceAddrs returns the addresses for an interface.
//
// If the named interface is absent, the returned error wraps
// errInterfaceNotFound. Errors from interface enumeration, a down interface,
// or address enumeration are returned to the caller.
func getInterfaceAddrs(name string) ([]net.Addr, error) {
	return getInterfaceAddrsWithHooks(name, net.Interfaces, func(iface net.Interface) ([]net.Addr, error) {
		return iface.Addrs()
	})
}

// getInterfaceAddrsWithHooks is the testable implementation of
// getInterfaceAddrs. It returns an errInterfaceNotFound-wrapped error when
// name is absent and returns errors from the matching interface unchanged or
// with context.
func getInterfaceAddrsWithHooks(name string,
	listInterfaces func() ([]net.Interface, error),
	listAddrs func(net.Interface) ([]net.Addr, error)) ([]net.Addr, error) {
	interfaces, err := listInterfaces()
	if err != nil {
		return nil, err
	}

	for i := range interfaces {
		if interfaces[i].Name != name {
			continue
		}
		if interfaces[i].Flags&net.FlagUp == 0 {
			return nil, errors.Errorf("interface %s is down", name)
		}

		addrs, err := listAddrs(interfaces[i])
		if err != nil {
			return nil, errors.Wrapf(err, "failed to list addresses for interface %s", name)
		}
		if len(addrs) == 0 {
			return nil, errors.Errorf("interface %s doesn't have address", name)
		}

		return addrs, nil
	}

	return nil, errors.Wrapf(errInterfaceNotFound, "interface %s not found", name)
}

func getIPFromAddr(addr net.Addr) net.IP {
	switch addr := addr.(type) {
	case *net.IPNet:
		if addr == nil {
			return nil
		}
		return addr.IP
	case *net.IPAddr:
		if addr == nil {
			return nil
		}
		return addr.IP
	default:
		return nil
	}
}

func getLocalIPFromAddrsByFamily(addrs []net.Addr, family IPFamily) string {
	for _, addr := range addrs {
		ip := getIPFromAddr(addr)
		if IsUsableIPForFamily(ip, family) {
			return ip.String()
		}
	}

	return ""
}

func getInterfaceNameByIP(ip net.IP) (string, error) {
	return getInterfaceNameByIPWithHooks(ip, net.Interfaces, func(iface net.Interface) ([]net.Addr, error) {
		return iface.Addrs()
	})
}

func getInterfaceNameByIPWithHooks(
	ip net.IP,
	interfacesFunc func() ([]net.Interface, error),
	addrsFunc func(net.Interface) ([]net.Addr, error),
) (string, error) {
	if ip == nil {
		return "", nil
	}

	interfaces, err := interfacesFunc()
	if err != nil {
		return "", err
	}

	for _, iface := range interfaces {
		addrs, err := addrsFunc(iface)
		if err != nil {
			return "", errors.Wrapf(err, "failed to list addresses for interface %s", iface.Name)
		}

		for _, addr := range addrs {
			addrIP := getIPFromAddr(addr)
			if addrIP != nil && addrIP.To16() != nil && addrIP.Equal(ip) {
				return iface.Name, nil
			}
		}
	}

	return "", nil
}

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

	if ip := getLocalIPFromAddrsByFamily(addrs, IPFamilyIPv4); ip != "" {
		return ip, nil
	}

	return "", errors.Errorf("interface %s don't have an IPv4 address", name)
}

func getAvailablePodAddrs(podIP string,
	interfaceAddrs func(string) ([]net.Addr, error),
	interfaceNameByIP func(net.IP) (string, error)) ([]net.Addr, error) {
	addrs, err := interfaceAddrs(StorageNetworkInterface)
	if err == nil {
		if len(addrs) != 0 {
			return addrs, nil
		}
		return nil, errors.Errorf("interface %s doesn't have address", StorageNetworkInterface)
	}
	if !errors.Is(err, errInterfaceNotFound) {
		return nil, err
	}
	logrus.WithError(err).Tracef("Failed to get IP from %v interface, fallback to use the default pod IP %v",
		StorageNetworkInterface, podIP)

	parsedPodIP := net.ParseIP(podIP)
	if parsedPodIP == nil {
		return nil, errors.Errorf("invalid %s %q", EnvPodIP, podIP)
	}

	// Keep the primary POD_IP first so it remains the preferred address when
	// the primary pod interface also has other addresses.
	addrs = []net.Addr{&net.IPAddr{IP: parsedPodIP}}
	interfaceName, err := interfaceNameByIP(parsedPodIP)
	if err != nil {
		return nil, err
	}
	if interfaceName == "" {
		return addrs, nil
	}

	interfaceAddrsForPodIP, err := interfaceAddrs(interfaceName)
	if err != nil {
		return nil, err
	}
	for _, addr := range interfaceAddrsForPodIP {
		ip := getIPFromAddr(addr)
		if ip == nil || !ip.Equal(parsedPodIP) {
			addrs = append(addrs, addr)
		}
	}

	return addrs, nil
}

func getIPForPod(family IPFamily, podIP string,
	interfaceAddrs func(string) ([]net.Addr, error),
	interfaceNameByIP func(net.IP) (string, error)) (string, error) {
	switch family {
	case IPFamilyUnspecified, IPFamilyIPv4, IPFamilyIPv6:
	default:
		return "", fmt.Errorf("invalid IP family %q", family)
	}

	addrs, err := getAvailablePodAddrs(podIP, interfaceAddrs, interfaceNameByIP)
	if err != nil {
		return "", err
	}
	if ip := getLocalIPFromAddrsByFamily(addrs, family); ip != "" {
		return ip, nil
	}

	familyDesc := string(family)
	if family == IPFamilyUnspecified {
		familyDesc = "IP"
	}
	return "", fmt.Errorf("no usable %s address found on interface %s or from %s", familyDesc, StorageNetworkInterface, EnvPodIP)
}

// SelectIPByNetworkPreference selects the first usable global-unicast address
// from the authoritative candidate list, preserving its order.
func SelectIPByNetworkPreference(storageNetworkPresent bool, storageIPs []string, podIPs []string) (string, error) {
	candidates := podIPs
	if storageNetworkPresent {
		candidates = storageIPs
	}

	for _, candidate := range candidates {
		if ip := net.ParseIP(candidate); IsUsableIPForFamily(ip, IPFamilyUnspecified) {
			return ip.String(), nil
		}
	}

	if storageNetworkPresent {
		return "", errors.Errorf("storage network interface %s has no usable global-unicast address", StorageNetworkInterface)
	}

	return "", fmt.Errorf("can't get a valid ip from either the specified interface or the environment variable")
}

// GetIPForPodByNetwork returns the pod IP selected from the storage network,
// falling back to the primary pod interface when the storage network is absent.
func GetIPForPodByNetwork() (ip string, err error) {
	return GetIPForPodByNetworkAndFamily(IPFamilyUnspecified)
}

// GetIPForPod returns the pod IP selected from the storage network, falling
// back to the primary pod interface when the storage network is absent.
//
// Deprecated: GetIPForPod has a misleading generic name and retains
// unspecified-family selection. Use GetIPForPodByNetwork for the same behavior,
// or GetIPForPodByNetworkAndFamily for explicit family selection.
func GetIPForPod() (ip string, err error) {
	return GetIPForPodByNetwork()
}

// GetIPForPodByNetworkAndFamily returns the pod IP for the requested address
// family from the storage network, or from the primary pod interface when
// the storage network is absent. It does not fall back on family mismatch.
func GetIPForPodByNetworkAndFamily(family IPFamily) (ip string, err error) {
	return getIPForPod(
		family,
		os.Getenv(EnvPodIP),
		getInterfaceAddrs,
		getInterfaceNameByIP,
	)
}

// IsUsableIPForFamily reports whether ip is a global-unicast address in the
// requested family.
func IsUsableIPForFamily(ip net.IP, family IPFamily) bool {
	if ip == nil || !ip.IsGlobalUnicast() {
		return false
	}

	switch family {
	case IPFamilyUnspecified:
		return true
	case IPFamilyIPv4:
		return ip.To4() != nil
	case IPFamilyIPv6:
		return ip.To4() == nil && ip.To16() != nil
	default:
		return false
	}
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
