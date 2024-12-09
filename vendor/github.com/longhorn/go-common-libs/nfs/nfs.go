package nfs

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/longhorn/go-common-libs/types"
)

const (
	nfsmountGlobalSection  = "NFSMount_Global_Options"
	nfsmountDefaultVersion = "Defaultvers"
)

// GetSystemDefaultNFSVersion reads the system default NFS version. This config can be overridden by nfsmount.conf under
// configDir. If configDir is empty, it will be /etc by default. If the nfsmount.conf is absent, or the default
// version is not overridden in the configuration file, a wrapped ErrNotConfigured error will be returned. It is possible
// for the caller to tell if the configuration is overridden by errors.Is(err, types.ErrNotConfigured).
func GetSystemDefaultNFSVersion(configDir string) (major, minor int, err error) {
	if configDir == "" {
		configDir = types.SysEtcDirectory
	}

	configMap, err := getSystemNFSMountConfigMap(configDir)
	if os.IsNotExist(err) {
		return 0, 0, fmt.Errorf("system default NFS version is not overridden under %q: %w", configDir, types.ErrNotConfigured)
	} else if err != nil {
		return 0, 0, err
	}
	if globalSection, ok := configMap[nfsmountGlobalSection]; ok {
		if configured, ok := globalSection[nfsmountDefaultVersion]; ok {
			majorStr, minorStr, _ := strings.Cut(configured, ".")
			major, err := strconv.Atoi(majorStr)
			if err != nil {
				return 0, 0, errors.Wrapf(err, "invalid NFS major version %q", configured)
			}

			minor := 0
			if minorStr != "" {
				minor, err = strconv.Atoi(minorStr)
				if err != nil {
					return 0, 0, errors.Wrapf(err, "invalid NFS minor version %q", configured)
				}
			}
			return major, minor, nil
		}
	}
	return 0, 0, fmt.Errorf("system default NFS version is not overridden under %q: %w", configDir, types.ErrNotConfigured)
}

// getSystemNFSMountConfigMap reads the nfsmount.conf under configDir. The returned result is in
// map[section]map[key]value structure, and the global options is result["NFSMount_Global_Options"]. Refer to man page
// for more detail: man 5 nfsmount.conf
func getSystemNFSMountConfigMap(configDir string) (map[string]map[string]string, error) {
	configFilePath := filepath.Join(configDir, types.NFSMountFileName)
	configFile, err := os.Open(configFilePath)
	if err != nil {
		return nil, err
	}
	defer configFile.Close()

	configMap := make(map[string]map[string]string)
	scanner := bufio.NewScanner(configFile)
	var section = ""
	var sectionMap map[string]string

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "[") {
			section = strings.TrimSpace(strings.Trim(line, "[]"))
			continue
		}
		if sectionMap = configMap[section]; sectionMap == nil {
			sectionMap = make(map[string]string)
			configMap[section] = sectionMap
		}
		if key, val, isParsable := strings.Cut(line, "="); isParsable {
			sectionMap[strings.TrimSpace(key)] = strings.TrimSpace(val)
		} else {
			return nil, fmt.Errorf("invalid key-value pair: '%s'", line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return configMap, nil
}
