package spdk

import (
	"strings"

	"github.com/sirupsen/logrus"

	spdkclient "github.com/longhorn/go-spdk-helper/pkg/spdk/client"
)

func svcLogSetLevel(spdkClient *spdkclient.Client, level string) error {
	log := logrus.WithFields(logrus.Fields{
		"level": level,
	})

	log.Trace("Setting log level")

	if _, err := spdkClient.LogSetLevel(level); err != nil {
		return err
	}
	if _, err := spdkClient.LogSetPrintLevel(level); err != nil {
		return err
	}
	return nil

}

func svcLogSetFlags(spdkClient *spdkclient.Client, flags string) (err error) {
	log := logrus.WithFields(logrus.Fields{
		"flags": flags,
	})

	log.Trace("Setting log flags")

	if flags == "" {
		_, err = spdkClient.LogClearFlag("all")
		return err
	}

	flagMap := commaSeparatedStringToMap(flags)
	if _, ok := flagMap["all"]; ok {
		_, err = spdkClient.LogSetFlag(flags)
		return err
	}

	currentFlagMap, err := spdkClient.LogGetFlags()
	if err != nil {
		return err
	}

	for flag, enabled := range currentFlagMap {
		targetFlagEnabled := flagMap[flag]
		if enabled != targetFlagEnabled {
			if targetFlagEnabled {
				_, err = spdkClient.LogSetFlag(flag)
			} else {
				_, err = spdkClient.LogClearFlag(flag)
			}
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func commaSeparatedStringToMap(flags string) map[string]bool {
	flagMap := make(map[string]bool)
	if flags == "" {
		return flagMap
	}

	flagList := strings.Split(flags, ",")
	for _, flag := range flagList {
		flagMap[flag] = true
	}

	return flagMap
}

func svcLogGetLevel(spdkClient *spdkclient.Client) (level string, err error) {
	logrus.Trace("Getting log level")

	return spdkClient.LogGetPrintLevel()
}

func svcLogGetFlags(spdkClient *spdkclient.Client) (flags string, err error) {
	logrus.Trace("Getting log flags")

	var flagsMap map[string]bool

	flagsMap, err = spdkClient.LogGetFlags()
	if err != nil {
		return "", err
	}
	return mapToCommaSeparatedString(flagsMap), nil
}

func mapToCommaSeparatedString(m map[string]bool) string {
	if len(m) == 0 {
		return ""
	}

	var s string
	for k, v := range m {
		if v {
			s += k + ","
		}
	}
	return s[:len(s)-1]
}
