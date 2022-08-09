package util

import (
	"errors"
	"net/url"
	"os"

	"github.com/longhorn/backing-image-manager/pkg/types"
	"github.com/longhorn/backupstore"

	// import backupstore/nfs to register nfs driver
	_ "github.com/longhorn/backupstore/nfs"
	// import backupstore/s3 to register s3 driver
	_ "github.com/longhorn/backupstore/s3"
)

func checkBackupType(backupTarget string) (string, error) {
	u, err := url.Parse(backupTarget)
	if err != nil {
		return "", err
	}

	return u.Scheme, nil
}

func GetBackupDriver(backupTarget string, credential map[string]string) (backupstore.BackupStoreDriver, error) {
	backupType, err := checkBackupType(backupTarget)
	if err != nil {
		return nil, err
	}
	// set aws credential
	if backupType == "s3" {
		if credential != nil {
			if credential[types.AWSAccessKey] == "" && credential[types.AWSSecretKey] != "" {
				return nil, errors.New("could not backup to s3 without setting credential access key")
			}
			if credential[types.AWSAccessKey] != "" && credential[types.AWSSecretKey] == "" {
				return nil, errors.New("could not backup to s3 without setting credential secret access key")
			}
			if credential[types.AWSAccessKey] != "" && credential[types.AWSSecretKey] != "" {
				os.Setenv(types.AWSAccessKey, credential[types.AWSAccessKey])
				os.Setenv(types.AWSSecretKey, credential[types.AWSSecretKey])
			}

			os.Setenv(types.AWSEndPoint, credential[types.AWSEndPoint])
			os.Setenv(types.HTTPSProxy, credential[types.HTTPSProxy])
			os.Setenv(types.HTTPProxy, credential[types.HTTPProxy])
			os.Setenv(types.NOProxy, credential[types.NOProxy])
			os.Setenv(types.VirtualHostedStyle, credential[types.VirtualHostedStyle])

			// set a custom ca cert if available
			if credential[types.AWSCert] != "" {
				os.Setenv(types.AWSCert, credential[types.AWSCert])
			}
		}
	}
	return backupstore.GetBackupStoreDriver(backupTarget)
}
