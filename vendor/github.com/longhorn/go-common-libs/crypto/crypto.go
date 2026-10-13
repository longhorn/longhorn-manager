// Package crypto wraps the cryptsetup operations Longhorn performs on an
// encrypted volume's device: format, open, close, resize and status. It is
// shared by every process that owns a LUKS mapping — the CSI node plugin for
// RWO and migratable RWX volumes, and the share manager for the rest — so the
// mapper naming and the LUKS options stay identical across them.
package crypto

import (
	"fmt"
	"path"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/sirupsen/logrus"

	lhns "github.com/longhorn/go-common-libs/ns"
	lhtypes "github.com/longhorn/go-common-libs/types"
)

const (
	// MapperFilePathPrefix is where device-mapper exposes an opened LUKS device.
	MapperFilePathPrefix = "/dev/mapper"
	mapperV2VolumeSuffix = "-encrypted"

	// dataEngineTypeV2 is the data engine whose volumes already own a dm device
	// under the volume's name, so the LUKS mapping needs a distinct one.
	dataEngineTypeV2 = "v2"

	CryptoKeyDefaultCipher = "aes-xts-plain64"
	CryptoKeyDefaultHash   = "sha256"
	CryptoKeyDefaultSize   = "256"
	CryptoDefaultPBKDF     = "argon2i"

	// Prefer no default value to avoid changing the existing cryptsetup behavior.
	// When empty, Longhorn does not pass the argument to cryptsetup, and cryptsetup will use its default value.
	CryptoDefaultPBKDFForceIterations = ""
	CryptoDefaultPBKDFMemory          = ""

	// Luks2MinimalVolumeSize the minimal volume size for the LUKS2format encryption.
	//  https://gitlab.com/cryptsetup/cryptsetup/-/wikis/FrequentlyAskedQuestions
	//  Section 10.10 What about the size of the LUKS2 header
	//  The default size is 16MB
	Luks2MinimalVolumeSize = lhtypes.Luks2EncryptionHeaderSize
)

// EncryptParams keeps the customized cipher options from the volume's Secret.
type EncryptParams struct {
	KeyProvider          string
	KeyCipher            string
	KeyHash              string
	KeySize              string
	PBKDF                string
	PBKDFForceIterations string // Optional. Passed to cryptsetup as --pbkdf-force-iterations when set.
	PBKDFMemory          string // Optional. Passed to cryptsetup as --pbkdf-memory when set. Unit: KiB.
}

func NewEncryptParams(keyProvider, keyCipher, keyHash, keySize, pbkdf, pbkdfForceIterations, pbkdfMemory string) *EncryptParams {
	return &EncryptParams{
		KeyProvider:          keyProvider,
		KeyCipher:            keyCipher,
		KeyHash:              keyHash,
		KeySize:              keySize,
		PBKDF:                pbkdf,
		PBKDFForceIterations: pbkdfForceIterations,
		PBKDFMemory:          pbkdfMemory,
	}
}

func (cp *EncryptParams) GetKeyCipher() string {
	if cp.KeyCipher == "" {
		return CryptoKeyDefaultCipher
	}
	return cp.KeyCipher
}

func (cp *EncryptParams) GetKeyHash() string {
	if cp.KeyHash == "" {
		return CryptoKeyDefaultHash
	}
	return cp.KeyHash
}

func (cp *EncryptParams) GetKeySize() string {
	if cp.KeySize == "" {
		return CryptoKeyDefaultSize
	}
	return cp.KeySize
}

func (cp *EncryptParams) GetPBKDF() string {
	if cp.PBKDF == "" {
		return CryptoDefaultPBKDF
	}
	return cp.PBKDF
}

func (cp *EncryptParams) GetPBKDFForceIterations() string {
	if cp.PBKDFForceIterations == "" {
		return CryptoDefaultPBKDFForceIterations
	}
	return cp.PBKDFForceIterations
}

func (cp *EncryptParams) GetPBKDFMemory() string {
	if cp.PBKDFMemory == "" {
		return CryptoDefaultPBKDFMemory
	}
	return cp.PBKDFMemory
}

// LuksFormatOptions returns the cryptsetup options these parameters describe.
func (cp *EncryptParams) LuksFormatOptions() *lhns.LuksFormatOptions {
	return &lhns.LuksFormatOptions{
		KeyCipher:            cp.GetKeyCipher(),
		KeyHash:              cp.GetKeyHash(),
		KeySize:              cp.GetKeySize(),
		PBKDF:                cp.GetPBKDF(),
		PBKDFForceIterations: cp.GetPBKDFForceIterations(),
		PBKDFMemory:          cp.GetPBKDFMemory(),
	}
}

// EncryptVolumeName is the device-mapper name of the volume's LUKS mapping.
func EncryptVolumeName(volume, dataEngine string) string {
	if dataEngine == dataEngineTypeV2 {
		return volume + mapperV2VolumeSuffix
	}
	return volume
}

// VolumeMapper returns the path of the volume's mapped encrypted device.
func VolumeMapper(volume, dataEngine string) string {
	if dataEngine == dataEngineTypeV2 {
		// v2 volume will use a dm device as default to control IO path when attaching.
		// This dm device will be created with the same name as the volume name.
		// The encrypted volume will be created with the volume name with "-encrypted" suffix to resolve the naming conflict.
		return path.Join(MapperFilePathPrefix, EncryptVolumeName(volume, dataEngine))
	}
	return path.Join(MapperFilePathPrefix, volume)
}

func newNamespaceExecutor() (*lhns.Executor, error) {
	namespaces := []lhtypes.Namespace{lhtypes.NamespaceMnt, lhtypes.NamespaceIpc}
	return lhns.NewNamespaceExecutor(lhtypes.ProcessNone, lhtypes.HostProcDirectory, namespaces)
}

// EncryptVolume encrypts the provided device with LUKS. A device that already
// carries a LUKS header is left alone: formatting it again would discard the
// key that opens the data on it.
func EncryptVolume(devicePath, passphrase string, cryptoParams *EncryptParams) error {
	isEncrypted, err := IsDeviceEncrypted(devicePath)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to check IsDeviceEncrypted before encrypting volume %v", devicePath)
		return err
	}
	if isEncrypted {
		logrus.Infof("The device %v is already encrypted. Skipping the encryption to avoid data lost", devicePath)
		return nil
	}

	nsexec, err := newNamespaceExecutor()
	if err != nil {
		return err
	}
	logrus.Infof("Encrypting device %s with LUKS", devicePath)
	if _, err := nsexec.LuksFormat(devicePath, passphrase, cryptoParams.LuksFormatOptions(), lhtypes.LuksTimeout); err != nil {
		return errors.Wrapf(err, "failed to encrypt device %s with LUKS", devicePath)
	}

	isEncrypted, err = IsDeviceEncrypted(devicePath)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to check IsDeviceEncrypted after encrypting volume %v", devicePath)
		return err
	}
	if !isEncrypted {
		logrus.Warnf("Attempted to encrypt device %s with LUKS, but it is not encrypted", devicePath)
		return nil
	}

	logrus.Infof("Device %s is encrypted with LUKS", devicePath)
	return nil
}

// OpenVolume opens the volume so that it can be used by the client. devicePath
// is the path of the volume on the host, for instance '/dev/longhorn/volume1'.
func OpenVolume(volume, dataEngine, devicePath, passphrase string) error {
	mapperPath := VolumeMapper(volume, dataEngine)
	if isOpen, _ := IsDeviceOpen(mapperPath); isOpen {
		logrus.Infof("Device %s is already opened at %s", devicePath, mapperPath)
		return nil
	}

	nsexec, err := newNamespaceExecutor()
	if err != nil {
		return err
	}

	encryptVolumeName := EncryptVolumeName(volume, dataEngine)
	logrus.Infof("Opening device %s with LUKS on %s", devicePath, encryptVolumeName)
	if _, err := nsexec.LuksOpen(encryptVolumeName, devicePath, passphrase, lhtypes.LuksTimeout); err != nil {
		logrus.WithError(err).Warnf("Failed to open LUKS device %s to %s", devicePath, encryptVolumeName)
		return err
	}
	return nil
}

// CloseVolume closes the encrypted volume so it can be detached.
func CloseVolume(volume, dataEngine string) error {
	nsexec, err := newNamespaceExecutor()
	if err != nil {
		return err
	}

	encryptVolumeName := EncryptVolumeName(volume, dataEngine)
	logrus.Infof("Closing LUKS device %s", encryptVolumeName)
	_, err = nsexec.LuksClose(encryptVolumeName, lhtypes.LuksTimeout)
	return err
}

// ResizeEncryptoDevice resizes the LUKS mapping after the underlying device grew.
func ResizeEncryptoDevice(volume, dataEngine, passphrase string) error {
	mapperPath := VolumeMapper(volume, dataEngine)
	if isOpen, err := IsDeviceOpen(mapperPath); err != nil {
		return err
	} else if !isOpen {
		return fmt.Errorf("volume %v encrypto device %v is closed for resizing", volume, mapperPath)
	}

	nsexec, err := newNamespaceExecutor()
	if err != nil {
		return err
	}

	_, err = nsexec.LuksResize(EncryptVolumeName(volume, dataEngine), passphrase, lhtypes.LuksTimeout)
	return err
}

// IsDeviceMappedToNullPath determines if the encrypted device is already open at
// a null path: 'cryptsetup status [crypted_device]' then shows "device:  (null)".
func IsDeviceMappedToNullPath(device string) (bool, error) {
	devPath, mappedFile, err := DeviceEncryptionStatus(device)
	if err != nil {
		return false, err
	}

	return mappedFile != "" && strings.Compare(devPath, "(null)") == 0, nil
}

// IsDeviceOpen determines if the encrypted device is already open.
func IsDeviceOpen(device string) (bool, error) {
	_, mappedFile, err := DeviceEncryptionStatus(device)
	return mappedFile != "", err
}

// IsDeviceEncrypted reports whether the device carries a LUKS header.
func IsDeviceEncrypted(devicePath string) (bool, error) {
	nsexec, err := newNamespaceExecutor()
	if err != nil {
		return false, err
	}
	return nsexec.IsLuks(devicePath, lhtypes.LuksTimeout)
}

// DeviceEncryptionStatus looks to identify if the passed device is a LUKS mapping
// and if so what the device is and the mapper name as used by LUKS.
// If not, just returns the original device and an empty string.
func DeviceEncryptionStatus(devicePath string) (mappedDevice, mapper string, err error) {
	if !strings.HasPrefix(devicePath, MapperFilePathPrefix) {
		return devicePath, "", nil
	}

	nsexec, err := newNamespaceExecutor()
	if err != nil {
		return devicePath, "", err
	}

	// Check the mapper device using `cryptsetup status`. Sample output:
	//   /dev/mapper/pvc-e2a1c50a-9409-4afd-9e7d-3c1f5a9afa7f is active and is in use.
	//     type:    LUKS2
	//     cipher:  aes-xts-plain64
	//     keysize: 256 bits
	//     key location: keyring
	//     device:  /dev/sda
	//     sector size:  512
	//     offset:  32768 sectors
	//     size:    110592 sectors
	//     mode:    read/write
	volume := strings.TrimPrefix(devicePath, MapperFilePathPrefix+"/")
	stdout, err := nsexec.LuksStatus(volume, lhtypes.LuksTimeout)
	if err != nil {
		logrus.WithError(err).Warnf("Device %s is not an active LUKS device", devicePath)
		return devicePath, "", nil
	}

	lines := strings.Split(string(stdout), "\n")
	if len(lines) < 1 {
		return "", "", fmt.Errorf("device encryption status returned no stdout for %s", devicePath)
	}

	// There are two possible cases for an encrypted mapper device:
	// - "/path/to/device is active.": the mapper device is activated by `cryptsetup luksOpen`
	// - "/path/to/device is active and is in use.": the activated device is mounted or exposed (e.g., NFS)
	if !strings.Contains(lines[0], " is active") {
		// Implies this is not a LUKS device
		return devicePath, "", nil
	}

	for i := 1; i < len(lines); i++ {
		kv := strings.SplitN(strings.TrimSpace(lines[i]), ":", 2)
		if len(kv) < 1 {
			return "", "", fmt.Errorf("device encryption status output for %s is badly formatted: %s",
				devicePath, lines[i])
		}
		if strings.Compare(kv[0], "device") == 0 {
			return strings.TrimSpace(kv[1]), volume, nil
		}
	}
	// Identified as LUKS, but failed to identify a mapped device
	return "", "", fmt.Errorf("mapped device not found in path %s", devicePath)
}
