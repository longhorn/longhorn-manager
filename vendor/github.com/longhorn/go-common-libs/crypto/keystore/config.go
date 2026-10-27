package keystore

import (
	"crypto/x509"
	"fmt"
	"net/url"
	"path"
	"strconv"
	"strings"

	lhtypes "github.com/longhorn/go-common-libs/types"
)

// TokenFile is the projected ServiceAccount token the vault provider logs in
// with. Every component mounts it at this path with the audience "vault".
const TokenFile = "/var/run/secrets/longhorn.io/vault/token"

// TokenAudience is the audience of that token. Vault binds it in the role, so
// the default API-server-audience token of a pod cannot be replayed against Vault.
const TokenAudience = "vault"

// Fields of the encryption Secret the vault provider reads. None of them is a
// passphrase: they say where the store is and how to authenticate to it.
const (
	VaultAddr          = "VAULT_ADDR"
	VaultAuthPath      = "VAULT_AUTH_PATH"
	VaultRole          = "VAULT_ROLE"
	VaultKVMount       = "VAULT_KV_MOUNT"
	VaultKVPrefix      = "VAULT_KV_PREFIX"
	VaultNamespace     = "VAULT_NAMESPACE"
	VaultCACert        = "VAULT_CA_CERT"
	VaultTLSServerName = "VAULT_TLS_SERVER_NAME"
	VaultDestroyKeys   = "VAULT_DESTROY_KEYS"
)

const (
	defaultKVMount  = "secret"
	defaultKVPrefix = "longhorn/"
	// volumeKeyDir and backupKeyDir divide the prefix. A volume name cannot
	// contain a slash, so neither namespace can reach into the other.
	volumeKeyDir = "volumes"
	backupKeyDir = "backups"
)

// vaultConfig is the vault half of an encryption Secret, after defaults.
type vaultConfig struct {
	Addr          string
	AuthPath      string
	Role          string
	KVMount       string
	KVPrefix      string
	Namespace     string
	CAPool        *x509.CertPool
	TLSServerName string
	DestroyKeys   bool
}

// parseVaultConfig validates the vault fields of an encryption Secret without
// contacting Vault, which is what lets admission reject a misconfigured Secret
// before a volume exists rather than at a mount on some node. It refuses a
// passphrase next to the vault provider, so a stale CRYPTO_KEY_VALUE can never
// be mistaken for the key of a volume.
func parseVaultConfig(secrets map[string]string) (*vaultConfig, error) {
	if len(secrets[lhtypes.CryptoKeyValue]) > 0 {
		return nil, fmt.Errorf("%v must not be set when %v is %v",
			lhtypes.CryptoKeyValue, lhtypes.CryptoKeyProvider, lhtypes.EncryptionKeyProviderVault)
	}

	cfg := &vaultConfig{
		Addr:          strings.TrimRight(strings.TrimSpace(secrets[VaultAddr]), "/"),
		AuthPath:      strings.Trim(strings.TrimSpace(secrets[VaultAuthPath]), "/"),
		Role:          strings.TrimSpace(secrets[VaultRole]),
		KVMount:       strings.Trim(strings.TrimSpace(secrets[VaultKVMount]), "/"),
		KVPrefix:      strings.TrimLeft(strings.TrimSpace(secrets[VaultKVPrefix]), "/"),
		Namespace:     strings.TrimSpace(secrets[VaultNamespace]),
		TLSServerName: strings.TrimSpace(secrets[VaultTLSServerName]),
		DestroyKeys:   true,
	}

	if cfg.Addr == "" {
		return nil, fmt.Errorf("missing %v in the encryption secret", VaultAddr)
	}
	u, err := url.Parse(cfg.Addr)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("%v %q is not an http(s) URL", VaultAddr, cfg.Addr)
	}

	if cfg.AuthPath == "" {
		return nil, fmt.Errorf("missing %v in the encryption secret", VaultAuthPath)
	}
	if !strings.HasPrefix(cfg.AuthPath, "auth/") {
		cfg.AuthPath = "auth/" + cfg.AuthPath
	}
	if cfg.Role == "" {
		return nil, fmt.Errorf("missing %v in the encryption secret", VaultRole)
	}
	if cfg.KVMount == "" {
		cfg.KVMount = defaultKVMount
	}
	if _, ok := secrets[VaultKVPrefix]; !ok {
		cfg.KVPrefix = defaultKVPrefix
	} else if cfg.KVPrefix != "" && !strings.HasSuffix(cfg.KVPrefix, "/") {
		cfg.KVPrefix += "/"
	}

	if pem := strings.TrimSpace(secrets[VaultCACert]); pem != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(pem)) {
			return nil, fmt.Errorf("%v does not contain a PEM certificate", VaultCACert)
		}
		cfg.CAPool = pool
	}

	if v, ok := secrets[VaultDestroyKeys]; ok && strings.TrimSpace(v) != "" {
		destroy, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return nil, fmt.Errorf("%v must be a boolean, got %q", VaultDestroyKeys, v)
		}
		cfg.DestroyKeys = destroy
	}

	return cfg, nil
}

// loginKey identifies a Vault login target: two configs with the same one
// share a client token.
func (c *vaultConfig) loginKey() string {
	return strings.Join([]string{c.Addr, c.Namespace, c.AuthPath, c.Role}, "\x00")
}

func (c *vaultConfig) dataPath(name string) string {
	return path.Join("v1", c.KVMount, "data", c.KVPrefix+name)
}

func (c *vaultConfig) metadataPath(name string) string {
	return path.Join("v1", c.KVMount, "metadata", c.KVPrefix+name)
}

// VolumeKey is the key name of a volume's passphrase.
func VolumeKey(volume string) string {
	return volumeKeyDir + "/" + volume
}

// BackupKey is the key name of a backup's passphrase. A backup owns an entry
// because it outlives the volume it was taken from: deleting the volume must
// not destroy the key to its backups.
func BackupKey(backup string) string {
	return backupKeyDir + "/" + backup
}
