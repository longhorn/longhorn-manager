package keystore

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	lhtypes "github.com/longhorn/go-common-libs/types"
)

const (
	vaultRequestTimeout = 30 * time.Second
	// vaultTokenTTLFraction is how much of the token TTL is used before logging in again.
	vaultTokenTTLFraction = 0.8
	// vaultPassphraseField is the KV data field that holds the passphrase.
	vaultPassphraseField = "passphrase"
	// vaultCASMismatch is the error text Vault returns when options.cas does not match.
	vaultCASMismatch = "check-and-set parameter did not match"
)

type cachedToken struct {
	token  string
	expiry time.Time
}

// tokenCache keeps one client token per Vault login target for the life of
// the process, so successive attaches do not log in again.
var (
	tokenCacheMu sync.Mutex
	tokenCache   = map[string]*cachedToken{}
)

type vaultProvider struct {
	cfg    *vaultConfig
	client *http.Client
	log    *logrus.Entry
}

func newVaultProvider(cfg *vaultConfig) (*vaultProvider, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, ServerName: cfg.TLSServerName}
	if cfg.CAPool != nil {
		tlsConfig.RootCAs = cfg.CAPool
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsConfig
	// Reachability must not depend on a pooled connection surviving an outage.
	transport.DisableKeepAlives = true
	return &vaultProvider{
		cfg:    cfg,
		client: &http.Client{Transport: transport, Timeout: vaultRequestTimeout},
		log:    logrus.WithFields(logrus.Fields{"component": "keystore", "provider": "vault", "vaultAddr": cfg.Addr}),
	}, nil
}

func (p *vaultProvider) Name() lhtypes.EncryptionKeyProvider {
	return lhtypes.EncryptionKeyProviderVault
}

type vaultError struct {
	status int
	errs   []string
}

func (e *vaultError) Error() string {
	return fmt.Sprintf("vault responded %d: %s", e.status, strings.Join(e.errs, "; "))
}

type vaultResponse struct {
	Data json.RawMessage `json:"data"`
	Auth *struct {
		ClientToken   string `json:"client_token"`
		LeaseDuration int    `json:"lease_duration"`
	} `json:"auth"`
	Errors []string `json:"errors"`
}

// request performs one HTTP call without any login handling. A 404 is returned
// as a vaultError with status 404 and no body decoding is attempted.
func (p *vaultProvider) request(ctx context.Context, method, reqPath, token string, body interface{}) (*vaultResponse, error) {
	var reader io.Reader
	if body != nil {
		buf, err := json.Marshal(body)
		if err != nil {
			return nil, err
		}
		reader = bytes.NewReader(buf)
	}
	req, err := http.NewRequestWithContext(ctx, method, p.cfg.Addr+"/"+reqPath, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Vault-Request", "true")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("X-Vault-Token", token)
	}
	if p.cfg.Namespace != "" {
		req.Header.Set("X-Vault-Namespace", p.cfg.Namespace)
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return nil, errors.Wrapf(ErrUnavailable, "%v %v: %v", method, reqPath, err)
	}
	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, errors.Wrapf(ErrUnavailable, "%v %v: reading response: %v", method, reqPath, err)
	}

	out := &vaultResponse{}
	if len(bytes.TrimSpace(raw)) > 0 {
		if err := json.Unmarshal(raw, out); err != nil && resp.StatusCode < 300 {
			return nil, errors.Wrapf(err, "%v %v: unexpected response", method, reqPath)
		}
	}
	if resp.StatusCode >= 500 {
		return nil, errors.Wrapf(ErrUnavailable, "%v %v: %v", method, reqPath, (&vaultError{status: resp.StatusCode, errs: out.Errors}).Error())
	}
	if resp.StatusCode >= 300 {
		return nil, &vaultError{status: resp.StatusCode, errs: out.Errors}
	}
	return out, nil
}

// tokenFilePath is TokenFile everywhere but in tests, which cannot write to it.
var tokenFilePath = TokenFile

func (p *vaultProvider) readTokenFile() (string, error) {
	raw, err := os.ReadFile(tokenFilePath)
	if err != nil {
		return "", errors.Wrapf(err, "the projected ServiceAccount token for Vault is not readable at %v", tokenFilePath)
	}
	jwt := strings.TrimSpace(string(raw))
	if jwt == "" {
		return "", fmt.Errorf("the projected ServiceAccount token for Vault at %v is empty", tokenFilePath)
	}
	return jwt, nil
}

// login posts the projected ServiceAccount token to the kubernetes auth mount.
// Vault validates it through the cluster's TokenReview API, presenting the same
// token as the credential for the review, so no per-cluster reviewer secret exists.
func (p *vaultProvider) login(ctx context.Context) (string, error) {
	jwt, err := p.readTokenFile()
	if err != nil {
		return "", err
	}
	resp, err := p.request(ctx, http.MethodPost, path.Join("v1", p.cfg.AuthPath, "login"), "",
		map[string]string{"role": p.cfg.Role, "jwt": jwt})
	if err != nil {
		return "", errors.Wrapf(err, "vault login at %v with role %v failed", p.cfg.AuthPath, p.cfg.Role)
	}
	if resp.Auth == nil || resp.Auth.ClientToken == "" {
		return "", fmt.Errorf("vault login at %v returned no client token", p.cfg.AuthPath)
	}
	ttl := time.Duration(float64(resp.Auth.LeaseDuration)*vaultTokenTTLFraction) * time.Second
	if ttl <= 0 {
		ttl = time.Minute
	}
	tokenCacheMu.Lock()
	tokenCache[p.cfg.loginKey()] = &cachedToken{token: resp.Auth.ClientToken, expiry: time.Now().Add(ttl)}
	tokenCacheMu.Unlock()
	p.log.Infof("Logged in to Vault at %v with role %v, token valid for %v", p.cfg.AuthPath, p.cfg.Role, ttl)
	return resp.Auth.ClientToken, nil
}

func (p *vaultProvider) token(ctx context.Context) (string, error) {
	tokenCacheMu.Lock()
	cached, ok := tokenCache[p.cfg.loginKey()]
	tokenCacheMu.Unlock()
	if ok && time.Now().Before(cached.expiry) {
		return cached.token, nil
	}
	return p.login(ctx)
}

func (p *vaultProvider) invalidateToken() {
	tokenCacheMu.Lock()
	delete(tokenCache, p.cfg.loginKey())
	tokenCacheMu.Unlock()
}

// call is request with a cached token and one re-login on 403.
func (p *vaultProvider) call(ctx context.Context, method, reqPath string, body interface{}) (*vaultResponse, error) {
	token, err := p.token(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := p.request(ctx, method, reqPath, token, body)
	var verr *vaultError
	if errors.As(err, &verr) && verr.status == http.StatusForbidden {
		p.log.Info("Vault rejected the cached token, logging in again")
		p.invalidateToken()
		if token, err = p.login(ctx); err != nil {
			return nil, err
		}
		resp, err = p.request(ctx, method, reqPath, token, body)
	}
	return resp, err
}

type kvReadData struct {
	Data     map[string]string `json:"data"`
	Metadata struct {
		Version      int    `json:"version"`
		Destroyed    bool   `json:"destroyed"`
		DeletionTime string `json:"deletion_time"`
	} `json:"metadata"`
}

// read fetches one entry, the current version when version is 0 and that exact
// version otherwise, and reports a soft-deleted or destroyed entry as missing.
func (p *vaultProvider) read(ctx context.Context, name string, version int) (passphrase []byte, got int, err error) {
	reqPath := p.cfg.dataPath(name)
	if version > 0 {
		reqPath += "?version=" + strconv.Itoa(version)
	}
	resp, err := p.call(ctx, http.MethodGet, reqPath, nil)
	var verr *vaultError
	if errors.As(err, &verr) && verr.status == http.StatusNotFound {
		return nil, 0, ErrKeyNotFound
	}
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to read the key of %v", name)
	}
	var kv kvReadData
	if err := json.Unmarshal(resp.Data, &kv); err != nil {
		return nil, 0, errors.Wrapf(err, "unexpected key entry for %v", name)
	}
	if kv.Metadata.Destroyed || kv.Metadata.DeletionTime != "" || kv.Data == nil {
		return nil, 0, ErrKeyNotFound
	}
	value, ok := kv.Data[vaultPassphraseField]
	if !ok || value == "" {
		return nil, 0, fmt.Errorf("the key entry of %v has no %v field", name, vaultPassphraseField)
	}
	return []byte(value), kv.Metadata.Version, nil
}

func (p *vaultProvider) Get(ctx context.Context, name string) ([]byte, int, error) {
	return p.read(ctx, name, 0)
}

// GetVersion reads one specific version of an entry. A derived volume uses it
// to read the key that was current when its source snapshot was taken.
func (p *vaultProvider) GetVersion(ctx context.Context, name string, version int) ([]byte, error) {
	if version <= 0 {
		return nil, fmt.Errorf("key version %v of %v is not a stored version", version, name)
	}
	passphrase, _, err := p.read(ctx, name, version)
	if errors.Is(err, ErrKeyNotFound) {
		return nil, errors.Wrapf(err, "version %v of the key of %v is gone", version, name)
	}
	return passphrase, err
}

func (p *vaultProvider) Put(ctx context.Context, name string, passphrase []byte, cas int) (int, error) {
	body := map[string]interface{}{
		"options": map[string]int{"cas": cas},
		"data":    map[string]string{vaultPassphraseField: string(passphrase)},
	}
	resp, err := p.call(ctx, http.MethodPost, p.cfg.dataPath(name), body)
	var verr *vaultError
	if errors.As(err, &verr) && verr.status == http.StatusBadRequest {
		for _, msg := range verr.errs {
			if strings.Contains(msg, vaultCASMismatch) {
				return 0, ErrKeyConflict
			}
		}
	}
	if err != nil {
		return 0, errors.Wrapf(err, "failed to store the key of %v", name)
	}
	var meta struct {
		Version int `json:"version"`
	}
	if err := json.Unmarshal(resp.Data, &meta); err != nil {
		return 0, errors.Wrapf(err, "unexpected response storing the key of %v", name)
	}
	return meta.Version, nil
}

// Delete removes the key. With destroyKeys every version and the metadata go;
// otherwise the current version is soft-deleted and stays recoverable.
func (p *vaultProvider) Delete(ctx context.Context, name string) error {
	reqPath := p.cfg.dataPath(name)
	if p.cfg.DestroyKeys {
		reqPath = p.cfg.metadataPath(name)
	}
	_, err := p.call(ctx, http.MethodDelete, reqPath, nil)
	var verr *vaultError
	if errors.As(err, &verr) && verr.status == http.StatusNotFound {
		return nil
	}
	return errors.Wrapf(err, "failed to delete the key of %v", name)
}
