// Package cert bootstraps and rotates the longhorn-webhook-{ca,tls} secrets.
//
// At scale, dynamiclistener's per-pod writers race on the secret and produce a
// sustained 409 storm on kube-apiserver. Marking the secret with
// listener.cattle.io/static="true" makes dynamiclistener treat it as read-only
// on every write path; this package then owns rotation (see rotate.go).
//
// Rotation reuses the existing CA when it is still healthy so that only the
// leaf Secret is updated. Updating the CA Secret triggers a non-atomic
// caBundle/leaf swap in apiserver webhook configs that can drop handshakes
// for a few seconds; the per-year leaf renewal path avoids it entirely.
// CA + leaf are regenerated together only when the CA itself is unparsable
// or near expiry.
package cert

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/rancher/dynamiclistener/factory"
	"github.com/sirupsen/logrus"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/retry"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
)

const (
	// Match dynamiclistener defaults: long-lived CA, yearly leaf. A long CA
	// validity is what lets the steady-state rotation path touch only the
	// leaf Secret.
	caValidityDays   = 365 * 10
	leafValidityDays = 365
	renewWithin      = 90 * 24 * time.Hour
	rsaKeyBits       = 2048

	// Backdate NotBefore so freshly-issued certs survive minor clock skew.
	clockSkewSlack = 5 * time.Minute
)

// EnsureWebhookSecrets brings the webhook Secrets to a healthy, static-annotated
// state. Three-way decision:
//
//   - CA + leaf healthy: annotate-only.
//   - CA healthy, leaf not: reuse the existing CA and re-issue the leaf only.
//   - otherwise: regenerate CA + leaf together.
//
// Idempotent; safe to call on every leader-elected rotation tick.
func EnsureWebhookSecrets(ctx context.Context, k8s kubernetes.Interface, namespace string) error {
	caSec, err := getSecret(ctx, k8s, namespace, types.CaName)
	if err != nil {
		return errors.Wrapf(err, "failed to get secret %s/%s", namespace, types.CaName)
	}
	leafSec, err := getSecret(ctx, k8s, namespace, types.CertName)
	if err != nil {
		return errors.Wrapf(err, "failed to get secret %s/%s", namespace, types.CertName)
	}

	caCert, caKey, caHealthy := loadHealthyCA(caSec)
	leafHealthy := caHealthy && leafIsHealthy(leafSec, caCert, namespace)

	switch {
	case caHealthy && leafHealthy:
		if err := ensureStaticAnnotation(ctx, k8s, namespace, caSec); err != nil {
			return errors.Wrapf(err, "failed to annotate secret %s/%s", namespace, types.CaName)
		}
		if err := ensureStaticAnnotation(ctx, k8s, namespace, leafSec); err != nil {
			return errors.Wrapf(err, "failed to annotate secret %s/%s", namespace, types.CertName)
		}
		return nil

	case caHealthy && !leafHealthy:
		logrus.Infof("Rotating webhook leaf only (reusing existing CA) for namespace %s", namespace)
		leafValidity := time.Duration(leafValidityDays) * 24 * time.Hour
		leafCert, leafKey, err := generateLeaf(caCert, caKey, leafSANs(namespace), leafValidity)
		if err != nil {
			return errors.Wrap(err, "failed to generate webhook leaf cert")
		}
		if err := upsertTLSSecret(ctx, k8s, namespace, types.CertName, leafCert, leafKey); err != nil {
			return errors.Wrapf(err, "failed to upsert secret %s/%s", namespace, types.CertName)
		}
		logrus.Infof("Applied new webhook leaf (validity %dd) to namespace %s", leafValidityDays, namespace)
		return nil

	default:
		logrus.Infof("Regenerating webhook CA and leaf for namespace %s", namespace)
		newCACert, newCAKey, err := generateCA(time.Duration(caValidityDays) * 24 * time.Hour)
		if err != nil {
			return errors.Wrap(err, "failed to generate webhook CA")
		}
		leafCert, leafKey, err := generateLeaf(newCACert, newCAKey, leafSANs(namespace), time.Duration(leafValidityDays)*24*time.Hour)
		if err != nil {
			return errors.Wrap(err, "failed to generate webhook leaf cert")
		}
		// CA first so OnChange(CaName) handlers can fan out the new caBundle
		// before the leaf swap.
		if err := upsertTLSSecret(ctx, k8s, namespace, types.CaName, newCACert, newCAKey); err != nil {
			return errors.Wrapf(err, "failed to upsert secret %s/%s", namespace, types.CaName)
		}
		if err := upsertTLSSecret(ctx, k8s, namespace, types.CertName, leafCert, leafKey); err != nil {
			return errors.Wrapf(err, "failed to upsert secret %s/%s", namespace, types.CertName)
		}
		logrus.Infof("Applied new webhook CA (validity %dd) and leaf (validity %dd) to namespace %s",
			caValidityDays, leafValidityDays, namespace)
		return nil
	}
}

// leafSANs covers both webhook services so either listener can terminate TLS
// without dynamiclistener attempting an AddCN at runtime (IsStatic blocks it).
func leafSANs(namespace string) []string {
	return []string{
		fmt.Sprintf("%s.%s.svc", types.AdmissionWebhookServiceName, namespace),
		fmt.Sprintf("%s.%s.svc", types.ConversionWebhookServiceName, namespace),
	}
}

func getSecret(ctx context.Context, k8s kubernetes.Interface, namespace, name string) (*corev1.Secret, error) {
	s, err := k8s.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return nil, nil
	}
	return s, err
}

// loadHealthyCA returns (cert, key, true) when the CA Secret parses and is
// not nearing expiry. Any failure path returns ok=false so the caller falls
// back to full CA + leaf regeneration.
func loadHealthyCA(caSec *corev1.Secret) (*x509.Certificate, *rsa.PrivateKey, bool) {
	if caSec == nil {
		return nil, nil, false
	}
	caCert, caKey, err := parseCASecret(caSec)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to parse webhook CA secret %s/%s", caSec.Namespace, caSec.Name)
		return nil, nil, false
	}
	if caCert.NotAfter.Before(time.Now().Add(renewWithin)) {
		logrus.Infof("Rotating webhook CA, expires at %s (within %s renewal window)", caCert.NotAfter, renewWithin)
		return nil, nil, false
	}
	return caCert, caKey, true
}

// leafIsHealthy reports whether the leaf Secret chains to ca, has all required
// SANs, and is not near expiry. Caller must have already verified ca is healthy.
func leafIsHealthy(leafSec *corev1.Secret, ca *x509.Certificate, namespace string) bool {
	if leafSec == nil {
		return false
	}
	leafCert, err := parseCertSecret(leafSec)
	if err != nil {
		logrus.WithError(err).Warnf("Failed to parse webhook leaf secret %s/%s", leafSec.Namespace, leafSec.Name)
		return false
	}
	if err := verifyChain(leafCert, ca); err != nil {
		logrus.WithError(err).Warn("Failed to verify webhook cert chain")
		return false
	}
	want := leafSANs(namespace)
	if !certHasAllSANs(leafCert, want) {
		logrus.Warnf("Webhook leaf SANs not all present, want %v, have %v", want, leafCert.DNSNames)
		return false
	}
	if leafCert.NotAfter.Before(time.Now().Add(renewWithin)) {
		logrus.Infof("Rotating webhook leaf, expires at %s (within %s renewal window)", leafCert.NotAfter, renewWithin)
		return false
	}
	return true
}

func parseCertSecret(s *corev1.Secret) (*x509.Certificate, error) {
	if s == nil {
		return nil, errors.New("nil secret")
	}
	pemBytes := s.Data[corev1.TLSCertKey]
	if len(pemBytes) == 0 {
		return nil, errors.New("empty tls.crt")
	}
	block, _ := pem.Decode(pemBytes)
	if block == nil {
		return nil, errors.New("no PEM block in tls.crt")
	}
	return x509.ParseCertificate(block.Bytes)
}

// parseCASecret loads both the CA cert and its RSA private key. Reusing the
// existing key is what makes leaf-only rotation possible without invalidating
// the apiserver-cached caBundle.
func parseCASecret(s *corev1.Secret) (*x509.Certificate, *rsa.PrivateKey, error) {
	cert, err := parseCertSecret(s)
	if err != nil {
		return nil, nil, err
	}
	keyPEM := s.Data[corev1.TLSPrivateKeyKey]
	if len(keyPEM) == 0 {
		return nil, nil, errors.New("empty tls.key")
	}
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return nil, nil, errors.New("no PEM block in tls.key")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, nil, errors.Wrap(err, "parse CA private key")
	}
	rsaKey, ok := key.(*rsa.PrivateKey)
	if !ok {
		return nil, nil, errors.New("CA private key is not RSA")
	}
	return cert, rsaKey, nil
}

func verifyChain(leaf, ca *x509.Certificate) error {
	pool := x509.NewCertPool()
	pool.AddCert(ca)
	_, err := leaf.Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	})
	return err
}

func certHasSAN(c *x509.Certificate, want string) bool {
	for _, dns := range c.DNSNames {
		if dns == want {
			return true
		}
	}
	return false
}

func certHasAllSANs(c *x509.Certificate, want []string) bool {
	for _, w := range want {
		if !certHasSAN(c, w) {
			return false
		}
	}
	return true
}

func generateCA(validity time.Duration) (*x509.Certificate, *rsa.PrivateKey, error) {
	key, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return nil, nil, err
	}
	serial, err := randSerial()
	if err != nil {
		return nil, nil, err
	}
	notBefore := time.Now().Add(-clockSkewSlack)
	tmpl := x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: types.CaName},
		NotBefore:             notBefore,
		NotAfter:              notBefore.Add(validity),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func generateLeaf(caCert *x509.Certificate, caKey *rsa.PrivateKey, sans []string, validity time.Duration) (*x509.Certificate, *rsa.PrivateKey, error) {
	key, err := rsa.GenerateKey(rand.Reader, rsaKeyBits)
	if err != nil {
		return nil, nil, err
	}
	serial, err := randSerial()
	if err != nil {
		return nil, nil, err
	}
	notBefore := time.Now().Add(-clockSkewSlack)
	tmpl := x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: sans[0]},
		DNSNames:     sans,
		NotBefore:    notBefore,
		NotAfter:     notBefore.Add(validity),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, &tmpl, caCert, &key.PublicKey, caKey)
	if err != nil {
		return nil, nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, err
	}
	return cert, key, nil
}

func randSerial() (*big.Int, error) {
	return rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
}

func encodePEM(cert *x509.Certificate, key *rsa.PrivateKey) ([]byte, []byte, error) {
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
	keyDER, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})
	return certPEM, keyPEM, nil
}

// upsertTLSSecret creates the Secret or updates it in place with the given
// material and the static annotation, preserving other annotations and labels.
func upsertTLSSecret(ctx context.Context, k8s kubernetes.Interface, namespace, name string, cert *x509.Certificate, key *rsa.PrivateKey) error {
	certPEM, keyPEM, err := encodePEM(cert, key)
	if err != nil {
		return err
	}

	desired := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Annotations: map[string]string{
				factory.Static: "true",
			},
		},
		Type: corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}
	if _, err := k8s.CoreV1().Secrets(namespace).Create(ctx, desired, metav1.CreateOptions{}); err == nil {
		return nil
	} else if !apierrors.IsAlreadyExists(err) {
		return err
	}

	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, err := k8s.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		updated := current.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = map[string]string{}
		}
		updated.Annotations[factory.Static] = "true"
		updated.Type = corev1.SecretTypeTLS
		if updated.Data == nil {
			updated.Data = map[string][]byte{}
		}
		updated.Data[corev1.TLSCertKey] = certPEM
		updated.Data[corev1.TLSPrivateKeyKey] = keyPEM
		_, err = k8s.CoreV1().Secrets(namespace).Update(ctx, updated, metav1.UpdateOptions{})
		return err
	})
}

// ensureStaticAnnotation patches the static annotation onto an existing Secret
// without touching cert material.
func ensureStaticAnnotation(ctx context.Context, k8s kubernetes.Interface, namespace string, s *corev1.Secret) error {
	if factory.IsStatic(s) {
		return nil
	}
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		current, err := k8s.CoreV1().Secrets(namespace).Get(ctx, s.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if factory.IsStatic(current) {
			return nil
		}
		updated := current.DeepCopy()
		if updated.Annotations == nil {
			updated.Annotations = map[string]string{}
		}
		updated.Annotations[factory.Static] = "true"
		_, err = k8s.CoreV1().Secrets(namespace).Update(ctx, updated, metav1.UpdateOptions{})
		if err == nil {
			logrus.Infof("Marked webhook secret %s/%s as static (preserving cert material)", namespace, s.Name)
		}
		return err
	})
}
