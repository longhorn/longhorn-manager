package cert

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"testing"
	"time"

	"github.com/rancher/dynamiclistener/factory"

	"k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/longhorn/longhorn-manager/types"
)

const testNS = "longhorn-system"

func TestEnsureWebhookSecrets_FreshCluster(t *testing.T) {
	k8s := fake.NewClientset()
	ctx := context.Background()

	if err := EnsureWebhookSecrets(ctx, k8s, testNS); err != nil {
		t.Fatalf("EnsureWebhookSecrets: %v", err)
	}

	caSec, err := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CaName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get CA: %v", err)
	}
	leafSec, err := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CertName, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("get leaf: %v", err)
	}

	if got := caSec.Annotations[factory.Static]; got != "true" {
		t.Errorf("CA static annotation = %q, want \"true\"", got)
	}
	if got := leafSec.Annotations[factory.Static]; got != "true" {
		t.Errorf("leaf static annotation = %q, want \"true\"", got)
	}
	if caSec.Type != corev1.SecretTypeTLS {
		t.Errorf("CA secret type = %q, want %q", caSec.Type, corev1.SecretTypeTLS)
	}

	caCert, err := parseCertSecret(caSec)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	leafCert, err := parseCertSecret(leafSec)
	if err != nil {
		t.Fatalf("parse leaf: %v", err)
	}
	if err := verifyChain(leafCert, caCert); err != nil {
		t.Errorf("chain verify: %v", err)
	}
	if want := leafSANs(testNS); !certHasAllSANs(leafCert, want) {
		t.Errorf("leaf SANs = %v, want all of %v", leafCert.DNSNames, want)
	}

	wantLeafValidity := time.Duration(leafValidityDays) * 24 * time.Hour
	if got := leafCert.NotAfter.Sub(leafCert.NotBefore); got < wantLeafValidity-time.Hour || got > wantLeafValidity+time.Hour {
		t.Errorf("leaf validity = %s, want ~%s", got, wantLeafValidity)
	}
	wantCAValidity := time.Duration(caValidityDays) * 24 * time.Hour
	if got := caCert.NotAfter.Sub(caCert.NotBefore); got < wantCAValidity-time.Hour || got > wantCAValidity+time.Hour {
		t.Errorf("CA validity = %s, want ~%s", got, wantCAValidity)
	}
}

// TestEnsureWebhookSecrets_HappyPathOnlyAddsAnnotation models the
// running-cluster scenario where dynamiclistener has already created the
// secrets without our annotation; we must keep the cert material as-is and
// only add the static annotation.
func TestEnsureWebhookSecrets_HappyPathOnlyAddsAnnotation(t *testing.T) {
	k8s := fake.NewClientset()
	ctx := context.Background()

	if err := EnsureWebhookSecrets(ctx, k8s, testNS); err != nil {
		t.Fatalf("first EnsureWebhookSecrets: %v", err)
	}

	for _, name := range []string{types.CaName, types.CertName} {
		s, err := k8s.CoreV1().Secrets(testNS).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("get %s: %v", name, err)
		}
		delete(s.Annotations, factory.Static)
		if _, err := k8s.CoreV1().Secrets(testNS).Update(ctx, s, metav1.UpdateOptions{}); err != nil {
			t.Fatalf("update %s: %v", name, err)
		}
	}

	caBefore, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CaName, metav1.GetOptions{})
	leafBefore, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CertName, metav1.GetOptions{})

	if err := EnsureWebhookSecrets(ctx, k8s, testNS); err != nil {
		t.Fatalf("second EnsureWebhookSecrets: %v", err)
	}

	caAfter, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CaName, metav1.GetOptions{})
	leafAfter, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CertName, metav1.GetOptions{})

	if !bytes.Equal(caBefore.Data[corev1.TLSCertKey], caAfter.Data[corev1.TLSCertKey]) {
		t.Error("CA cert was rewritten on annotation-only path")
	}
	if !bytes.Equal(leafBefore.Data[corev1.TLSCertKey], leafAfter.Data[corev1.TLSCertKey]) {
		t.Error("leaf cert was rewritten on annotation-only path")
	}
	if !factory.IsStatic(caAfter) {
		t.Error("CA static annotation missing after Phase 2")
	}
	if !factory.IsStatic(leafAfter) {
		t.Error("leaf static annotation missing after Phase 2")
	}
}

func TestEnsureWebhookSecrets_RotatesNearingExpiry(t *testing.T) {
	k8s := fake.NewClientset()
	ctx := context.Background()

	// Plant secrets whose NotAfter is within renewWithin so the next
	// EnsureWebhookSecrets call must regenerate, not just annotate.
	shortValidity := 10 * 24 * time.Hour
	caCert, caKey, err := generateCA(shortValidity)
	if err != nil {
		t.Fatalf("generate near-expiry CA: %v", err)
	}
	leafCert, leafKey, err := generateLeaf(caCert, caKey, leafSANs(testNS), shortValidity)
	if err != nil {
		t.Fatalf("generate near-expiry leaf: %v", err)
	}
	mustCreateTLSSecret(t, k8s, ctx, types.CaName, caCert, caKey)
	mustCreateTLSSecret(t, k8s, ctx, types.CertName, leafCert, leafKey)

	caBefore, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CaName, metav1.GetOptions{})
	leafBefore, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CertName, metav1.GetOptions{})

	if err := EnsureWebhookSecrets(ctx, k8s, testNS); err != nil {
		t.Fatalf("EnsureWebhookSecrets: %v", err)
	}

	caAfter, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CaName, metav1.GetOptions{})
	leafAfter, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CertName, metav1.GetOptions{})

	if bytes.Equal(caBefore.Data[corev1.TLSCertKey], caAfter.Data[corev1.TLSCertKey]) {
		t.Error("CA was NOT rotated despite being near expiry")
	}
	if bytes.Equal(leafBefore.Data[corev1.TLSCertKey], leafAfter.Data[corev1.TLSCertKey]) {
		t.Error("leaf was NOT rotated despite being near expiry")
	}

	caAfterParsed, err := parseCertSecret(caAfter)
	if err != nil {
		t.Fatalf("parse rotated CA: %v", err)
	}
	leafAfterParsed, err := parseCertSecret(leafAfter)
	if err != nil {
		t.Fatalf("parse rotated leaf: %v", err)
	}
	if err := verifyChain(leafAfterParsed, caAfterParsed); err != nil {
		t.Errorf("rotated chain verify: %v", err)
	}
	// The freshly-rotated certs must be on the full validity schedule again,
	// not the 10-day schedule of the seed.
	if got := leafAfterParsed.NotAfter.Sub(leafAfterParsed.NotBefore); got < shortValidity*2 {
		t.Errorf("rotated leaf validity = %s, want > %s", got, shortValidity*2)
	}
}

func TestEnsureWebhookSecrets_LeafReissuedOnChainMismatch(t *testing.T) {
	// CA and leaf both exist and individually parse, but the leaf is signed
	// by a different CA. The healthy CA Secret must be preserved (touching
	// it would force a non-atomic apiserver caBundle/leaf swap); only the
	// leaf is re-issued, signed by the current CA, which heals the chain.
	k8s := fake.NewClientset()
	ctx := context.Background()
	caValidity := time.Duration(caValidityDays) * 24 * time.Hour
	leafValidity := time.Duration(leafValidityDays) * 24 * time.Hour

	caA, caAKey, err := generateCA(caValidity)
	if err != nil {
		t.Fatalf("generate CA A: %v", err)
	}
	caB, caBKey, err := generateCA(caValidity)
	if err != nil {
		t.Fatalf("generate CA B: %v", err)
	}
	mismatchedLeaf, mismatchedKey, err := generateLeaf(caA, caAKey, leafSANs(testNS), leafValidity)
	if err != nil {
		t.Fatalf("generate mismatched leaf: %v", err)
	}

	mustCreateTLSSecret(t, k8s, ctx, types.CaName, caB, caBKey)
	mustCreateTLSSecret(t, k8s, ctx, types.CertName, mismatchedLeaf, mismatchedKey)

	caBefore, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CaName, metav1.GetOptions{})
	leafBefore, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CertName, metav1.GetOptions{})

	if err := EnsureWebhookSecrets(ctx, k8s, testNS); err != nil {
		t.Fatalf("EnsureWebhookSecrets: %v", err)
	}

	caAfter, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CaName, metav1.GetOptions{})
	leafAfter, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CertName, metav1.GetOptions{})

	if !bytes.Equal(caBefore.Data[corev1.TLSCertKey], caAfter.Data[corev1.TLSCertKey]) {
		t.Error("CA was rotated; healthy CA must be preserved on chain mismatch")
	}
	if bytes.Equal(leafBefore.Data[corev1.TLSCertKey], leafAfter.Data[corev1.TLSCertKey]) {
		t.Error("leaf was NOT re-issued despite chain mismatch")
	}

	caAfterParsed, err := parseCertSecret(caAfter)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	leafAfterParsed, err := parseCertSecret(leafAfter)
	if err != nil {
		t.Fatalf("parse rotated leaf: %v", err)
	}
	if err := verifyChain(leafAfterParsed, caAfterParsed); err != nil {
		t.Errorf("rotated chain verify: %v", err)
	}
}

func TestEnsureWebhookSecrets_LeafReissuedOnMissingSAN(t *testing.T) {
	k8s := fake.NewClientset()
	ctx := context.Background()
	caValidity := time.Duration(caValidityDays) * 24 * time.Hour
	leafValidity := time.Duration(leafValidityDays) * 24 * time.Hour

	caCert, caKey, err := generateCA(caValidity)
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}
	wrongSAN := []string{"some.other.svc"}
	leafCert, leafKey, err := generateLeaf(caCert, caKey, wrongSAN, leafValidity)
	if err != nil {
		t.Fatalf("generate wrong-SAN leaf: %v", err)
	}
	mustCreateTLSSecret(t, k8s, ctx, types.CaName, caCert, caKey)
	mustCreateTLSSecret(t, k8s, ctx, types.CertName, leafCert, leafKey)

	caBefore, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CaName, metav1.GetOptions{})
	leafBefore, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CertName, metav1.GetOptions{})

	if err := EnsureWebhookSecrets(ctx, k8s, testNS); err != nil {
		t.Fatalf("EnsureWebhookSecrets: %v", err)
	}

	caAfter, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CaName, metav1.GetOptions{})
	leafAfter, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CertName, metav1.GetOptions{})
	if !bytes.Equal(caBefore.Data[corev1.TLSCertKey], caAfter.Data[corev1.TLSCertKey]) {
		t.Error("CA was rotated; healthy CA must be preserved on SAN mismatch")
	}
	if bytes.Equal(leafBefore.Data[corev1.TLSCertKey], leafAfter.Data[corev1.TLSCertKey]) {
		t.Error("leaf was NOT re-issued despite SAN mismatch")
	}
	parsed, err := parseCertSecret(leafAfter)
	if err != nil {
		t.Fatalf("parse rotated leaf: %v", err)
	}
	if want := leafSANs(testNS); !certHasAllSANs(parsed, want) {
		t.Errorf("rotated leaf SANs = %v, want all of %v", parsed.DNSNames, want)
	}
}

// TestEnsureWebhookSecrets_LeafOnlyRotation_NearExpiry covers the steady-state
// yearly rotation: CA is long-lived and healthy, leaf is within renewWithin.
// We must re-sign a new leaf with the existing CA and leave the CA Secret
// byte-identical so apiserver's caBundle never changes.
func TestEnsureWebhookSecrets_LeafOnlyRotation_NearExpiry(t *testing.T) {
	k8s := fake.NewClientset()
	ctx := context.Background()

	caCert, caKey, err := generateCA(time.Duration(caValidityDays) * 24 * time.Hour)
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}
	// Leaf valid but inside the renewWithin window.
	shortLeafValidity := 10 * 24 * time.Hour
	leafCert, leafKey, err := generateLeaf(caCert, caKey, leafSANs(testNS), shortLeafValidity)
	if err != nil {
		t.Fatalf("generate near-expiry leaf: %v", err)
	}
	mustCreateTLSSecret(t, k8s, ctx, types.CaName, caCert, caKey)
	mustCreateTLSSecret(t, k8s, ctx, types.CertName, leafCert, leafKey)

	caBefore, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CaName, metav1.GetOptions{})
	leafBefore, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CertName, metav1.GetOptions{})

	if err := EnsureWebhookSecrets(ctx, k8s, testNS); err != nil {
		t.Fatalf("EnsureWebhookSecrets: %v", err)
	}

	caAfter, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CaName, metav1.GetOptions{})
	leafAfter, _ := k8s.CoreV1().Secrets(testNS).Get(ctx, types.CertName, metav1.GetOptions{})

	if !bytes.Equal(caBefore.Data[corev1.TLSCertKey], caAfter.Data[corev1.TLSCertKey]) {
		t.Error("CA cert was rotated; must stay byte-identical when only leaf is near expiry")
	}
	if !bytes.Equal(caBefore.Data[corev1.TLSPrivateKeyKey], caAfter.Data[corev1.TLSPrivateKeyKey]) {
		t.Error("CA key was rotated; must stay byte-identical when only leaf is near expiry")
	}
	if bytes.Equal(leafBefore.Data[corev1.TLSCertKey], leafAfter.Data[corev1.TLSCertKey]) {
		t.Error("leaf was NOT rotated despite being near expiry")
	}

	// New leaf must chain to the unchanged CA and have full validity again.
	caParsed, err := parseCertSecret(caAfter)
	if err != nil {
		t.Fatalf("parse CA: %v", err)
	}
	leafParsed, err := parseCertSecret(leafAfter)
	if err != nil {
		t.Fatalf("parse rotated leaf: %v", err)
	}
	if err := verifyChain(leafParsed, caParsed); err != nil {
		t.Errorf("rotated chain verify: %v", err)
	}
	wantLeafValidity := time.Duration(leafValidityDays) * 24 * time.Hour
	if got := leafParsed.NotAfter.Sub(leafParsed.NotBefore); got < wantLeafValidity-time.Hour {
		t.Errorf("rotated leaf validity = %s, want ~%s", got, wantLeafValidity)
	}
}

func mustCreateTLSSecret(t *testing.T, k8s *fake.Clientset, ctx context.Context, name string, cert *x509.Certificate, key *rsa.PrivateKey) {
	t.Helper()
	certPEM, keyPEM, err := encodePEM(cert, key)
	if err != nil {
		t.Fatalf("encodePEM: %v", err)
	}
	_, err = k8s.CoreV1().Secrets(testNS).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNS},
		Type:       corev1.SecretTypeTLS,
		Data: map[string][]byte{
			corev1.TLSCertKey:       certPEM,
			corev1.TLSPrivateKeyKey: keyPEM,
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
}
