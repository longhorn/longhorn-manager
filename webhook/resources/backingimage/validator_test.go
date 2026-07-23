package backingimage

import (
	"testing"

	"github.com/stretchr/testify/assert"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

func TestValidateDownloadParameters(t *testing.T) {
	v := &backingImageValidator{}

	cases := []struct {
		name      string
		url       string
		expectErr bool
	}{
		{"empty URL", "", true},
		{"file scheme", "file:///etc/passwd", true},
		{"ftp scheme", "ftp://example.com/image.img", true},
		{"relative no-scheme", "//example.com/image.img", true},
		{"bare hostname", "example.com/image.img", true},
		{"loopback IPv4 127.0.0.1", "http://127.0.0.1/metadata", true},
		{"loopback IPv4 127.0.0.2", "http://127.0.0.2/metadata", true},
		{"loopback 0.0.0.0", "http://0.0.0.0/metadata", true},
		{"loopback hostname localhost", "http://localhost/metadata", true},
		{"loopback IPv6 ::1", "http://[::1]/path", true},
		{"valid http", "http://example.com/image.img", false},
		{"valid https", "https://example.com/image.img", false},
		{"valid https with port", "https://example.com:8080/image.img", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bi := &longhorn.BackingImage{
				Spec: longhorn.BackingImageSpec{
					SourceType: longhorn.BackingImageDataSourceTypeDownload,
					SourceParameters: map[string]string{
						longhorn.DataSourceTypeDownloadParameterURL: tc.url,
					},
				},
			}
			err := v.validateDownloadParameters(bi)
			if tc.expectErr {
				assert.Error(t, err, "expected error for URL %q", tc.url)
			} else {
				assert.NoError(t, err, "expected no error for URL %q", tc.url)
			}
		})
	}
}
