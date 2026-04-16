package backuptarget

import (
	"testing"
)

func TestApplyNFSSoftMountDefaults(t *testing.T) {
	cases := []struct {
		name        string
		input       string
		want        string
		wantChanged bool
	}{
		{
			name:        "empty URL unchanged",
			input:       "",
			want:        "",
			wantChanged: false,
		},
		{
			name:        "s3 URL unchanged",
			input:       "s3://mybucket@us-east-1/",
			want:        "s3://mybucket@us-east-1/",
			wantChanged: false,
		},
		{
			name:        "cifs URL unchanged",
			input:       "cifs://server/share",
			want:        "cifs://server/share",
			wantChanged: false,
		},
		{
			name:        "NFS without nfsOptions left unchanged",
			input:       "nfs://192.168.1.1:/share",
			want:        "nfs://192.168.1.1:/share",
			wantChanged: false,
		},
		{
			name:        "NFS with blank nfsOptions strips the parameter",
			input:       "nfs://192.168.1.1:/share?nfsOptions=",
			want:        "nfs://192.168.1.1:/share",
			wantChanged: true,
		},
		{
			name:        "NFS with multi-value nfsOptions gets flattened and merged",
			input:       "nfs://192.168.1.1:/share?nfsOptions=soft&nfsOptions=timeo%3D600",
			want:        "nfs://192.168.1.1:/share?nfsOptions=soft%2Ctimeo%3D600%2Cretry%3D2",
			wantChanged: true,
		},
		{
			name:        "NFS with rw,nolock gets soft defaults merged in",
			input:       "nfs://192.168.1.1:/share?nfsOptions=rw%2Cnolock",
			want:        "nfs://192.168.1.1:/share?nfsOptions=rw%2Cnolock%2Csoft%2Ctimeo%3D300%2Cretry%3D2",
			wantChanged: true,
		},
		{
			name:        "hard is removed and soft added",
			input:       "nfs://192.168.1.1:/share?nfsOptions=hard%2Crw",
			want:        "nfs://192.168.1.1:/share?nfsOptions=rw%2Csoft%2Ctimeo%3D300%2Cretry%3D2",
			wantChanged: true,
		},
		{
			name:        "custom timeo preserved, soft and retry added",
			input:       "nfs://192.168.1.1:/share?nfsOptions=timeo%3D600",
			want:        "nfs://192.168.1.1:/share?nfsOptions=timeo%3D600%2Csoft%2Cretry%3D2",
			wantChanged: true,
		},
		{
			name:        "custom retry preserved, soft and timeo added",
			input:       "nfs://192.168.1.1:/share?nfsOptions=retry%3D5",
			want:        "nfs://192.168.1.1:/share?nfsOptions=retry%3D5%2Csoft%2Ctimeo%3D300",
			wantChanged: true,
		},
		{
			name:        "all three defaults already present, URL unchanged",
			input:       "nfs://192.168.1.1:/share?nfsOptions=soft%2Ctimeo%3D300%2Cretry%3D2",
			want:        "nfs://192.168.1.1:/share?nfsOptions=soft%2Ctimeo%3D300%2Cretry%3D2",
			wantChanged: false,
		},
		{
			name:        "hard removed when soft already present",
			input:       "nfs://192.168.1.1:/share?nfsOptions=soft%2Chard%2Ctimeo%3D300%2Cretry%3D2",
			want:        "nfs://192.168.1.1:/share?nfsOptions=soft%2Ctimeo%3D300%2Cretry%3D2",
			wantChanged: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, changed, err := applyNFSSoftMountDefaults(tc.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("applyNFSSoftMountDefaults(%q)\n  got:  %q\n  want: %q", tc.input, got, tc.want)
			}
			if changed != tc.wantChanged {
				t.Errorf("applyNFSSoftMountDefaults(%q) changed=%v, want %v", tc.input, changed, tc.wantChanged)
			}
		})
	}
}
