package controller

import "testing"

// TestSanitizeBackupStoreErrorMessage verifies that volatile per-request
// identifiers (S3 request IDs, HTTP-status+request-ID pairs) are stripped
// from backup store error messages so that repeated identical failures
// produce identical Status.Conditions[].Message values.
//
// This matters because reconcile() only calls UpdateBackupTargetStatus when
// backupTarget.Status changed (via reflect.DeepEqual); if every failed
// attempt has a distinct message, the controller storms the remote backup
// target instead of respecting Spec.PollInterval.
//
// Regression test for https://github.com/longhorn/longhorn/issues/1547
func TestSanitizeBackupStoreErrorMessage(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name: "s3 status code and hex request id",
			input: "failed to list objects with param: {\n  Bucket: \"mybucket\",\n  Delimiter: \"/\",\n  Prefix: \"/\"\n} " +
				"error: AWS Error:  InvalidAccessKeyId Malformed Access Key Id <nil>\n403 1eed0c50c2cb9133\n",
			expected: "failed to list objects with param: {\n  Bucket: \"mybucket\",\n  Delimiter: \"/\",\n  Prefix: \"/\"\n} " +
				"error: AWS Error:  InvalidAccessKeyId Malformed Access Key Id <nil>\n<redacted>\n",
		},
		{
			name:     "explicit RequestId field",
			input:    "AWS Error: SlowDown Please reduce your request rate. RequestId: 7f159e1736fb70263abc123",
			expected: "AWS Error: SlowDown Please reduce your request rate. <redacted>",
		},
		{
			name:     "no volatile content",
			input:    "failed to init backup target clients: credential secret not found",
			expected: "failed to init backup target clients: credential secret not found",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := sanitizeBackupStoreErrorMessage(tt.input)
			if got != tt.expected {
				t.Fatalf("sanitizeBackupStoreErrorMessage(%q):\n  got:      %q\n  expected: %q", tt.input, got, tt.expected)
			}
		})
	}
}

// TestSanitizeBackupStoreErrorMessageStableAcrossAttempts verifies the core
// property that motivated this fix: two error messages that only differ by
// their embedded request ID sanitize to the same string.
func TestSanitizeBackupStoreErrorMessageStableAcrossAttempts(t *testing.T) {
	attempt1 := "AWS Error:  InvalidAccessKeyId Malformed Access Key Id <nil>\n403 1eed0c50c2cb9133\n"
	attempt2 := "AWS Error:  InvalidAccessKeyId Malformed Access Key Id <nil>\n403 9e4e1d90d4c091bc\n"

	got1 := sanitizeBackupStoreErrorMessage(attempt1)
	got2 := sanitizeBackupStoreErrorMessage(attempt2)

	if got1 != got2 {
		t.Fatalf("expected sanitized messages to be equal across attempts, got %q vs %q", got1, got2)
	}
}
