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
		{
			// Regression case: error text captured from an exec'd subprocess's
			// stderr (via backupstore's parseAwsError -> longhorn-manager's
			// errors.Wrapf) contains a literal two-character "\n" escape
			// sequence (backslash + n), not a real newline byte, immediately
			// before the status code. A \b word-boundary assertion before the
			// digits fails here because both 'n' and '4' are word characters,
			// so the regex must not require a boundary on that side.
			// This is the exact message format observed live on a test
			// cluster with invalid B2 credentials.
			name: "literal backslash-n before status code (exec'd subprocess stderr)",
			input: `failed to list system backups in s3://bucket@region/: error listing system backup in s3://bucket@region/: ` +
				`failed to execute: /path/longhorn [/path/longhorn system-backup list s3://bucket@region/], output , stderr ` +
				`time="2026-07-24T16:26:21.852427323Z" level=error msg="Failed to list s3" func="s3.(*BackupStoreDriver).List" file="s3.go:116" ` +
				`error="failed to list objects with param: {\n  Bucket: \"bucket\",\n  Delimiter: \"/\",\n  Prefix: \"/\"\n} ` +
				`error: AWS Error:  InvalidAccessKeyId Malformed Access Key Id <nil>\n403 dcc174e30ac8453c\n" pkg=s3`,
			expected: `failed to list system backups in s3://bucket@region/: error listing system backup in s3://bucket@region/: ` +
				`failed to execute: /path/longhorn [/path/longhorn system-backup list s3://bucket@region/], output , stderr ` +
				`time="2026-07-24T16:26:21.852427323Z" level=error msg="Failed to list s3" func="s3.(*BackupStoreDriver).List" file="s3.go:116" ` +
				`error="failed to list objects with param: {\n  Bucket: \"bucket\",\n  Delimiter: \"/\",\n  Prefix: \"/\"\n} ` +
				`error: AWS Error:  InvalidAccessKeyId Malformed Access Key Id <nil>\n<redacted>\n" pkg=s3`,
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

// TestSanitizeBackupStoreErrorMessageStableAcrossAttemptsWithSubprocessLogs
// is a regression test for a live-cluster finding: the exec'd `longhorn`
// engine binary's own logrus output embeds a fresh RFC3339-nano timestamp
// on every invocation (in addition to the request ID), so two otherwise
// identical failures still produced different Status.Conditions[].Message
// values until timestamps were also normalized. Verified live: before this
// fix, an invalid-credentials backup target sustained ~6 reconciles/second
// indefinitely instead of settling to one attempt per poll interval.
func TestSanitizeBackupStoreErrorMessageStableAcrossAttemptsWithSubprocessLogs(t *testing.T) {
	attempt1 := `failed to list system backups in s3://bucket@region/: error listing system backup in s3://bucket@region/: ` +
		`failed to execute: /path/longhorn [/path/longhorn system-backup list s3://bucket@region/], output , stderr ` +
		`time="2026-07-24T16:31:27.852675962Z" level=error msg="Failed to list s3" func="s3.(*BackupStoreDriver).List" file="s3.go:116" ` +
		`error="failed to list objects with param: {\n  Bucket: \"bucket\",\n  Delimiter: \"/\",\n  Prefix: \"/\"\n} ` +
		"error: AWS Error:  InvalidAccessKeyId Malformed Access Key Id <nil>\\n403 aaaaaaaaaaaaaaaa\\n\" pkg=s3\n" +
		`time="2026-07-24T16:31:27.852799994Z" level=fatal msg="Failed to run list system backup command" ` +
		`func=cmd.SystemBackupCmd.SystemBackupListCmd.func4 file="system_backup.go:72" ` +
		`error="failed to list objects with param: {\n  Bucket: \"bucket\",\n  Delimiter: \"/\",\n  Prefix: \"/\"\n} ` +
		"error: AWS Error:  InvalidAccessKeyId Malformed Access Key Id <nil>\\n403 aaaaaaaaaaaaaaaa\\n\"\n" +
		": exit status 1"

	attempt2 := `failed to list system backups in s3://bucket@region/: error listing system backup in s3://bucket@region/: ` +
		`failed to execute: /path/longhorn [/path/longhorn system-backup list s3://bucket@region/], output , stderr ` +
		`time="2026-07-24T16:31:33.204981123Z" level=error msg="Failed to list s3" func="s3.(*BackupStoreDriver).List" file="s3.go:116" ` +
		`error="failed to list objects with param: {\n  Bucket: \"bucket\",\n  Delimiter: \"/\",\n  Prefix: \"/\"\n} ` +
		"error: AWS Error:  InvalidAccessKeyId Malformed Access Key Id <nil>\\n403 bbbbbbbbbbbbbbbb\\n\" pkg=s3\n" +
		`time="2026-07-24T16:31:33.205102741Z" level=fatal msg="Failed to run list system backup command" ` +
		`func=cmd.SystemBackupCmd.SystemBackupListCmd.func4 file="system_backup.go:72" ` +
		`error="failed to list objects with param: {\n  Bucket: \"bucket\",\n  Delimiter: \"/\",\n  Prefix: \"/\"\n} ` +
		"error: AWS Error:  InvalidAccessKeyId Malformed Access Key Id <nil>\\n403 bbbbbbbbbbbbbbbb\\n\"\n" +
		": exit status 1"

	got1 := sanitizeBackupStoreErrorMessage(attempt1)
	got2 := sanitizeBackupStoreErrorMessage(attempt2)

	if got1 != got2 {
		t.Fatalf("expected sanitized messages to be equal across attempts, got:\n%q\nvs\n%q", got1, got2)
	}
}
