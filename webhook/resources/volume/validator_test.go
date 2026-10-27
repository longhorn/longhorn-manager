package volume

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"k8s.io/client-go/kubernetes/fake"

	apiextensionsfake "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset/fake"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/types"
	"github.com/longhorn/longhorn-manager/util"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
	lhfake "github.com/longhorn/longhorn-manager/k8s/pkg/client/clientset/versioned/fake"
	werror "github.com/longhorn/longhorn-manager/webhook/error"
)

const tib = int64(1) << 40

func TestCheckECExpansionCeiling(t *testing.T) {
	testCases := []struct {
		name         string
		creationSize int64
		newSize      int64
		wantErr      bool
	}{
		{
			name:         "unknown creation size fails open",
			creationSize: 0,
			newSize:      100 * tib,
			wantErr:      false,
		},
		{
			name:         "exactly 10x is allowed",
			creationSize: tib,
			newSize:      10 * tib,
			wantErr:      false,
		},
		{
			name:         "one byte past 10x is rejected",
			creationSize: tib,
			newSize:      10*tib + 1,
			wantErr:      true,
		},
		{
			name:         "below ceiling is allowed",
			creationSize: tib,
			newSize:      2 * tib,
			wantErr:      false,
		},
		{
			name:         "growth past the creation cap is allowed within 10x",
			creationSize: 200 * tib,
			newSize:      2000 * tib, // > EcLvstoreMaxCreationSize; the cap is creation-only
			wantErr:      false,
		},
		{
			name:         "overflow guard rejects absurd creation size",
			creationSize: int64(^uint64(0) >> 1), // math.MaxInt64
			newSize:      tib,
			wantErr:      true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkECExpansionCeiling("test-vol", tc.creationSize, tc.newSize)
			if (err != nil) != tc.wantErr {
				t.Errorf("checkECExpansionCeiling(creationSize=%v, newSize=%v) error = %v, wantErr %v",
					tc.creationSize, tc.newSize, err, tc.wantErr)
			}
		})
	}
}

func TestECCreationSizeCap(t *testing.T) {
	// The cap comes from the vendored EcLvstoreMaxCreationSize: SPDK rejects
	// lvstore creation when the metadata page count exceeds UINT32_MAX, which
	// at the pinned ratio is (MaxUint32 / 10) * 4 MiB, roughly 1.6 PiB. Pin
	// its magnitude to catch a silently changed constant.
	maxCreationSize := int64(spdktypes.EcLvstoreMaxCreationSize)
	const pib = tib << 10
	if maxCreationSize <= pib || maxCreationSize >= 2*pib {
		t.Errorf("EcLvstoreMaxCreationSize = %v, expected within (1 PiB, 2 PiB)", maxCreationSize)
	}

	// The maximum volume size depends on the EC geometry because the cap is
	// checked against the EC bdev usable size, not the raw volume size.
	const k, stripSizeKB = 4, 64
	maxVolumeSize := spdktypes.MaxECVolumeSizeForCreation(k, stripSizeKB)
	if maxVolumeSize >= maxCreationSize {
		t.Errorf("MaxECVolumeSizeForCreation = %v, expected below EcLvstoreMaxCreationSize %v (metadata margin)", maxVolumeSize, maxCreationSize)
	}

	testCases := []struct {
		name       string
		layoutType longhorn.VolumeDataLayoutType
		size       int64
		wantErr    bool
	}{
		{
			name:       "sharded volume at the maximum is allowed",
			layoutType: longhorn.VolumeDataLayoutTypeSharded,
			size:       maxVolumeSize,
			wantErr:    false,
		},
		{
			name:       "sharded volume one byte over the maximum is rejected",
			layoutType: longhorn.VolumeDataLayoutTypeSharded,
			size:       maxVolumeSize + 1,
			wantErr:    true,
		},
		{
			name:       "non-sharded volume over the maximum is allowed",
			layoutType: longhorn.VolumeDataLayoutTypeReplicated,
			size:       maxVolumeSize + 1,
			wantErr:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			volume := &longhorn.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: "test-vol"},
				Spec: longhorn.VolumeSpec{
					Size: tc.size,
					DataLayout: longhorn.VolumeDataLayout{
						Type:        tc.layoutType,
						DataChunks:  k,
						StripSizeKB: stripSizeKB,
					},
				},
			}
			err := checkECCreationSizeCap(volume, tc.size)
			if (err != nil) != tc.wantErr {
				t.Errorf("checkECCreationSizeCap(type=%v, size=%v) error = %v, wantErr %v",
					tc.layoutType, tc.size, err, tc.wantErr)
			}
		})
	}
}

func TestValidateTopologyZonePin(t *testing.T) {
	newVolume := func(terms []longhorn.VolumeTopologyTerm, affinity longhorn.ReplicaZoneSoftAntiAffinity) *longhorn.Volume {
		return &longhorn.Volume{
			Spec: longhorn.VolumeSpec{
				TopologyRequirement:         terms,
				ReplicaZoneSoftAntiAffinity: affinity,
			},
		}
	}
	pinned := []longhorn.VolumeTopologyTerm{{Zone: "zone-1", Region: "region-1"}}

	assert.NoError(t, validateTopologyZonePin(newVolume(pinned, longhorn.ReplicaZoneSoftAntiAffinityEnabled)))

	err := validateTopologyZonePin(newVolume(pinned, longhorn.ReplicaZoneSoftAntiAffinityDisabled))
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "zone anti-affinity cannot be satisfied within one zone")

	// The mutator fills enabled before validation, so ignored on a pinned
	// volume only occurs when something bypasses it; the invariant still
	// rejects it.
	assert.Error(t, validateTopologyZonePin(newVolume(pinned, longhorn.ReplicaZoneSoftAntiAffinityDefault)))

	// Volumes that do not pin a single zone are free to use any value.
	regionOnly := []longhorn.VolumeTopologyTerm{{Region: "region-1"}}
	assert.NoError(t, validateTopologyZonePin(newVolume(regionOnly, longhorn.ReplicaZoneSoftAntiAffinityDisabled)))
	assert.NoError(t, validateTopologyZonePin(newVolume(nil, longhorn.ReplicaZoneSoftAntiAffinityDisabled)))
}

const linkedCloneTestNamespace = "longhorn-system"

// newLinkedCloneTestDataStore returns a datastore whose volume and snapshot
// listers serve the given objects, as if the informers had already delivered them.
func newLinkedCloneTestDataStore(t *testing.T, volumes []*longhorn.Volume, snapshots []*longhorn.Snapshot) *datastore.DataStore {
	kubeClient := fake.NewSimpleClientset()                    // nolint: staticcheck
	lhClient := lhfake.NewSimpleClientset()                    // nolint: staticcheck
	extensionsClient := apiextensionsfake.NewSimpleClientset() // nolint: staticcheck

	informerFactories := util.NewInformerFactories(linkedCloneTestNamespace, kubeClient, lhClient, 0)
	ds := datastore.NewDataStoreForGlobal(linkedCloneTestNamespace, lhClient, kubeClient, extensionsClient, informerFactories)

	volumeIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Volumes().Informer().GetIndexer()
	for _, volume := range volumes {
		require.NoError(t, volumeIndexer.Add(volume))
	}
	snapshotIndexer := informerFactories.LhInformerFactory.Longhorn().V1beta2().Snapshots().Informer().GetIndexer()
	for _, snapshot := range snapshots {
		require.NoError(t, snapshotIndexer.Add(snapshot))
	}
	return ds
}

// TestValidateLinkedCloneSize checks the size a linked clone inherits from its
// source. Restores are skipped: their size comes from the backup, which the mutator
// has already applied, so there is no user input to validate.
func TestValidateLinkedCloneSize(t *testing.T) {
	const (
		srcVolName = "src-vol"
		snapName   = "snap-0"
		gib        = int64(1) << 30
	)

	testCases := []struct {
		name        string
		size        int64
		fromBackup  string
		dataSource  longhorn.VolumeDataSource
		restoreSize int64
		srcSpecSize int64
		wantErr     bool
	}{
		{
			name:        "size matching the source snapshot is accepted",
			size:        2 * gib,
			dataSource:  types.NewVolumeDataSourceTypeSnapshot(srcVolName, snapName),
			restoreSize: 2 * gib,
			srcSpecSize: 2 * gib,
		},
		{
			name:        "size not matching the source snapshot is rejected",
			size:        3 * gib,
			dataSource:  types.NewVolumeDataSourceTypeSnapshot(srcVolName, snapName),
			restoreSize: 2 * gib,
			srcSpecSize: 2 * gib,
			wantErr:     true,
		},
		{
			// The source volume spec.size must not stand in for the snapshot's, so an
			// expanded source cannot make a stale size look valid.
			name:        "unsynced RestoreSize is rejected",
			size:        3 * gib,
			dataSource:  types.NewVolumeDataSourceTypeSnapshot(srcVolName, snapName),
			restoreSize: 0,
			srcSpecSize: 3 * gib,
			wantErr:     true,
		},
		{
			name:        "without a snapshot the source volume size is used",
			size:        2 * gib,
			dataSource:  types.NewVolumeDataSourceTypeVolume(srcVolName),
			srcSpecSize: 2 * gib,
		},
		{
			// Larger than the source: the clone may have been expanded before backup.
			name:        "restore is not checked against the source",
			size:        3 * gib,
			fromBackup:  "s3://backupbucket@us-east-1/?backup=backup-1&volume=clone-vol",
			dataSource:  types.NewVolumeDataSourceTypeSnapshot(srcVolName, snapName),
			restoreSize: 2 * gib,
			srcSpecSize: 2 * gib,
		},
		{
			// Syncing RestoreSize needs a running engine, so a detached source must not
			// block a restore that does not depend on its size.
			name:        "restore with unsynced RestoreSize is accepted",
			size:        3 * gib,
			fromBackup:  "s3://backupbucket@us-east-1/?backup=backup-1&volume=clone-vol",
			dataSource:  types.NewVolumeDataSourceTypeSnapshot(srcVolName, snapName),
			restoreSize: 0,
			srcSpecSize: 2 * gib,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srcVol := &longhorn.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: srcVolName, Namespace: linkedCloneTestNamespace},
				Spec:       longhorn.VolumeSpec{Size: tc.srcSpecSize},
			}
			snap := &longhorn.Snapshot{
				ObjectMeta: metav1.ObjectMeta{Name: snapName, Namespace: linkedCloneTestNamespace},
				Spec:       longhorn.SnapshotSpec{Volume: srcVolName},
				Status:     longhorn.SnapshotStatus{RestoreSize: tc.restoreSize},
			}
			v := &volumeValidator{ds: newLinkedCloneTestDataStore(t,
				[]*longhorn.Volume{srcVol}, []*longhorn.Snapshot{snap})}

			err := v.validateLinkedCloneSize(&longhorn.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: "clone-vol", Namespace: linkedCloneTestNamespace},
				Spec: longhorn.VolumeSpec{
					Size:       tc.size,
					FromBackup: tc.fromBackup,
					DataSource: tc.dataSource,
					CloneMode:  longhorn.CloneModeLinkedClone,
				},
			})
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
		})
	}
}

func TestVerifyVolumeDataSourceEncryption(t *testing.T) {
	const (
		srcVolName  = "src-vol"
		destVolName = "dest-vol"
		snapName    = "snap-0"
	)

	testCases := []struct {
		name          string
		dataSource    longhorn.VolumeDataSource
		srcEncrypted  bool
		destEncrypted bool
		srcSecret     string
		destSecret    string
		wantErr       bool
		wantField     string
	}{
		{
			name:          "plain to plain is allowed",
			dataSource:    types.NewVolumeDataSourceTypeVolume(srcVolName),
			srcEncrypted:  false,
			destEncrypted: false,
			wantErr:       false,
		},
		{
			name:          "encrypted to encrypted is allowed",
			dataSource:    types.NewVolumeDataSourceTypeVolume(srcVolName),
			srcEncrypted:  true,
			destEncrypted: true,
			wantErr:       false,
		},
		{
			name:          "plain to encrypted is rejected",
			dataSource:    types.NewVolumeDataSourceTypeVolume(srcVolName),
			srcEncrypted:  false,
			destEncrypted: true,
			wantErr:       true,
		},
		{
			name:          "encrypted to plain is rejected",
			dataSource:    types.NewVolumeDataSourceTypeVolume(srcVolName),
			srcEncrypted:  true,
			destEncrypted: false,
			wantErr:       true,
		},
		{
			name:          "snapshot source with matching encryption is allowed",
			dataSource:    types.NewVolumeDataSourceTypeSnapshot(srcVolName, snapName),
			srcEncrypted:  true,
			destEncrypted: true,
			wantErr:       false,
		},
		{
			name:          "snapshot source plain to encrypted is rejected",
			dataSource:    types.NewVolumeDataSourceTypeSnapshot(srcVolName, snapName),
			srcEncrypted:  false,
			destEncrypted: true,
			wantErr:       true,
		},
		{
			name:          "vault source to vault with the same secret is allowed",
			dataSource:    types.NewVolumeDataSourceTypeVolume(srcVolName),
			srcEncrypted:  true,
			destEncrypted: true,
			srcSecret:     "vault-a",
			destSecret:    "vault-a",
		},
		{
			name:          "vault source to vault with another secret is allowed",
			dataSource:    types.NewVolumeDataSourceTypeSnapshot(srcVolName, snapName),
			srcEncrypted:  true,
			destEncrypted: true,
			srcSecret:     "vault-a",
			destSecret:    "vault-b",
		},
		{
			name:          "secret source to vault is rejected",
			dataSource:    types.NewVolumeDataSourceTypeSnapshot(srcVolName, snapName),
			srcEncrypted:  true,
			destEncrypted: true,
			destSecret:    "vault-a",
			wantErr:       true,
			wantField:     "spec.encryption.provider",
		},
		{
			name:          "vault source to secret is rejected",
			dataSource:    types.NewVolumeDataSourceTypeVolume(srcVolName),
			srcEncrypted:  true,
			destEncrypted: true,
			srcSecret:     "vault-a",
			wantErr:       true,
			wantField:     "spec.encryption.provider",
		},
		{
			name:          "snapshot source encrypted to plain is rejected",
			dataSource:    types.NewVolumeDataSourceTypeSnapshot(srcVolName, snapName),
			srcEncrypted:  true,
			destEncrypted: false,
			wantErr:       true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srcVol := &longhorn.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: srcVolName, Namespace: linkedCloneTestNamespace},
				Spec: longhorn.VolumeSpec{
					Encrypted:  tc.srcEncrypted,
					Encryption: vaultEncryption(tc.srcSecret),
					DataEngine: longhorn.DataEngineTypeV1,
				},
			}
			destVol := &longhorn.Volume{
				ObjectMeta: metav1.ObjectMeta{Name: destVolName, Namespace: linkedCloneTestNamespace},
				Spec: longhorn.VolumeSpec{
					DataSource: tc.dataSource,
					Encrypted:  tc.destEncrypted,
					Encryption: vaultEncryption(tc.destSecret),
					DataEngine: longhorn.DataEngineTypeV1,
				},
			}
			ds := newLinkedCloneTestDataStore(t, []*longhorn.Volume{srcVol}, nil)

			err := verifyVolumeDataSource(ds, destVol)
			if !tc.wantErr {
				assert.NoError(t, err)
				return
			}
			var admitErr werror.AdmitError
			require.ErrorAs(t, err, &admitErr)
			status := admitErr.AsResult()
			assert.Equal(t, metav1.StatusReasonInvalid, status.Reason)
			require.NotNil(t, status.Details)
			require.Len(t, status.Details.Causes, 1)
			wantField := tc.wantField
			if wantField == "" {
				wantField = "spec.encrypted"
			}
			assert.Equal(t, wantField, status.Details.Causes[0].Field)
		})
	}
}

// vaultEncryption is a vault volume's encryption spec, or the default secret provider for "".
func vaultEncryption(secretName string) longhorn.VolumeEncryptionSpec {
	if secretName == "" {
		return longhorn.VolumeEncryptionSpec{}
	}
	return longhorn.VolumeEncryptionSpec{Provider: longhorn.EncryptionKeyProviderVault, SecretName: secretName}
}

func TestValidateEncryptionUpdate(t *testing.T) {
	encrypted := func(encryption longhorn.VolumeEncryptionSpec) *longhorn.Volume {
		return &longhorn.Volume{Spec: longhorn.VolumeSpec{Encrypted: true, Encryption: encryption}}
	}
	vault := func(secretName string) longhorn.VolumeEncryptionSpec {
		return longhorn.VolumeEncryptionSpec{Provider: longhorn.EncryptionKeyProviderVault, SecretName: secretName}
	}
	secret := longhorn.VolumeEncryptionSpec{Provider: longhorn.EncryptionKeyProviderSecret}

	assert.NoError(t, validateEncryptionUpdate(encrypted(vault("vault-a")), encrypted(vault("vault-a"))))
	assert.NoError(t, validateEncryptionUpdate(encrypted(longhorn.VolumeEncryptionSpec{}), encrypted(secret)),
		"an empty provider is the secret provider")
	assert.Error(t, validateEncryptionUpdate(encrypted(vault("vault-a")), encrypted(longhorn.VolumeEncryptionSpec{})),
		"removing the encryption settings")
	assert.Error(t, validateEncryptionUpdate(encrypted(longhorn.VolumeEncryptionSpec{}), encrypted(vault("vault-a"))),
		"adding a vault provider")
	assert.Error(t, validateEncryptionUpdate(encrypted(vault("vault-a")), encrypted(vault("vault-b"))))
}
