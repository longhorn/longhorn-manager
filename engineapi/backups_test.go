package engineapi

import (
	"testing"

	"github.com/stretchr/testify/require"
)

const oneBackupText = `
{
	"Name": "backup-072d7a718f854328",
	"URL": "vfs:///var/lib/longhorn/backups/default?backup=backup-072d7a718f854328\u0026volume=qq",
	"SnapshotName": "volume-snap-snap4.img",
	"SnapshotCreated": "2017-03-25T02:26:59Z",
	"Created": "2017-03-25T02:27:00Z",
	"Size": "169869312",
	"VolumeName": "qq",
	"VolumeSize": "10737418240",
	"VolumeCreated": "2017-03-25T02:25:53Z",
	"VolumeBackingImageName": "",
	"VolumeBackingImageURL":  ""
}
`

const backupsListText = `
{
	"qq": {
		"Name": "qq",
		"Size": "10737418240",
		"Created": "2017-03-25T02:25:53Z",
		"LastBackupName": "backup-072d7a718f854328",
		"LastBackupAt": "2017-03-25T02:26:59Z",
		"BackingImageName": "",
		"BackingImageURL":  "",
		"DataStored": "41943040",
		"Backups": {
			"vfs:///var/lib/longhorn/backups/default?backup=backup-072d7a718f854328\u0026volume=qq": {
				"Name": "backup-072d7a718f854328",
				"URL": "vfs:///var/lib/longhorn/backups/default?backup=backup-072d7a718f854328\u0026volume=qq",
				"SnapshotName": "volume-snap-snap4.img",
				"SnapshotCreated": "2017-03-25T02:26:59Z",
				"Created": "2017-03-25T02:27:00Z",
				"Size": "169869312"
			},
			"vfs:///var/lib/longhorn/backups/default?backup=backup-3ddb6c6a09424a05\u0026volume=qq": {
				"Name": "backup-3ddb6c6a09424a05",
				"URL": "vfs:///var/lib/longhorn/backups/default?backup=backup-3ddb6c6a09424a05\u0026volume=qq",
				"SnapshotName": "volume-snap-snap1.img",
				"SnapshotCreated": "2017-03-25T02:25:53Z",
				"Created": "2017-03-25T02:25:54Z",
				"Size": "167772160"
			}
		}
	}
}
`

const backupVolumesListText = `
{
	"pvc-1": {
		"Name": "pvc-1",
		"Size": "1073741824",
		"Labels":{
			"KubernetesStatus": "{\"pvName\":\"pvc-1\",\"namespace\":\"default\",\"pvcName\":\"longhorn-1-volv-pvc\",\"workloadsStatus\":[{\"podName\":\"volume-test-2\",\"workloadType\":\"\"}]}",
			"foo": "bar"
		},
		"Created": "2017-03-25T02:26:59Z",
		"LastBackupName": "backup-1",
		"LastBackupAt": "2017-03-25T02:27:00Z",
		"DataStored": "1163919360"
	}
}
`

func TestParseOneBackup(t *testing.T) {
	assert := require.New(t)

	b, err := parseOneBackup(oneBackupText)
	assert.Nil(err)
	assert.Equal(Backup{
		Name:                   "backup-072d7a718f854328",
		URL:                    "vfs:///var/lib/longhorn/backups/default?backup=backup-072d7a718f854328\u0026volume=qq",
		SnapshotName:           "volume-snap-snap4.img",
		SnapshotCreated:        "2017-03-25T02:26:59Z",
		Created:                "2017-03-25T02:27:00Z",
		Size:                   "169869312",
		VolumeName:             "qq",
		VolumeSize:             "10737418240",
		VolumeCreated:          "2017-03-25T02:25:53Z",
		VolumeBackingImageName: "",
		VolumeBackingImageURL:  "",
	}, *b)
}

func TestParseBackupsList(t *testing.T) {
	assert := require.New(t)

	bs, err := parseBackupsList(backupsListText, "qq")
	assert.Nil(err)
	assert.Equal(2, len(bs))

	snapshots := map[string]struct{}{}
	for _, b := range bs {
		snapshots[b.SnapshotName] = struct{}{}
	}
	assert.NotNil(snapshots["volume-snap-snap1.img"])
	assert.NotNil(snapshots["volume-snap-snap4.img"])
}

func TestParseBackupVolumesList(t *testing.T) {
	assert := require.New(t)

	bvl, err := parseBackupVolumesList(backupVolumesListText)
	assert.Nil(err)
	assert.Equal(map[string]*BackupVolume{
		"pvc-1": {
			Name: "pvc-1",
			Size: "1073741824",
			Labels: map[string]string{
				"KubernetesStatus": "{\"pvName\":\"pvc-1\",\"namespace\":\"default\",\"pvcName\":\"longhorn-1-volv-pvc\",\"workloadsStatus\":[{\"podName\":\"volume-test-2\",\"workloadType\":\"\"}]}",
				"foo":              "bar",
			},
			Created:        "2017-03-25T02:26:59Z",
			LastBackupName: "backup-1",
			LastBackupAt:   "2017-03-25T02:27:00Z",
			DataStored:     "1163919360",
		},
	}, bvl)
}
