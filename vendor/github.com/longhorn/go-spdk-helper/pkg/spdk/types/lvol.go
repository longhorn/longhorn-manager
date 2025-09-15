package types

import (
	"fmt"
	"strings"
)

type BdevDriverSpecificLvol struct {
	LvolStoreUUID        string            `json:"lvol_store_uuid"`
	BaseBdev             string            `json:"base_bdev"`
	BaseSnapshot         string            `json:"base_snapshot,omitempty"`
	ThinProvision        bool              `json:"thin_provision"`
	NumAllocatedClusters uint64            `json:"num_allocated_clusters"`
	Snapshot             bool              `json:"snapshot"`
	Clone                bool              `json:"clone"`
	Clones               []string          `json:"clones,omitempty"`
	Xattrs               map[string]string `json:"xattrs,omitempty"`
}

type LvstoreInfo struct {
	UUID              string `json:"uuid"`
	Name              string `json:"name"`
	BaseBdev          string `json:"base_bdev"`
	TotalDataClusters uint64 `json:"total_data_clusters"`
	FreeClusters      uint64 `json:"free_clusters"`
	BlockSize         uint64 `json:"block_size"`
	ClusterSize       uint64 `json:"cluster_size"`
}

type LvolInfo struct {
	Alias             string `json:"alias"`
	UUID              string `json:"uuid"`
	Name              string `json:"name"`
	IsThinProvisioned bool   `json:"is_thin_provisioned"`
	IsSnapshot        bool   `json:"is_snapshot"`
	IsClone           bool   `json:"is_clone"`
	IsEsnapClone      bool   `json:"is_esnap_clone"`
	IsDegraded        bool   `json:"is_degraded"`
	Lvs               struct {
		Name string `json:"name"`
		UUID string `json:"uuid"`
	} `json:"lvs"`
}

type ShallowCopy struct {
	OperationId uint32 `json:"operation_id"`
}

type ShallowCopyStatus struct {
	State            string `json:"state"`
	CopiedClusters   uint64 `json:"copied_clusters"`
	UnmappedClusters uint64 `json:"unmapped_clusters,omitempty"`
	TotalClusters    uint64 `json:"total_clusters"`
	Error            string `json:"error,omitempty"`
}

type DeepCopy struct {
	OperationId uint32 `json:"operation_id"`
}

type DeepCopyStatus struct {
	State             string `json:"state"`
	ProcessedClusters uint64 `json:"processed_clusters"`
	TotalClusters     uint64 `json:"total_clusters"`
	Error             string `json:"error,omitempty"`
}

type BdevLvolCreateLvstoreRequest struct {
	BdevName string `json:"bdev_name"`
	LvsName  string `json:"lvs_name"`

	ClusterSz uint32 `json:"cluster_sz,omitempty"`
	// ClearMethod               string `json:"clear_method,omitempty"`
	// NumMdPagesPerClusterRatio uint32 `json:"num_md_pages_per_cluster_ratio,omitempty"`
}

type BdevLvolDeleteLvstoreRequest struct {
	UUID    string `json:"uuid,omitempty"`
	LvsName string `json:"lvs_name,omitempty"`
}

type BdevLvolRenameLvstoreRequest struct {
	OldName string `json:"old_name"`
	NewName string `json:"new_name"`
}

type BdevLvolSetXattrRequest struct {
	Name       string `json:"name"`
	XattrName  string `json:"xattr_name"`
	XattrValue string `json:"xattr_value"`
}

type BdevLvolGetXattrRequest struct {
	Name      string `json:"name"`
	XattrName string `json:"xattr_name"`
}

type BdevLvolGetLvstoreRequest struct {
	UUID    string `json:"uuid,omitempty"`
	LvsName string `json:"lvs_name,omitempty"`
}

type BdevLvolClearMethod string

const (
	BdevLvolClearMethodNone        = "none"
	BdevLvolClearMethodUnmap       = "unmap"
	BdevLvolClearMethodWriteZeroes = "write_zeroes"
)

type BdevLvolCreateRequest struct {
	LvsName       string              `json:"lvs_name,omitempty"`
	UUID          string              `json:"uuid,omitempty"`
	LvolName      string              `json:"lvol_name"`
	SizeInMib     uint64              `json:"size_in_mib"`
	ClearMethod   BdevLvolClearMethod `json:"clear_method,omitempty"`
	ThinProvision bool                `json:"thin_provision,omitempty"`
}

type BdevLvolDeleteRequest struct {
	Name string `json:"name"`
}

type BdevLvolSnapshotRequest struct {
	LvolName     string            `json:"lvol_name"`
	SnapshotName string            `json:"snapshot_name"`
	Xattrs       map[string]string `json:"xattrs,omitempty"`
}

type BdevLvolCloneRequest struct {
	SnapshotName string `json:"snapshot_name"`
	CloneName    string `json:"clone_name"`
}

type BdevLvolCloneBdevRequest struct {
	Bdev      string `json:"bdev"`
	LvsName   string `json:"lvs_name"`
	CloneName string `json:"clone_name"`
}

type BdevLvolDecoupleParentRequest struct {
	Name string `json:"name"`
}

type BdevLvolDetachParentRequest struct {
	Name string `json:"name"`
}

type BdevLvolSetParentRequest struct {
	LvolName   string `json:"lvol_name"`
	ParentName string `json:"parent_name"`
}

type BdevLvolResizeRequest struct {
	Name      string `json:"name"`
	SizeInMib uint64 `json:"size_in_mib"`
}

type BdevLvolShallowCopyRequest struct {
	SrcLvolName string `json:"src_lvol_name"`
	DstBdevName string `json:"dst_bdev_name"`
}

type BdevLvolRangeShallowCopyRequest struct {
	SrcLvolName string   `json:"src_lvol_name"`
	DstBdevName string   `json:"dst_bdev_name"`
	Clusters    []uint64 `json:"clusters"`
}

type BdevLvolDeepCopyRequest struct {
	SrcLvolName string `json:"src_lvol_name"`
	DstBdevName string `json:"dst_bdev_name"`
}

type BdevLvolGetFragmapRequest struct {
	Name   string `json:"name"`
	Offset uint64 `json:"offset"`
	Size   uint64 `json:"size"`
}

type BdevLvolRenameRequest struct {
	OldName string `json:"old_name"`
	NewName string `json:"new_name"`
}

type BdevLvolRegisterSnapshotChecksumRequest struct {
	Name string `json:"name"`
}

type BdevLvolRegisterRangeChecksumsRequest struct {
	Name string `json:"name"`
}

type BdevLvolGetSnapshotChecksumRequest struct {
	Name string `json:"name"`
}

type BdevLvolGetRangeChecksumsRequest struct {
	Name              string `json:"name"`
	ClusterStartIndex uint64 `json:"cluster_start_index"`
	ClusterCount      uint64 `json:"cluster_count"`
}

type BdevLvolSnapshotChecksum struct {
	Checksum uint64 `json:"checksum"`
}

type BdevLvolRangeChecksum struct {
	ClusterIndex uint64 `json:"cluster_index"`
	Checksum     uint64 `json:"checksum"`
}

type BdevLvolStopSnapshotChecksumRequest struct {
	Name string `json:"name"`
}

func GetLvolAlias(lvsName, lvolName string) string {
	return fmt.Sprintf("%s/%s", lvsName, lvolName)
}

func GetLvsNameFromAlias(alias string) string {
	splitRes := strings.Split(alias, "/")
	if len(splitRes) != 2 {
		return ""
	}
	return splitRes[0]
}

func GetLvolNameFromAlias(alias string) string {
	splitRes := strings.Split(alias, "/")
	if len(splitRes) != 2 {
		return ""
	}
	return splitRes[1]
}
