package client

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"

	spdktypes "github.com/longhorn/go-spdk-helper/pkg/spdk/types"
)

type Xattr struct {
	Name  string
	Value string
}

const (
	UserCreated       = "user_created"
	SnapshotTimestamp = "snapshot_timestamp"
	SnapshotChecksum  = "snapshot_checksum"
)

// BdevGetBdevs get information about block devices (bdevs).
//
//	"name": Optional. If this is not specified, the function will list all block devices.
//
//	"timeout": Optional. 0 by default, meaning the method returns immediately whether the bdev exists or not.
func (c *Client) BdevGetBdevs(name string, timeout uint64) (bdevInfoList []spdktypes.BdevInfo, err error) {
	req := spdktypes.BdevGetBdevsRequest{
		Name:    name,
		Timeout: timeout,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_get_bdevs", req)
	if err != nil {
		return nil, err
	}

	return bdevInfoList, json.Unmarshal(cmdOutput, &bdevInfoList)
}

// BdevAioCreate constructs Linux AIO bdev.
func (c *Client) BdevAioCreate(filePath, name string, blockSize uint64) (bdevName string, err error) {
	req := spdktypes.BdevAioCreateRequest{
		Name:      name,
		Filename:  filePath,
		BlockSize: blockSize,
	}

	// Long blob recovery time might be needed if the spdk_tgt is not shutdown gracefully.
	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_aio_create", req)
	if err != nil {
		return "", err
	}

	return bdevName, json.Unmarshal(cmdOutput, &bdevName)
}

// BdevAioDelete deletes Linux AIO bdev.
func (c *Client) BdevAioDelete(name string) (deleted bool, err error) {
	req := spdktypes.BdevAioDeleteRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_aio_delete", req)
	if err != nil {
		return false, err
	}

	return deleted, json.Unmarshal(cmdOutput, &deleted)
}

// BdevAioGet will list all AIO bdevs if a name is not specified.
//
//		"name": Optional. Name of an AIO bdev.
//	         For an AIO bdev, there is no alias nor UUID.
//			 	If this is not specified, the function will list all AIO bdevs.
//
//		"timeout": Optional. 0 by default, meaning the method returns immediately whether the AIO bdev exists or not.
func (c *Client) BdevAioGet(name string, timeout uint64) (bdevAioInfoList []spdktypes.BdevInfo, err error) {
	req := spdktypes.BdevGetBdevsRequest{
		Name:    name,
		Timeout: timeout,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_get_bdevs", req)
	if err != nil {
		return nil, err
	}
	bdevInfoList := []spdktypes.BdevInfo{}
	if err = json.Unmarshal(cmdOutput, &bdevInfoList); err != nil {
		return nil, err
	}

	for _, b := range bdevInfoList {
		if spdktypes.GetBdevType(&b) != spdktypes.BdevTypeAio {
			continue
		}
		bdevAioInfoList = append(bdevAioInfoList, b)
	}

	return bdevAioInfoList, nil
}

// BdevLvolCreateLvstore constructs a logical volume store.
func (c *Client) BdevLvolCreateLvstore(bdevName, lvsName string, clusterSize uint32) (uuid string, err error) {
	req := spdktypes.BdevLvolCreateLvstoreRequest{
		BdevName:  bdevName,
		LvsName:   lvsName,
		ClusterSz: clusterSize,
	}

	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_lvol_create_lvstore", req)
	if err != nil {
		return "", err
	}

	return uuid, json.Unmarshal(cmdOutput, &uuid)
}

// BdevLvolDeleteLvstore destroys a logical volume store. It receives either lvs_name or UUID.
func (c *Client) BdevLvolDeleteLvstore(lvsName, uuid string) (deleted bool, err error) {
	req := spdktypes.BdevLvolDeleteLvstoreRequest{
		LvsName: lvsName,
		UUID:    uuid,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_delete_lvstore", req)
	if err != nil {
		return false, err
	}

	return deleted, json.Unmarshal(cmdOutput, &deleted)
}

// BdevLvolGetLvstore receives either lvs_name or UUID.
func (c *Client) BdevLvolGetLvstore(lvsName, uuid string) (lvstoreInfoList []spdktypes.LvstoreInfo, err error) {
	req := spdktypes.BdevLvolGetLvstoreRequest{
		LvsName: lvsName,
		UUID:    uuid,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_get_lvstores", req)
	if err != nil {
		return nil, err
	}

	return lvstoreInfoList, json.Unmarshal(cmdOutput, &lvstoreInfoList)
}

// BdevLvolGetLvols receives either lvs_name or UUID.
func (c *Client) BdevLvolGetLvols(lvsName, uuid string) (lvolInfoList []spdktypes.LvolInfo, err error) {
	req := spdktypes.BdevLvolGetLvstoreRequest{
		LvsName: lvsName,
		UUID:    uuid,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_get_lvols", req)
	if err != nil {
		return nil, err
	}

	return lvolInfoList, json.Unmarshal(cmdOutput, &lvolInfoList)
}

// BdevLvolRenameLvstore renames a logical volume store.
func (c *Client) BdevLvolRenameLvstore(oldName, newName string) (renamed bool, err error) {
	req := spdktypes.BdevLvolRenameLvstoreRequest{
		OldName: oldName,
		NewName: newName,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_rename_lvstore", req)
	if err != nil {
		return false, err
	}

	return renamed, json.Unmarshal(cmdOutput, &renamed)
}

// BdevLvolCreate create a logical volume on a logical volume store.
//
//	"lvol_name": Required. Name of logical volume to create. The bdev name/alias will be <LVSTORE NAME>/<LVOL NAME>.
//
//	"lvstoreName": Either this or "lvstoreUUID" is required. Name of logical volume store to create logical volume on.
//
//	"lvstoreUUID": Either this or "lvstoreName" is required. UUID of logical volume store to create logical volume on.
//
//	"sizeInMib": Optional. Logical volume size in Mib. And size will be rounded up to a multiple of cluster size.
//
//	"clearMethod": Optional. Change default data clusters clear method. Available: none, unmap, write_zeroes. unmap by default for this API.
//
//	"thinProvision": Optional. True to enable thin provisioning. True by default for this API.
func (c *Client) BdevLvolCreate(lvstoreName, lvstoreUUID, lvolName string, sizeInMib uint64, clearMethod spdktypes.BdevLvolClearMethod, thinProvision bool) (uuid string, err error) {
	if clearMethod == "" {
		clearMethod = spdktypes.BdevLvolClearMethodUnmap
	}
	req := spdktypes.BdevLvolCreateRequest{
		LvsName:       lvstoreName,
		UUID:          lvstoreUUID,
		LvolName:      lvolName,
		SizeInMib:     sizeInMib,
		ClearMethod:   clearMethod,
		ThinProvision: thinProvision,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_create", req)
	if err != nil {
		return "", err
	}

	return uuid, json.Unmarshal(cmdOutput, &uuid)
}

// BdevLvolSetXattr sets extended attribute of a logical volume.
func (c *Client) BdevLvolSetXattr(name, xattrName string, xattrValue string) (set bool, err error) {
	req := spdktypes.BdevLvolSetXattrRequest{
		Name:       name,
		XattrName:  xattrName,
		XattrValue: xattrValue,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_set_xattr", req)
	if err != nil {
		return false, err
	}

	return set, json.Unmarshal(cmdOutput, &set)
}

// BdevLvolGetXattr gets the value of an extended attribute of a logical volume.
func (c *Client) BdevLvolGetXattr(name, xattrName string) (value string, err error) {
	req := spdktypes.BdevLvolGetXattrRequest{
		Name:      name,
		XattrName: xattrName,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_get_xattr", req)
	if err != nil {
		return "", err
	}

	return value, json.Unmarshal(cmdOutput, &value)
}

// BdevLvolDelete destroys a logical volume.
//
//	"name": Required. UUID or alias of the logical volume. The alias of a lvol is <LVSTORE NAME>/<LVOL NAME>.
func (c *Client) BdevLvolDelete(name string) (deleted bool, err error) {
	req := spdktypes.BdevLvolDeleteRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_delete", req)
	if err != nil {
		return false, err
	}

	return deleted, json.Unmarshal(cmdOutput, &deleted)
}

// BdevLvolGetByName gets information about a single lvol bdevs with the specified name.
//
//		"name": Required. UUID or alias of a logical volume (lvol) bdev.
//	        	The alias of a lvol bdev is <LVSTORE NAME>/<LVOL NAME>. And the name of a lvol bdev is UUID.
//
//		"timeout": Optional. 0 by default, meaning the method returns immediately whether the lvol bdev exists or not.
func (c *Client) BdevLvolGetByName(name string, timeout uint64) (bdevLvol spdktypes.BdevInfo, err error) {
	bdevLvolList, err := c.BdevLvolGetWithFilter(name, timeout, func(*spdktypes.BdevInfo) bool { return true })
	if err != nil {
		return spdktypes.BdevInfo{}, err
	}
	if len(bdevLvolList) != 1 {
		return spdktypes.BdevInfo{}, fmt.Errorf("zero or multiple lvols with name %s found", name)
	}
	return bdevLvolList[0], nil
}

// BdevLvolGet gets information about lvol bdevs if a name is not specified.
//
//		"name": Optional. UUID or alias of a logical volume (lvol) bdev.
//	        	The alias of a lvol bdev is <LVSTORE NAME>/<LVOL NAME>. And the name of a lvol bdev is UUID.
//			 	If this is not specified, the function will list all lvol bdevs.
//
//		"timeout": Optional. 0 by default, meaning the method returns immediately whether the lvol bdev exists or not.
func (c *Client) BdevLvolGet(name string, timeout uint64) (bdevLvolInfoList []spdktypes.BdevInfo, err error) {
	return c.BdevLvolGetWithFilter(name, timeout, func(*spdktypes.BdevInfo) bool { return true })
}

// BdevLvolGetWithFilter gets information about some specific lvol bdevs.
//
//		"name": Optional. UUID or alias of a logical volume (lvol) bdev.
//	        	The alias of a lvol bdev is <LVSTORE NAME>/<LVOL NAME>. And the name of a lvol bdev is UUID.
//			 	If this is not specified, the function will list all lvol bdevs.
//
//		"timeout": Optional. 0 by default, meaning the method returns immediately whether the lvol bdev exists or not.
//
//		"filter": Only the lvol bdevs that pass the filter will be returned.
func (c *Client) BdevLvolGetWithFilter(name string, timeout uint64, filter func(*spdktypes.BdevInfo) bool) (bdevLvolInfoList []spdktypes.BdevInfo, err error) {
	req := spdktypes.BdevGetBdevsRequest{
		Name:    name,
		Timeout: timeout,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_get_bdevs", req)
	if err != nil {
		return nil, err
	}
	bdevInfoList := []spdktypes.BdevInfo{}
	if err := json.Unmarshal(cmdOutput, &bdevInfoList); err != nil {
		return nil, err
	}

	bdevLvolInfoList = []spdktypes.BdevInfo{}
	for _, b := range bdevInfoList {
		if spdktypes.GetBdevType(&b) != spdktypes.BdevTypeLvol {
			continue
		}
		if !filter(&b) {
			continue
		}
		b.DriverSpecific.Lvol.Xattrs = make(map[string]string)
		user_created, err := c.BdevLvolGetXattr(b.Name, UserCreated)
		if err == nil {
			b.DriverSpecific.Lvol.Xattrs[UserCreated] = user_created
		} else {
			b.DriverSpecific.Lvol.Xattrs[UserCreated] = strconv.FormatBool(true)
		}
		snapshot_timestamp, err := c.BdevLvolGetXattr(b.Name, SnapshotTimestamp)
		if err == nil {
			b.DriverSpecific.Lvol.Xattrs[SnapshotTimestamp] = snapshot_timestamp
		}
		if b.DriverSpecific.Lvol.Snapshot {
			checksum, err := c.BdevLvolGetSnapshotChecksum(b.Name)
			if err == nil {
				b.DriverSpecific.Lvol.Xattrs[SnapshotChecksum] = checksum
			}
		}

		bdevLvolInfoList = append(bdevLvolInfoList, b)
	}

	return bdevLvolInfoList, nil
}

// BdevLvolSnapshot capture a snapshot of the current state of a logical volume as a new bdev lvol.
//
//	"name": Required. UUID or alias of the logical volume to create a snapshot from. The alias of a lvol is <LVSTORE NAME>/<LVOL NAME>.
//
//	"snapshotName": Required. the logical volume name for the newly created snapshot.
func (c *Client) BdevLvolSnapshot(name, snapshotName string, xattrs []Xattr) (uuid string, err error) {
	req := spdktypes.BdevLvolSnapshotRequest{
		LvolName:     name,
		SnapshotName: snapshotName,
	}

	req.Xattrs = make(map[string]string)
	for _, s := range xattrs {
		req.Xattrs[s.Name] = s.Value
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_snapshot", req)
	if err != nil {
		return "", err
	}

	return uuid, json.Unmarshal(cmdOutput, &uuid)
}

// BdevLvolClone creates a logical volume based on a snapshot.
//
//	"snapshot": Required. UUID or alias of the snapshot lvol to clone. The alias of a lvol is <LVSTORE NAME>/<SNAPSHOT LVOL NAME>.
//
//	"cloneName": Required. the name for the newly created lvol.
func (c *Client) BdevLvolClone(snapshot, cloneName string) (uuid string, err error) {
	req := spdktypes.BdevLvolCloneRequest{
		SnapshotName: snapshot,
		CloneName:    cloneName,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_clone", req)
	if err != nil {
		return "", err
	}

	return uuid, json.Unmarshal(cmdOutput, &uuid)
}

// BdevLvolCloneBdev creates a logical volume based on an external snapshot bdev.
// The external snapshot bdev is a bdev that will not be written to by any consumer and must not be an lvol in the lvstore as the clone.
//
//	"bdev": Required. UUID or name for bdev that acts as the external snapshot.
//
//	"lvsName": Required. logical volume store name of the newly created lvol.
//
//	"cloneName": Required. name for the newly created lvol.
func (c *Client) BdevLvolCloneBdev(bdev, lvsName, cloneName string) (uuid string, err error) {
	req := spdktypes.BdevLvolCloneBdevRequest{
		Bdev:      bdev,
		LvsName:   lvsName,
		CloneName: cloneName,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_clone_bdev", req)
	if err != nil {
		return "", err
	}

	return uuid, json.Unmarshal(cmdOutput, &uuid)
}

// BdevLvolDecoupleParent decouples the parent of a logical volume.
// For unallocated clusters which is allocated in the parent, they are allocated and copied from the parent,
// but for unallocated clusters which is thin provisioned in the parent, they are kept thin provisioned. Then all dependencies on the parent are removed.
//
//	"name": Required. UUID or alias of the logical volume to decouple the parent of it. The alias of a lvol is <LVSTORE NAME>/<LVOL NAME>.
func (c *Client) BdevLvolDecoupleParent(name string) (decoupled bool, err error) {
	req := spdktypes.BdevLvolDecoupleParentRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_lvol_decouple_parent", req)
	if err != nil {
		return false, err
	}

	return decoupled, json.Unmarshal(cmdOutput, &decoupled)
}

// BdevLvolDetachParent detach the parent of a logical volume.
// No new clusters are allocated to the child blob, no data are copied from the parent to the child, so lvol's data are not modified.
// The parent must be a standard snapshot, not an external snapshot. All dependencies on the parent are removed
//
//	"name": Required. UUID or alias of the logical volume to detach the parent of it. The alias of a lvol is <LVSTORE NAME>/<LVOL NAME>.
func (c *Client) BdevLvolDetachParent(name string) (decoupled bool, err error) {
	req := spdktypes.BdevLvolDetachParentRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_lvol_detach_parent", req)
	if err != nil {
		return false, err
	}

	return decoupled, json.Unmarshal(cmdOutput, &decoupled)
}

// BdevLvolSetParent sets a snapshot as the parent of a lvol, making the lvol a clone/child of this snapshot.
// The previous parent of the lvol can be another snapshot or an external snapshot, if the lvol is not a clone must be thin-provisioned.
// Lvol and parent snapshot must have the same size and must belong to the same lvol store.
//
//	"lvol": Required. Alias or UUID for the lvol to set parent of. The alias of a lvol is <LVSTORE NAME>/<LVOL NAME>.
//
//	"parent": Required. Alias or UUID for the snapshot lvol to become the parent.
func (c *Client) BdevLvolSetParent(lvol, parent string) (set bool, err error) {
	req := spdktypes.BdevLvolSetParentRequest{
		LvolName:   lvol,
		ParentName: parent,
	}

	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_lvol_set_parent", req)
	if err != nil {
		return false, err
	}

	return set, json.Unmarshal(cmdOutput, &set)
}

// BdevLvolResize resizes a logical volume.
//
//	"name": Required. UUID or alias of the logical volume to resize.
//
//	"sizeInMib": Required. Desired size of the logical volume in bytes.
func (c *Client) BdevLvolResize(name string, sizeInMib uint64) (resized bool, err error) {
	req := spdktypes.BdevLvolResizeRequest{
		Name:      name,
		SizeInMib: sizeInMib,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_resize", req)
	if err != nil {
		return false, err
	}

	return resized, json.Unmarshal(cmdOutput, &resized)
}

// BdevLvolStartShallowCopy start a shallow copy of lvol over a given bdev.
// Only clusters allocated to the lvol will be written on the bdev.
// Returns the operation ID needed to check the shallow copy status with BdevLvolCheckShallowCopy.
//
//	"srcLvolName": Required. UUID or alias of lvol to create a copy from.
//
//	"dstBdevName": Required. Name of the bdev that acts as destination for the copy.
func (c *Client) BdevLvolStartShallowCopy(srcLvolName, dstBdevName string) (operationId uint32, err error) {
	req := spdktypes.BdevLvolShallowCopyRequest{
		SrcLvolName: srcLvolName,
		DstBdevName: dstBdevName,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_start_shallow_copy", req)
	if err != nil {
		return 0, err
	}

	shallowCopy := spdktypes.ShallowCopy{}
	err = json.Unmarshal(cmdOutput, &shallowCopy)
	if err != nil {
		return 0, err
	}

	return shallowCopy.OperationId, nil
}

// BdevLvolStartRangeShallowCopy start a range shallow copy of lvol over a given bdev.
// For the indexes specified in the array, clusters allocated to the lvol will be written on the bdev,
// for the others an unmap command is sent to the bdev.
// Returns the operation ID needed to check the shallow copy status with BdevLvolCheckShallowCopy.
//
//	"srcLvolName": Required. UUID or alias of lvol to create a copy from.
//
//	"dstBdevName": Required. Name of the bdev that acts as destination for the copy.
//
//	"clusters": Required. Array of clusters indexes to be synchronized with copy or unmap.
func (c *Client) BdevLvolStartRangeShallowCopy(srcLvolName, dstBdevName string, clusters []uint64) (operationId uint32, err error) {
	req := spdktypes.BdevLvolRangeShallowCopyRequest{
		SrcLvolName: srcLvolName,
		DstBdevName: dstBdevName,
		Clusters:    clusters,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_start_range_shallow_copy", req)
	if err != nil {
		return 0, err
	}

	shallowCopy := spdktypes.ShallowCopy{}
	err = json.Unmarshal(cmdOutput, &shallowCopy)
	if err != nil {
		return 0, err
	}

	return shallowCopy.OperationId, nil
}

// BdevLvolCheckShallowCopy check the status of a shallow copy previously started.
// It can be used to check both BdevLvolStartShallowCopy and BdevLvolStartRangeShallowCopy.
//
//	"operationId": Required. Operation ID of the shallow copy to check.
func (c *Client) BdevLvolCheckShallowCopy(operationId uint32) (*spdktypes.ShallowCopyStatus, error) {
	shallowCopy := spdktypes.ShallowCopy{
		OperationId: operationId,
	}
	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_check_shallow_copy", shallowCopy)
	if err != nil {
		return nil, err
	}

	var shallowCopyStatus spdktypes.ShallowCopyStatus
	err = json.Unmarshal(cmdOutput, &shallowCopyStatus)
	if err != nil {
		return nil, err
	}

	return &shallowCopyStatus, nil
}

// BdevLvolStartDeepCopy start a deep copy of lvol over a given bdev.
// Only clusters allocated to the lvol or the lvol's ancestors will be written on the bdev.
// Returns the operation ID needed to check the deep copy status with BdevLvolCheckDeepCopy.
//
//	"srcLvolName": Required. UUID or alias of lvol to create a copy from.
//
//	"dstBdevName": Required. Name of the bdev that acts as destination for the copy.
func (c *Client) BdevLvolStartDeepCopy(srcLvolName, dstBdevName string) (operationId uint32, err error) {
	req := spdktypes.BdevLvolDeepCopyRequest{
		SrcLvolName: srcLvolName,
		DstBdevName: dstBdevName,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_start_deep_copy", req)
	if err != nil {
		return 0, err
	}

	deepCopy := spdktypes.DeepCopy{}
	err = json.Unmarshal(cmdOutput, &deepCopy)
	if err != nil {
		return 0, err
	}

	return deepCopy.OperationId, nil
}

// BdevLvolCheckDeepCopy check the status of a deep copy previously started.
//
//	"operationId": Required. Operation ID of the deep copy to check.
func (c *Client) BdevLvolCheckDeepCopy(operationId uint32) (*spdktypes.DeepCopyStatus, error) {
	deepCopy := spdktypes.DeepCopy{
		OperationId: operationId,
	}
	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_check_deep_copy", deepCopy)
	if err != nil {
		return nil, err
	}

	var deepCopyStatus spdktypes.DeepCopyStatus
	err = json.Unmarshal(cmdOutput, &deepCopyStatus)
	if err != nil {
		return nil, err
	}

	return &deepCopyStatus, nil
}

// BdevLvolGetFragmap gets fragmap of the specific segment of the logical volume.
//
//	"name": Required. UUID or alias of the logical volume.
//
//	"offset": Optional. Offset in bytes of the specific segment of the logical volume (Default: 0).
//
//	"size": Optional. Size in bytes of the specific segment of the logical volume (Default: 0 for representing the entire file).
func (c *Client) BdevLvolGetFragmap(name string, offset, size uint64) (*spdktypes.BdevLvolFragmap, error) {
	req := spdktypes.BdevLvolGetFragmapRequest{
		Name:   name,
		Offset: offset,
		Size:   size,
	}

	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_lvol_get_fragmap", req)
	if err != nil {
		return nil, err
	}

	var result spdktypes.BdevLvolFragmap
	err = json.Unmarshal(cmdOutput, &result)
	if err != nil {
		return nil, err
	}
	return &result, nil
}

// BdevLvolRegisterSnapshotChecksum compute and store checksum of snapshot's data. Overwrite old checksum if already registered.
//
//	"name": Required. UUID or alias of the snapshot. The alias of a snapshot is <LVSTORE NAME>/<SNAPSHOT NAME>.
func (c *Client) BdevLvolRegisterSnapshotChecksum(name string) (registered bool, err error) {
	req := spdktypes.BdevLvolRegisterSnapshotChecksumRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_lvol_register_snapshot_checksum", req)
	if err != nil {
		return false, err
	}

	return registered, json.Unmarshal(cmdOutput, &registered)
}

// BdevLvolGetSnapshotChecksum gets snapshot's stored checksum. The checksum must has been previously registered.
//
//	"name": Required. UUID or alias of the snapshot. The alias of a snapshot is <LVSTORE NAME>/<SNAPSHOT NAME>.
func (c *Client) BdevLvolGetSnapshotChecksum(name string) (checksum string, err error) {
	req := spdktypes.BdevLvolGetSnapshotChecksumRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_lvol_get_snapshot_checksum", req)
	if err != nil {
		return "", err
	}

	var snapshotChecksum spdktypes.BdevLvolSnapshotChecksum
	err = json.Unmarshal(cmdOutput, &snapshotChecksum)
	if err != nil {
		return "", err
	}

	return strconv.FormatUint(snapshotChecksum.Checksum, 10), nil
}

// BdevLvolRegisterRangeChecksums compute and store a checksum for the whole snapshot and a checksum for every snapshot's cluster data. Overwrite old checksums if already registered.
//
//	"name": Required. UUID or alias of the snapshot. The alias of a snapshot is <LVSTORE NAME>/<SNAPSHOT NAME>.
func (c *Client) BdevLvolRegisterRangeChecksums(name string) (registered bool, err error) {
	req := spdktypes.BdevLvolRegisterRangeChecksumsRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_lvol_register_snapshot_range_checksums", req)
	if err != nil {
		return false, err
	}

	return registered, json.Unmarshal(cmdOutput, &registered)
}

// BdevLvolGetRangeChecksums gets snapshot's stored checksums for the clusters in the range. The checksums must have been previously registered.
//
//	"name": Required. UUID or alias of the snapshot. The alias of a snapshot is <LVSTORE NAME>/<SNAPSHOT NAME>.
//
//	"clusterStartIndex": Required. The index of the first cluster in the range.
//
//	"clusterCount": Required. The number of clusters in the range.
func (c *Client) BdevLvolGetRangeChecksums(name string, clusterStartIndex, clusterCount uint64) (dataChecksums map[uint64]uint64, err error) {
	req := spdktypes.BdevLvolGetRangeChecksumsRequest{
		Name:              name,
		ClusterStartIndex: clusterStartIndex,
		ClusterCount:      clusterCount,
	}

	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_lvol_get_snapshot_range_checksums", req)
	if err != nil {
		return nil, err
	}

	var rangeChecksums []spdktypes.BdevLvolRangeChecksum
	err = json.Unmarshal(cmdOutput, &rangeChecksums)
	if err != nil {
		return nil, err
	}

	dataChecksums = make(map[uint64]uint64)
	for _, clusterChecksum := range rangeChecksums {
		dataChecksums[clusterChecksum.ClusterIndex] = clusterChecksum.Checksum
	}

	return dataChecksums, nil
}

// BdevLvolStopSnapshotChecksum stop an ongoing registration of a snapshot's checksum.
// It can be used to stop both BdevLvolRegisterSnapshotChecksum and BdevLvolRegisterRangeChecksums.
//
//	"name": Required. UUID or alias of the snapshot. The alias of a snapshot is <LVSTORE NAME>/<SNAPSHOT NAME>.
func (c *Client) BdevLvolStopSnapshotChecksum(name string) (registered bool, err error) {
	req := spdktypes.BdevLvolStopSnapshotChecksumRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_lvol_stop_snapshot_checksum", req)
	if err != nil {
		return false, err
	}

	return registered, json.Unmarshal(cmdOutput, &registered)
}

// BdevLvolRename renames a logical volume.
//
//	"oldName": Required. UUID or alias of the existing logical volume.
//
//	"newName": Required. New logical volume name.
func (c *Client) BdevLvolRename(oldName, newName string) (renamed bool, err error) {
	req := spdktypes.BdevLvolRenameRequest{
		OldName: oldName,
		NewName: newName,
	}

	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_lvol_rename", req)
	if err != nil {
		return false, err
	}

	err = json.Unmarshal(cmdOutput, &renamed)
	return renamed, err
}

// BdevRaidCreate constructs a new RAID bdev.
//
//		"name": Required. a RAID bdev name rather than an alias or a UUID.
//
//		"raidLevel": Required. RAID level. It can be "0"/"raid0", "1"/"raid1", "5f"/"raid5f", or "concat".
//
//		"stripSizeKb": Required. Strip size in KB. It's valid for raid0 and raid5f only. For other raid levels, this would be modified to 0.
//
//		"baseBdevs": Required. The bdev list used as the underlying disk of the RAID.
//
//	 	"uuid": Optional. Create the raid bdev with specific uuid
func (c *Client) BdevRaidCreate(name string, raidLevel spdktypes.BdevRaidLevel, stripSizeKb uint32, baseBdevs []string, uuid string) (created bool, err error) {
	if raidLevel != spdktypes.BdevRaidLevel0 && raidLevel != spdktypes.BdevRaidLevelRaid0 && raidLevel != spdktypes.BdevRaidLevel5f && raidLevel != spdktypes.BdevRaidLevelRaid5f {
		stripSizeKb = 0
	}
	req := spdktypes.BdevRaidCreateRequest{
		Name:        name,
		RaidLevel:   raidLevel,
		StripSizeKb: stripSizeKb,
		BaseBdevs:   baseBdevs,
	}

	if uuid != "" {
		req.UUID = uuid
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_raid_create", req)
	if err != nil {
		return false, err
	}

	return created, json.Unmarshal(cmdOutput, &created)
}

// BdevRaidDelete destroys a logical volume.
func (c *Client) BdevRaidDelete(name string) (deleted bool, err error) {
	req := spdktypes.BdevRaidDeleteRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_raid_delete", req)
	if err != nil {
		return false, err
	}

	return deleted, json.Unmarshal(cmdOutput, &deleted)
}

// BdevRaidGet gets information about RAID bdevs if a name is not specified.
//
//		"name": Optional. Name of a RAID bdev.
//	         For a RAID bdev, there is no alias nor UUID.
//			 	If this is not specified, the function will list all RAID bdevs.
//
//		"timeout": Optional. 0 by default, meaning the method returns immediately whether the RAID bdev exists or not.
func (c *Client) BdevRaidGet(name string, timeout uint64) (bdevRaidInfoList []spdktypes.BdevInfo, err error) {
	req := spdktypes.BdevGetBdevsRequest{
		Name:    name,
		Timeout: timeout,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_get_bdevs", req)
	if err != nil {
		return nil, err
	}
	bdevInfoList := []spdktypes.BdevInfo{}
	if err := json.Unmarshal(cmdOutput, &bdevInfoList); err != nil {
		return nil, err
	}

	bdevRaidInfoList = []spdktypes.BdevInfo{}
	for _, b := range bdevInfoList {
		if spdktypes.GetBdevType(&b) != spdktypes.BdevTypeRaid {
			continue
		}
		// For the result of bdev_get_bdevs, this field is empty.
		// To avoid confusion or potential issues, we will fill it manually here.
		b.DriverSpecific.Raid.Name = name
		bdevRaidInfoList = append(bdevRaidInfoList, b)
	}

	return bdevRaidInfoList, nil
}

// BdevRaidGetInfoByCategory is used to list all the raid info details based on the input category requested.
//
//	"category": Required. This should be one of 'all', 'online', 'configuring' or 'offline'.
//		'all' means all the raid bdevs whether they are online or configuring or offline.
//		'online' is the raid bdev which is registered with bdev layer.
//		'offline' is the raid bdev which is not registered with bdev as of now and it has encountered any error or user has requested to offline the raid bdev.
//		'configuring' is the raid bdev which does not have full configuration discovered yet.
func (c *Client) BdevRaidGetInfoByCategory(category spdktypes.BdevRaidCategory) (bdevRaidInfoList []spdktypes.BdevRaidInfo, err error) {
	req := spdktypes.BdevRaidGetBdevsRequest{
		Category: category,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_raid_get_bdevs", req)
	if err != nil {
		return nil, err
	}

	return bdevRaidInfoList, json.Unmarshal(cmdOutput, &bdevRaidInfoList)
}

// BdevRaidRemoveBaseBdev is used to list all the raid info details based on the input category requested.
//
//	"name": Required. The base bdev name to be removed from RAID bdevs.
func (c *Client) BdevRaidRemoveBaseBdev(name string) (removed bool, err error) {
	req := spdktypes.BdevRaidRemoveBaseBdevRequest{
		Name: name,
	}

	// Notice that RAID `num_base_bdevs_discovered` will decrease but `num_base_bdevs` won't change after the removal.
	// And it will leave a meaningless record in the `base_bdev_list`, for example:
	//
	// 	"driver_specific": {
	//		"raid": {
	//			"name": "raid01",
	//			"strip_size_kb": 0,
	//			"state": "online",
	//			"raid_level": "raid1",
	//			"num_base_bdevs": 2,
	//			"num_base_bdevs_discovered": 1,
	//			"num_base_bdevs_operational": 1,
	//			"base_bdevs_list": [
	//				{
	//					"name": "spdk-00/lvol0",
	//					"uuid": "617f5bc6-9a86-43c1-9223-2fa9e07894e2",
	//					"is_configured": true,
	//					"data_offset": 0,
	//					"data_size": 25600
	//				},
	//				{
	//					"name": "",
	//					"uuid": "00000000-0000-0000-0000-000000000000",
	//					"is_configured": false,
	//					"data_offset": 0,
	//					"data_size": 25600
	//				}
	//			],
	//			"superblock": false
	//		}
	//	}
	cmdOutput, err := c.jsonCli.SendCommand("bdev_raid_remove_base_bdev", req)
	if err != nil {
		return false, err
	}

	return removed, json.Unmarshal(cmdOutput, &removed)
}

// BdevRaidGrowBaseBdev adds a base bdev to a raid bdev, growing the raid's size if needed
//
//	"raidName": Required. The RAID bdev name.
//
//	"baseBdevName": Required. The base bdev name to be added to the RAID bdev.
func (c *Client) BdevRaidGrowBaseBdev(raidName, baseBdevName string) (growed bool, err error) {
	req := spdktypes.BdevRaidGrowBaseBdevRequest{
		RaidName: raidName,
		BaseName: baseBdevName,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_raid_grow_base_bdev", req)
	if err != nil {
		return false, err
	}

	return growed, json.Unmarshal(cmdOutput, &growed)
}

// BdevNvmeAttachController constructs NVMe bdev.
//
//	"name": Name of the NVMe controller. And the corresponding bdev nvme name are same as the nvme namespace name, which is `{ControllerName}n1`
//
//	"subnqn": NVMe-oF target subnqn. It can be the nvmf subsystem nqn.
//
//	"trsvcid": NVMe-oF target trsvcid: port number
//
//	"trtype": NVMe-oF target trtype: "tcp", "rdma" or "pcie"
//
//	"traddr": NVMe-oF target address: ip or BDF
//
//	"adrfam": NVMe-oF target adrfam: ipv4, ipv6, ib, fc, intra_host
//
// "ctrlrLossTimeoutSec": Controller loss timeout in seconds
//
// "reconnectDelaySec": Controller reconnect delay in seconds
//
// "fastIOFailTimeoutSec": Fast I/O failure timeout in seconds
//
// "multipath": Multipathing behavior: disable, failover, multipath. Default is failover
func (c *Client) BdevNvmeAttachController(name, subnqn, traddr, trsvcid string, trtype spdktypes.NvmeTransportType, adrfam spdktypes.NvmeAddressFamily,
	ctrlrLossTimeoutSec, reconnectDelaySec, fastIOFailTimeoutSec int32, multipath string) (bdevNameList []string, err error) {
	req := spdktypes.BdevNvmeAttachControllerRequest{
		Name: name,
		NvmeTransportID: spdktypes.NvmeTransportID{
			Traddr:  traddr,
			Trtype:  trtype,
			Subnqn:  subnqn,
			Trsvcid: trsvcid,
			Adrfam:  adrfam,
		},
		CtrlrLossTimeoutSec:  ctrlrLossTimeoutSec,
		ReconnectDelaySec:    reconnectDelaySec,
		FastIOFailTimeoutSec: fastIOFailTimeoutSec,
		Multipath:            multipath,
	}

	// Long blob recovery time might be needed if the spdk_tgt is not shutdown gracefully.
	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_nvme_attach_controller", req)
	if err != nil {
		return nil, err
	}

	return bdevNameList, json.Unmarshal(cmdOutput, &bdevNameList)
}

// BdevNvmeDetachController detach NVMe controller and delete any associated bdevs.
//
//	"name": Name of the NVMe controller. e.g., "Nvme0"
func (c *Client) BdevNvmeDetachController(name string) (detached bool, err error) {
	req := spdktypes.BdevNvmeDetachControllerRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_nvme_detach_controller", req)
	if err != nil {
		return false, err
	}

	return detached, json.Unmarshal(cmdOutput, &detached)
}

// BdevNvmeGetControllers gets information about bdev NVMe controllers.
//
//	"name": Name of the NVMe controller. Optional. If this is not specified, the function will list all NVMe controllers.
func (c *Client) BdevNvmeGetControllers(name string) (controllerInfoList []spdktypes.BdevNvmeControllerInfo, err error) {
	req := spdktypes.BdevNvmeGetControllersRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_nvme_get_controllers", req)
	if err != nil {
		return nil, err
	}

	return controllerInfoList, json.Unmarshal(cmdOutput, &controllerInfoList)
}

// BdevNvmeGetControllerHealthInfo retrieves health information for a specified
// NVMe bdev controller.
//
//	"name": Name of the NVMe controller
func (c *Client) BdevNvmeGetControllerHealthInfo(name string) (healthInfo spdktypes.BdevNvmeControllerHealthInfo, err error) {
	req := spdktypes.BdevNvmeGetControllerHealthInfoRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_nvme_get_controller_health_info", req)
	if err != nil {
		return healthInfo, err
	}

	if err := json.Unmarshal(cmdOutput, &healthInfo); err != nil {
		return healthInfo, err
	}

	// Normalize temperature: SPDK writes temperature as unsigned (Kelvin-273).
	// When controller reports invalid/0 temperature in Kelvin, subtracting 273
	// on uint64 underflows and produces a huge number (~2^64 - 273), which is
	// meaningless in Celsius. Clamp such outliers to -1 to indicate unknown.
	if healthInfo.TemperatureCelsius > 255 { // Values >255°C are invalid for a uint8 S.M.A.R.T. temperature.
		healthInfo.TemperatureCelsius = spdktypes.UnknownTemperature
	}

	return healthInfo, nil
}

// BdevNvmeSetOptions sets global parameters for all bdev NVMe.
// This RPC may only be called before SPDK subsystems have been initialized or any bdev NVMe
// has been created.
// Parameters, ctrlr_loss_timeout_sec, reconnect_delay_sec, and fast_io_fail_timeout_sec, are
// for I/O error resiliency. They can be overridden if they are given by the RPC bdev_nvme_attach_controller.
//
// "ctrlrLossTimeoutSec": Controller loss timeout in seconds
//
// "reconnectDelaySec": Controller reconnect delay in seconds
//
// "fastIOFailTimeoutSec": Fast I/O failure timeout in seconds
//
// "transportAckTimeout": Time to wait ack until retransmission for RDMA or connection close for TCP. Range 0-31 where 0 means use default
//
// "keepAliveTimeoutMs": Keep alive timeout in milliseconds.
func (c *Client) BdevNvmeSetOptions(ctrlrLossTimeoutSec, reconnectDelaySec, fastIOFailTimeoutSec, transportAckTimeout, keepAliveTimeoutMs int32) (result bool, err error) {
	req := spdktypes.BdevNvmeSetOptionsRequest{
		CtrlrLossTimeoutSec:  ctrlrLossTimeoutSec,
		ReconnectDelaySec:    reconnectDelaySec,
		FastIOFailTimeoutSec: fastIOFailTimeoutSec,
		TransportAckTimeout:  transportAckTimeout,
		KeepAliveTimeoutMs:   keepAliveTimeoutMs,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_nvme_set_options", req)
	if err != nil {
		return false, err
	}

	return result, json.Unmarshal(cmdOutput, &result)
}

// BdevNvmeGet gets information about NVMe bdevs if a name is not specified.
//
//	"name": Optional. UUID or name of a NVMe bdev.
//	        For a NVMe bdev, the name is `<NVMe namespace name>`, which is typically `<NVMe Controller Name>n1`. And the only alias is UUID.
//		 	If this is not specified, the function will list all NVMe bdevs.
//
//	"timeout": Optional. 0 by default, meaning the method returns immediately whether the NVMe bdev exists or not.
func (c *Client) BdevNvmeGet(name string, timeout uint64) (bdevNvmeInfoList []spdktypes.BdevInfo, err error) {
	req := spdktypes.BdevGetBdevsRequest{
		Name:    name,
		Timeout: timeout,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_get_bdevs", req)
	if err != nil {
		return nil, err
	}
	bdevInfoList := []spdktypes.BdevInfo{}
	if err := json.Unmarshal(cmdOutput, &bdevInfoList); err != nil {
		return nil, err
	}

	bdevNvmeInfoList = []spdktypes.BdevInfo{}
	for _, b := range bdevInfoList {
		if spdktypes.GetBdevType(&b) != spdktypes.BdevTypeNvme {
			continue
		}
		bdevNvmeInfoList = append(bdevNvmeInfoList, b)
	}

	return bdevNvmeInfoList, nil
}

// NvmfCreateTransport initializes an NVMe-oF transport with the given options.
//
//	"trtype": Required. Transport type, "tcp" or "rdma". "tcp" by default.
func (c *Client) NvmfCreateTransport(trtype spdktypes.NvmeTransportType) (created bool, err error) {
	if trtype == "" {
		trtype = spdktypes.NvmeTransportTypeTCP
	}
	req := spdktypes.NvmfCreateTransportRequest{
		Trtype: trtype,
	}

	cmdOutput, err := c.jsonCli.SendCommand("nvmf_create_transport", req)
	if err != nil {
		return false, err
	}

	return created, json.Unmarshal(cmdOutput, &created)
}

// NvmfGetTransports lists all transports if no parameters specified.
//
//	"trtype": Optional. Transport type, "tcp" or "rdma"
//
//	"tgtName": Optional. Parent NVMe-oF target name.
func (c *Client) NvmfGetTransports(trtype spdktypes.NvmeTransportType, tgtName string) (transportList []spdktypes.NvmfTransport, err error) {
	req := spdktypes.NvmfGetTransportRequest{
		Trtype:  trtype,
		TgtName: tgtName,
	}

	cmdOutput, err := c.jsonCli.SendCommand("nvmf_get_transports", req)
	if err != nil {
		return nil, err
	}

	return transportList, json.Unmarshal(cmdOutput, &transportList)
}

// NvmfCreateSubsystem constructs an NVMe over Fabrics target subsystem..
//
//	"nqn": Required. Subsystem NQN.
func (c *Client) NvmfCreateSubsystem(nqn string) (created bool, err error) {
	req := spdktypes.NvmfCreateSubsystemRequest{
		Nqn:          nqn,
		AllowAnyHost: true,
	}

	cmdOutput, err := c.jsonCli.SendCommand("nvmf_create_subsystem", req)
	if err != nil {
		return false, err
	}

	return created, json.Unmarshal(cmdOutput, &created)
}

// NvmfDeleteSubsystem constructs an NVMe over Fabrics target subsystem..
//
//	"nqn": Required. Subsystem NQN.
//
//	"tgtName": Optional. Parent NVMe-oF target name.
func (c *Client) NvmfDeleteSubsystem(nqn, targetName string) (deleted bool, err error) {
	req := spdktypes.NvmfDeleteSubsystemRequest{
		Nqn:     nqn,
		TgtName: targetName,
	}

	cmdOutput, err := c.jsonCli.SendCommand("nvmf_delete_subsystem", req)
	if err != nil {
		return false, err
	}

	return deleted, json.Unmarshal(cmdOutput, &deleted)
}

// NvmfGetSubsystems lists all subsystem for the specified NVMe-oF target.
//
//	"nqn": Optional. Subsystem NQN.
//
//	"tgtName": Optional. Parent NVMe-oF target name.
func (c *Client) NvmfGetSubsystems(nqn, tgtName string) (subsystemList []spdktypes.NvmfSubsystem, err error) {
	req := spdktypes.NvmfGetSubsystemsRequest{
		Nqn:     nqn,
		TgtName: tgtName,
	}

	cmdOutput, err := c.jsonCli.SendCommand("nvmf_get_subsystems", req)
	if err != nil {
		return nil, err
	}

	return subsystemList, json.Unmarshal(cmdOutput, &subsystemList)
}

// NvmfSubsystemAddNs constructs an NVMe over Fabrics target subsystem..
//
//	"nqn": Required. Subsystem NQN.
//
//	"bdevName": Required. Name of bdev to expose as a namespace.
//
//	"nguid": Optional. Namespace globally unique identifier.
func (c *Client) NvmfSubsystemAddNs(nqn, bdevName, nguid string) (nsid uint32, err error) {
	req := spdktypes.NvmfSubsystemAddNsRequest{
		Nqn: nqn,
		Namespace: spdktypes.NvmfSubsystemNamespace{
			BdevName: bdevName,
			Nguid:    nguid,
		},
	}

	cmdOutput, err := c.jsonCli.SendCommand("nvmf_subsystem_add_ns", req)
	if err != nil {
		return 0, err
	}

	return nsid, json.Unmarshal(cmdOutput, &nsid)
}

// NvmfSubsystemRemoveNs constructs an NVMe over Fabrics target subsystem..
//
//	"nqn": Required. Subsystem NQN.
//
//	"nsid": Required. Namespace ID.
func (c *Client) NvmfSubsystemRemoveNs(nqn string, nsid uint32) (deleted bool, err error) {
	req := spdktypes.NvmfSubsystemRemoveNsRequest{
		Nqn:  nqn,
		Nsid: nsid,
	}

	cmdOutput, err := c.jsonCli.SendCommand("nvmf_subsystem_remove_ns", req)
	if err != nil {
		return false, err
	}

	return deleted, json.Unmarshal(cmdOutput, &deleted)
}

// NvmfSubsystemsGetNss lists all namespaces for the specified NVMe-oF target subsystem if bdev name or NSID is not specified.
//
//	"nqn": Required. Subsystem NQN.
//
//	"bdevName": Optional. Name of bdev to expose as a namespace. It's better not to specify this and "nsid" simultaneously.
//
//	"nsid": Optional. Namespace ID. It's better not to specify this and "bdevName" simultaneously.
func (c *Client) NvmfSubsystemsGetNss(nqn, bdevName string, nsid uint32) (nsList []spdktypes.NvmfSubsystemNamespace, err error) {
	req := spdktypes.NvmfGetSubsystemsRequest{}

	cmdOutput, err := c.jsonCli.SendCommand("nvmf_get_subsystems", req)
	if err != nil {
		return nil, err
	}
	subsystemList := []spdktypes.NvmfSubsystem{}
	if err = json.Unmarshal(cmdOutput, &subsystemList); err != nil {
		return nil, err
	}

	nsList = []spdktypes.NvmfSubsystemNamespace{}
	for _, subsystem := range subsystemList {
		if subsystem.Nqn != nqn {
			continue
		}
		for _, ns := range subsystem.Namespaces {
			if nsid > 0 && ns.Nsid != nsid {
				continue
			}
			if bdevName != "" && ns.BdevName != bdevName {
				continue
			}
			nsList = append(nsList, ns)
		}
	}

	return nsList, nil
}

// NvmfSubsystemAddListener adds a new listen address to an NVMe-oF subsystem.
//
//		"nqn": Required. Subsystem NQN.
//
//		"traddr": Required. NVMe-oF target address: an ip or BDF.
//
//		"trsvcid": Required. NVMe-oF target trsvcid: a port number.
//
//		"trtype": Required. NVMe-oF target trtype: "tcp", "rdma" or "pcie". "tcp" by default.
//
//	 	"adrfam": Required. Address family ("ipv4", "ipv6", "ib", or "fc"). "ipv4" by default.
func (c *Client) NvmfSubsystemAddListener(nqn, traddr, trsvcid string, trtype spdktypes.NvmeTransportType, adrfam spdktypes.NvmeAddressFamily) (created bool, err error) {
	req := spdktypes.NvmfSubsystemAddListenerRequest{
		Nqn: nqn,
		ListenAddress: spdktypes.NvmfSubsystemListenAddress{
			Traddr:  traddr,
			Trsvcid: trsvcid,
			Trtype:  trtype,
			Adrfam:  adrfam,
		},
	}

	cmdOutput, err := c.jsonCli.SendCommand("nvmf_subsystem_add_listener", req)
	if err != nil {
		return false, err
	}

	return created, json.Unmarshal(cmdOutput, &created)
}

// NvmfSubsystemRemoveListener removes a listen address from an NVMe-oF subsystem.
//
//		"nqn": Required. Subsystem NQN.
//
//		"traddr": Required. NVMe-oF target address: an ip or BDF.
//
//		"trsvcid": Required. NVMe-oF target trsvcid: a port number.
//
//		"trtype": Required. NVMe-oF target trtype: "tcp", "rdma" or "pcie". "tcp" by default.
//
//	 	"adrfam": Required. Address family ("IPv4", "IPv6", "IB", or "FC"). "IPv4" by default.
func (c *Client) NvmfSubsystemRemoveListener(nqn, traddr, trsvcid string, trtype spdktypes.NvmeTransportType, adrfam spdktypes.NvmeAddressFamily) (deleted bool, err error) {
	req := spdktypes.NvmfSubsystemRemoveListenerRequest{
		Nqn: nqn,
		ListenAddress: spdktypes.NvmfSubsystemListenAddress{
			Traddr:  traddr,
			Trsvcid: trsvcid,
			Trtype:  trtype,
			Adrfam:  adrfam,
		},
	}

	cmdOutput, err := c.jsonCli.SendCommand("nvmf_subsystem_remove_listener", req)
	if err != nil {
		return false, err
	}

	return deleted, json.Unmarshal(cmdOutput, &deleted)
}

// NvmfSubsystemGetListeners lists all listeners for the specified NVMe-oF target subsystem.
//
//	"nqn": Required. Subsystem NQN.
//
//	"tgtName": Optional. Parent NVMe-oF target name.
//
// Note:
//
//  1. Trying to get listeners of a non-existing subsystem will return error: {"code": -32602, "message": "Invalid parameters"}
func (c *Client) NvmfSubsystemGetListeners(nqn, tgtName string) (listenerList []spdktypes.NvmfSubsystemListener, err error) {
	req := spdktypes.NvmfSubsystemGetListenersRequest{
		Nqn:     nqn,
		TgtName: tgtName,
	}

	cmdOutput, err := c.jsonCli.SendCommand("nvmf_subsystem_get_listeners", req)
	if err != nil {
		return nil, err
	}

	return listenerList, json.Unmarshal(cmdOutput, &listenerList)
}

// LogSetFlag sets the log flag.
//
// "flag": Required. Log flag to set.
func (c *Client) LogSetFlag(flag string) (result bool, err error) {
	req := spdktypes.LogSetFlagRequest{
		Flag: flag,
	}

	cmdOutput, err := c.jsonCli.SendCommand("log_set_flag", req)
	if err != nil {
		return false, err
	}

	return result, json.Unmarshal(cmdOutput, &result)
}

// LogClearFlag clears the log flag.
//
// "flag": Required. Log flag to clear.
func (c *Client) LogClearFlag(flag string) (result bool, err error) {
	req := spdktypes.LogClearFlagRequest{
		Flag: flag,
	}

	cmdOutput, err := c.jsonCli.SendCommand("log_clear_flag", req)
	if err != nil {
		return false, err
	}

	return result, json.Unmarshal(cmdOutput, &result)
}

// LogGetFlags gets the log flags.
func (c *Client) LogGetFlags() (flags map[string]bool, err error) {
	req := spdktypes.LogGetFlagsRequest{}

	cmdOutput, err := c.jsonCli.SendCommand("log_get_flags", req)
	if err != nil {
		return nil, err
	}

	return flags, json.Unmarshal(cmdOutput, &flags)
}

// LogSetLevel sets the log level.
//
// "level": Required. Supported values are "disabled", "error", "warn", "notice", "info", "debug". Default is "notice".
func (c *Client) LogSetLevel(level string) (result bool, err error) {
	req := spdktypes.LogSetLevelRequest{
		Level: level,
	}

	cmdOutput, err := c.jsonCli.SendCommand("log_set_level", req)
	if err != nil {
		return false, err
	}

	return result, json.Unmarshal(cmdOutput, &result)
}

// LogGetLevel gets the log level.
func (c *Client) LogGetLevel() (string, error) {
	req := spdktypes.LogGetLevelRequest{}

	level, err := c.jsonCli.SendCommand("log_get_level", req)
	if err != nil {
		return "", err
	}

	return strings.Trim(string(level), "\"\n"), nil
}

// LogSetPrintLevel sets the log print level. The log print level is the level at which log messages are printed to the console.
//
// "level": Required. Supported values are "disabled", "error", "warn", "notice", "info", "debug". Default is "notice".
func (c *Client) LogSetPrintLevel(level string) (result bool, err error) {
	req := spdktypes.LogSetPrintLevelRequest{
		Level: level,
	}

	cmdOutput, err := c.jsonCli.SendCommand("log_set_print_level", req)
	if err != nil {
		return false, err
	}

	return result, json.Unmarshal(cmdOutput, &result)
}

// LogGetPrintLevel gets the log print level. The log print level is the level at which log messages are printed to the console.
func (c *Client) LogGetPrintLevel() (string, error) {
	req := spdktypes.LogGetPrintLevelRequest{}

	level, err := c.jsonCli.SendCommand("log_get_print_level", req)
	if err != nil {
		return "", err
	}

	return strings.Trim(string(level), "\"\n"), nil
}

// BdevVirtioAttachController creates new initiator Virtio SCSI or Virtio Block and expose all found bdevs.
//
// "name": Required. Use this name as base for new created bdevs.
//
// "trtype": Required. Transport type, "user" or "pci".
//
// "traddr": Required. Transport type specific target address: e.g. UNIX domain socket path or BDF.
//
// "devType": Required. Device type, "scsi" or "blk".
func (c *Client) BdevVirtioAttachController(name, trtype, traddr, devType string) ([]string, error) {
	req := spdktypes.BdevVirtioAttachControllerRequest{
		Name:    name,
		Trtype:  trtype,
		Traddr:  traddr,
		DevType: devType,
	}

	// Long blob recovery time might be needed if the spdk_tgt is not shutdown gracefully.
	cmdOutput, err := c.jsonCli.SendCommandWithLongTimeout("bdev_virtio_attach_controller", req)
	if err != nil {
		return nil, err
	}

	var disks []string
	err = json.Unmarshal([]byte(cmdOutput), &disks)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to unmarshal disks: %s", cmdOutput)
	}

	return disks, nil
}

// BdevVirtioDetachController removes a Virtio device.
//
// "name": Required. Use this name as base for new created bdevs.
func (c *Client) BdevVirtioDetachController(name string) (deleted bool, err error) {
	req := spdktypes.BdevVirtioDetachControllerRequest{
		Name: name,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_virtio_detach_controller", req)
	if err != nil {
		return false, err
	}

	return deleted, json.Unmarshal(cmdOutput, &deleted)
}

// BdevGetIostat get I/O statistics of block devices (bdevs).
//
//	"name": Optional. If this is not specified, the function will list all block devices.
//
//	"per_channel": Optional. Display per channel data for specified block device.
func (c *Client) BdevGetIostat(name string, perChannel bool) (resp *spdktypes.BdevIostatResponse, err error) {
	req := spdktypes.BdevIostatRequest{
		Name:       name,
		PerChannel: perChannel,
	}

	cmdOutput, err := c.jsonCli.SendCommand("bdev_get_iostat", req)
	if err != nil {
		return nil, err
	}

	resp = &spdktypes.BdevIostatResponse{}
	if err := json.Unmarshal(cmdOutput, resp); err != nil {
		return nil, errors.Wrap(err, "failed to parse bdev_get_iostat response")
	}

	return resp, nil
}

// BdevSetQosLimit sets the quality of service rate limits on a bdev.
//
//	"name": Required. Block device name to apply QoS settings to.
//
//	"rw_ios_per_sec": Optional. Number of R/W I/Os per second to allow. 0 means unlimited.
//
//	"rw_mbytes_per_sec": Optional. Number of R/W megabytes per second to allow. 0 means unlimited.
//
//	"r_mbytes_per_sec": Optional. Number of Read megabytes per second to allow. 0 means unlimited.
//
//	"w_mbytes_per_sec": Optional. Number of Write megabytes per second to allow. 0 means unlimited.
func (c *Client) BdevSetQosLimit(bdevName string, rwIOsPerSec, rwMBPerSec, rMBPerSec, wMBPerSec int64) error {
	params := map[string]interface{}{
		"name":              bdevName,
		"rw_ios_per_sec":    rwIOsPerSec,
		"rw_mbytes_per_sec": rwMBPerSec,
		"r_mbytes_per_sec":  rMBPerSec,
		"w_mbytes_per_sec":  wMBPerSec,
	}

	resp, err := c.jsonCli.SendCommand("bdev_set_qos_limit", params)
	if err != nil {
		return errors.Wrap(err, "failed to send bdev_set_qos_limit")
	}

	var result bool
	if err := json.Unmarshal(resp, &result); err != nil {
		return errors.Wrapf(err, "invalid response format: %s", string(resp))
	}
	if !result {
		return fmt.Errorf("SPDK returned false for bdev_set_qos_limit")
	}

	return nil
}

// SpdkKillInstance sends a signal to the application.
func (c *Client) SpdkKillInstance(sig string) (result bool, err error) {
	req := spdktypes.SpdkKillInstanceRequest{
		SigName: sig,
	}

	cmdOutput, err := c.jsonCli.SendCommand("spdk_kill_instance", req)
	if err != nil {
		return false, err
	}

	return result, json.Unmarshal(cmdOutput, &result)
}
