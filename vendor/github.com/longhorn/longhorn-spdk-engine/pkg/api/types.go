package api

import (
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/longhorn/types/pkg/generated/spdkrpc"

	"github.com/longhorn/longhorn-spdk-engine/pkg/types"
)

type SnapshotOptions struct {
	UserCreated bool
	Timestamp   string
}

type Replica struct {
	Name             string           `json:"name"`
	LvsName          string           `json:"lvs_name"`
	LvsUUID          string           `json:"lvs_uuid"`
	SpecSize         uint64           `json:"spec_size"`
	ActualSize       uint64           `json:"actual_size"`
	Head             *Lvol            `json:"head"`
	Snapshots        map[string]*Lvol `json:"snapshots"`
	IP               string           `json:"ip"`
	PortStart        int32            `json:"port_start"`
	PortEnd          int32            `json:"port_end"`
	State            string           `json:"state"`
	ErrorMsg         string           `json:"error_msg"`
	Rebuilding       bool             `json:"rebuilding"`
	BackingImageName string           `json:"backing_image_name"`
	UUID             string           `json:"uuid"`
}

type Lvol struct {
	Name              string          `json:"name"`
	UUID              string          `json:"uuid"`
	SpecSize          uint64          `json:"spec_size"`
	ActualSize        uint64          `json:"actual_size"`
	Parent            string          `json:"parent"`
	Children          map[string]bool `json:"children"`
	CreationTime      string          `json:"creation_time"`
	UserCreated       bool            `json:"user_created"`
	SnapshotTimestamp string          `json:"snapshot_timestamp"`
	SnapshotChecksum  string          `json:"snapshot_checksum"`
}

func ProtoLvolToLvol(l *spdkrpc.Lvol) *Lvol {
	if l == nil {
		return nil
	}
	parent := l.Parent
	if types.IsBackingImageSnapLvolName(parent) {
		parent = ""
	}
	return &Lvol{
		Name: l.Name,
		// UUID:         l.Uuid,
		SpecSize:          l.SpecSize,
		ActualSize:        l.ActualSize,
		Parent:            parent,
		Children:          l.Children,
		CreationTime:      l.CreationTime,
		UserCreated:       l.UserCreated,
		SnapshotTimestamp: l.SnapshotTimestamp,
		SnapshotChecksum:  l.SnapshotChecksum,
	}
}

func LvolToProtoLvol(l *Lvol) *spdkrpc.Lvol {
	if l == nil {
		return nil
	}
	return &spdkrpc.Lvol{
		Name: l.Name,
		// Uuid:         l.UUID,
		SpecSize:          l.SpecSize,
		ActualSize:        l.ActualSize,
		Parent:            l.Parent,
		Children:          l.Children,
		CreationTime:      l.CreationTime,
		UserCreated:       l.UserCreated,
		SnapshotTimestamp: l.SnapshotTimestamp,
		SnapshotChecksum:  l.SnapshotChecksum,
	}
}

func ProtoReplicaToReplica(r *spdkrpc.Replica) *Replica {
	res := &Replica{
		Name:       r.Name,
		LvsName:    r.LvsName,
		LvsUUID:    r.LvsUuid,
		SpecSize:   r.SpecSize,
		ActualSize: r.ActualSize,
		Head:       ProtoLvolToLvol(r.Head),
		Snapshots:  map[string]*Lvol{},
		IP:         r.Ip,
		PortStart:  r.PortStart,
		PortEnd:    r.PortEnd,
		State:      r.State,
		ErrorMsg:   r.ErrorMsg,
		Rebuilding: r.Rebuilding,
		UUID:       r.Uuid,
	}
	for snapName, snapProtoLvol := range r.Snapshots {
		res.Snapshots[snapName] = ProtoLvolToLvol(snapProtoLvol)
	}

	if r.BackingImageName != "" {
		res.BackingImageName = r.BackingImageName
	}

	return res
}

func ReplicaToProtoReplica(r *Replica) *spdkrpc.Replica {
	snapshots := map[string]*spdkrpc.Lvol{}
	for name, snapshot := range r.Snapshots {
		snapshots[name] = LvolToProtoLvol(snapshot)
	}

	res := &spdkrpc.Replica{
		Name:       r.Name,
		LvsName:    r.LvsName,
		LvsUuid:    r.LvsUUID,
		SpecSize:   r.SpecSize,
		ActualSize: r.ActualSize,
		Ip:         r.IP,
		PortStart:  r.PortStart,
		PortEnd:    r.PortEnd,
		Head:       LvolToProtoLvol(r.Head),
		Snapshots:  snapshots,
		Rebuilding: r.Rebuilding,
		State:      r.State,
		ErrorMsg:   r.ErrorMsg,
		Uuid:       r.UUID,
	}

	if r.BackingImageName != "" {
		res.BackingImageName = r.BackingImageName
	}
	return res
}

type Engine struct {
	Name              string                `json:"name"`
	VolumeName        string                `json:"volumeName"`
	SpecSize          uint64                `json:"spec_size"`
	ActualSize        uint64                `json:"actual_size"`
	IP                string                `json:"ip"`
	Port              int32                 `json:"port"`
	TargetIP          string                `json:"target_ip"`
	TargetPort        int32                 `json:"target_port"`
	StandbyTargetPort int32                 `json:"standby_target_port"`
	ReplicaAddressMap map[string]string     `json:"replica_address_map"`
	ReplicaModeMap    map[string]types.Mode `json:"replica_mode_map"`
	Head              *Lvol                 `json:"head"`
	Snapshots         map[string]*Lvol      `json:"snapshots"`
	Frontend          string                `json:"frontend"`
	Endpoint          string                `json:"endpoint"`
	State             string                `json:"state"`
	ErrorMsg          string                `json:"error_msg"`
	UblkID            int32                 `json:"ublk_id"`
	UUID              string                `json:"uuid"`
}

func ProtoEngineToEngine(e *spdkrpc.Engine) *Engine {
	res := &Engine{
		Name:              e.Name,
		VolumeName:        e.VolumeName,
		SpecSize:          e.SpecSize,
		ActualSize:        e.ActualSize,
		IP:                e.Ip,
		Port:              e.Port,
		TargetIP:          e.TargetIp,
		TargetPort:        e.TargetPort,
		StandbyTargetPort: e.StandbyTargetPort,
		ReplicaAddressMap: e.ReplicaAddressMap,
		ReplicaModeMap:    map[string]types.Mode{},
		Head:              ProtoLvolToLvol(e.Head),
		Snapshots:         map[string]*Lvol{},
		Frontend:          e.Frontend,
		Endpoint:          e.Endpoint,
		State:             e.State,
		ErrorMsg:          e.ErrorMsg,
		UblkID:            e.UblkId,
		UUID:              e.Uuid,
	}
	for rName, mode := range e.ReplicaModeMap {
		res.ReplicaModeMap[rName] = types.GRPCReplicaModeToReplicaMode(mode)
	}
	for snapshotName, snapProtoLvol := range e.Snapshots {
		res.Snapshots[snapshotName] = ProtoLvolToLvol(snapProtoLvol)
	}

	return res
}

type BackingImage struct {
	Name             string `json:"name"`
	BackingImageUUID string `json:"backing_image_uuid"`
	LvsName          string `json:"lvs_name"`
	LvsUUID          string `json:"lvs_uuid"`
	Size             uint64 `json:"size"`
	ExpectedChecksum string `json:"expected_checksum"`
	Snapshot         *Lvol  `json:"snapshot"`
	Progress         int32  `json:"progress"`
	State            string `json:"state"`
	CurrentChecksum  string `json:"current_checksum"`
	ErrorMsg         string `json:"error_msg"`
}

func ProtoBackingImageToBackingImage(bi *spdkrpc.BackingImage) *BackingImage {
	res := &BackingImage{
		Name:             bi.Name,
		BackingImageUUID: bi.BackingImageUuid,
		LvsName:          bi.LvsName,
		LvsUUID:          bi.LvsUuid,
		Size:             bi.Size,
		ExpectedChecksum: bi.ExpectedChecksum,
		Snapshot:         ProtoLvolToLvol(bi.Snapshot),
		Progress:         bi.Progress,
		State:            bi.State,
		CurrentChecksum:  bi.CurrentChecksum,
		ErrorMsg:         bi.ErrorMsg,
	}

	return res
}

func BackingImageToProtoBackingImage(bi *BackingImage) *spdkrpc.BackingImage {
	return &spdkrpc.BackingImage{
		Name:             bi.Name,
		BackingImageUuid: bi.BackingImageUUID,
		LvsName:          bi.LvsName,
		LvsUuid:          bi.LvsUUID,
		Size:             bi.Size,
		ExpectedChecksum: bi.ExpectedChecksum,
		Snapshot:         LvolToProtoLvol(bi.Snapshot),
		Progress:         bi.Progress,
		State:            bi.State,
		CurrentChecksum:  bi.CurrentChecksum,
		ErrorMsg:         bi.ErrorMsg,
	}
}

type DiskInfo struct {
	ID          string
	Name        string
	UUID        string
	Path        string
	Type        string
	TotalSize   int64
	FreeSize    int64
	TotalBlocks int64
	FreeBlocks  int64
	BlockSize   int64
	ClusterSize int64
}

type ReplicaRebuildingStatus struct {
	DstReplicaName    string `json:"dst_replica_name"`
	DstReplicaAddress string `json:"dst_replica_address"`
	SrcReplicaName    string `json:"src_replica_name"`
	SrcReplicaAddress string `json:"src_replica_address"`
	SnapshotName      string `json:"snapshot_name"`
	State             string `json:"state"`
	Progress          uint32 `json:"progress"`
	TotalState        string `json:"total_state"`
	TotalProgress     uint32 `json:"total_progress"`
	Error             string `json:"error"`
}

func ProtoShallowCopyStatusToReplicaRebuildingStatus(replicaName, replicaAddress string, status *spdkrpc.ReplicaRebuildingDstShallowCopyCheckResponse) *ReplicaRebuildingStatus {
	return &ReplicaRebuildingStatus{
		DstReplicaName:    replicaName,
		DstReplicaAddress: replicaAddress,
		SrcReplicaName:    status.SrcReplicaName,
		SrcReplicaAddress: status.SrcReplicaAddress,
		SnapshotName:      status.SnapshotName,
		State:             status.State,
		Progress:          status.Progress,
		TotalState:        status.TotalState,
		TotalProgress:     status.TotalProgress,
		Error:             status.Error,
	}
}

type ReplicaSnapshotCloneSrcStatus struct {
	State             string `json:"state"`
	ProcessedClusters uint64 `json:"processed_clusters"`
	TotalClusters     uint64 `json:"total_clusters"`
	ErrorMsg          string `json:"error_msg"`
}

func ProtoReplicaSnapshotCloneSrcStatusCheckResponseToSnapshotCloneSrcStatus(status *spdkrpc.ReplicaSnapshotCloneSrcStatusCheckResponse) *ReplicaSnapshotCloneSrcStatus {
	state := status.State
	if state == types.SPDKDeepCopyStateInProgress {
		state = types.ProgressStateInProgress
	}
	return &ReplicaSnapshotCloneSrcStatus{
		State:             state,
		ProcessedClusters: status.ProcessedClusters,
		TotalClusters:     status.TotalClusters,
		ErrorMsg:          status.ErrorMsg,
	}
}

type ReplicaSnapshotCloneDstStatus struct {
	IsCloning         bool   `json:"is_cloning"`
	SrcReplicaName    string `json:"src_replica_name"`
	SrcReplicaAddress string `json:"src_replica_address"`
	SnapshotName      string `json:"snapshot_name"`
	State             string `json:"state"`
	Progress          uint32 `json:"progress"`
	Error             string `json:"error"`
}

func ProtoReplicaSnapshotCloneDstStatusCheckResponseToSnapshotCloneDstStatus(status *spdkrpc.ReplicaSnapshotCloneDstStatusCheckResponse) *ReplicaSnapshotCloneDstStatus {
	state := status.State
	if state == types.SPDKDeepCopyStateInProgress {
		state = types.ProgressStateInProgress
	}
	return &ReplicaSnapshotCloneDstStatus{
		IsCloning:         status.IsCloning,
		SrcReplicaName:    status.SrcReplicaName,
		SrcReplicaAddress: status.SrcReplicaAddress,
		SnapshotName:      status.SnapshotName,
		State:             state,
		Progress:          status.Progress,
		Error:             status.Error,
	}
}

type ReplicaStream struct {
	stream spdkrpc.SPDKService_ReplicaWatchClient
}

func NewReplicaStream(stream spdkrpc.SPDKService_ReplicaWatchClient) *ReplicaStream {
	return &ReplicaStream{
		stream,
	}
}

func (s *ReplicaStream) Recv() (*emptypb.Empty, error) {
	return s.stream.Recv()
}

type EngineStream struct {
	stream spdkrpc.SPDKService_EngineWatchClient
}

func NewEngineStream(stream spdkrpc.SPDKService_EngineWatchClient) *EngineStream {
	return &EngineStream{
		stream,
	}
}

func (s *EngineStream) Recv() (*emptypb.Empty, error) {
	return s.stream.Recv()
}

type BackingImageStream struct {
	stream spdkrpc.SPDKService_BackingImageWatchClient
}

func NewBackingImageStream(stream spdkrpc.SPDKService_BackingImageWatchClient) *BackingImageStream {
	return &BackingImageStream{
		stream,
	}
}

func (s *BackingImageStream) Recv() (*emptypb.Empty, error) {
	return s.stream.Recv()
}
