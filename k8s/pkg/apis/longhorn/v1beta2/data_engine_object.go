package v1beta2

// DataEngineObject is the compile-time safe abstraction over *Engine and
// *EngineFrontend. It replaces interface{} in the EngineClient API so that
// callers and implementations remain type-safe at compile time.
type DataEngineObject interface {
	// GetDataEngine returns the data engine type string (e.g. "v1" or "v2").
	GetDataEngine() string
	// GetEngineName returns the engine CR name.
	// For Engine objects this is the object's own name.
	// For EngineFrontend objects this is the referenced engine (Spec.EngineName).
	GetEngineName() string
	// GetEngineFrontendName returns the engine frontend CR name.
	// For Engine objects this returns "".
	// For EngineFrontend objects this is the object's own name.
	GetEngineFrontendName() string
	// GetVolumeName returns the volume name from Spec.
	GetVolumeName() string
	// GetVolumeSize returns Spec.VolumeSize.
	GetVolumeSize() int64
	// GetInstanceManagerName returns Status.InstanceManagerName.
	GetInstanceManagerName() string
	// GetStorageIP returns Status.StorageIP.
	GetStorageIP() string
	// GetPort returns Status.Port.
	GetPort() int
}

// Compile-time proof that both CRD pointer types satisfy DataPlaneObject.
var (
	_ DataEngineObject = (*Engine)(nil)
	_ DataEngineObject = (*EngineFrontend)(nil)
)

// --- *Engine ---

func (e *Engine) GetDataEngine() string         { return string(e.Spec.DataEngine) }
func (e *Engine) GetEngineName() string          { return e.Name }
func (e *Engine) GetEngineFrontendName() string  { return "" }
func (e *Engine) GetVolumeName() string          { return e.Spec.VolumeName }
func (e *Engine) GetVolumeSize() int64           { return e.Spec.VolumeSize }
func (e *Engine) GetInstanceManagerName() string { return e.Status.InstanceManagerName }
func (e *Engine) GetStorageIP() string           { return e.Status.StorageIP }
func (e *Engine) GetPort() int                   { return e.Status.Port }

// --- *EngineFrontend ---

func (ef *EngineFrontend) GetDataEngine() string         { return string(ef.Spec.DataEngine) }
func (ef *EngineFrontend) GetEngineName() string          { return ef.Spec.EngineName }
func (ef *EngineFrontend) GetEngineFrontendName() string  { return ef.Name }
func (ef *EngineFrontend) GetVolumeName() string          { return ef.Spec.VolumeName }
// GetVolumeSize returns the EngineFrontend's own desired size (ef.Spec.Size),
// which is the size the frontend device should be driven toward. This is
// distinct from ef.Spec.VolumeSize, which tracks the volume-level size
// propagated from Volume.Spec.Size and is used by DataEngineObject consumers
// (e.g., VolumeExpand) that need the EF-scoped target size.
func (ef *EngineFrontend) GetVolumeSize() int64           { return ef.Spec.Size }
func (ef *EngineFrontend) GetInstanceManagerName() string { return ef.Status.InstanceManagerName }
func (ef *EngineFrontend) GetStorageIP() string           { return ef.Status.StorageIP }
func (ef *EngineFrontend) GetPort() int                   { return ef.Status.Port }
