package controller

import (
	"errors"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"github.com/longhorn/longhorn-manager/datastore"
	"github.com/longhorn/longhorn-manager/engineapi"

	longhorn "github.com/longhorn/longhorn-manager/k8s/pkg/apis/longhorn/v1beta2"
)

type failingSnapshotEngineProxy struct {
	engineapi.EngineClientProxy
	purgeCalls int
	cloneCalls int
	err        error
}

func (p *failingSnapshotEngineProxy) SnapshotPurgeStatus(engineapi.DataEngineObject) (map[string]*longhorn.PurgeStatus, error) {
	p.purgeCalls++
	return nil, p.err
}

func (p *failingSnapshotEngineProxy) SnapshotCloneStatus(engineapi.DataEngineObject) (map[string]*longhorn.SnapshotCloneStatus, error) {
	p.cloneCalls++
	return nil, p.err
}

func TestSnapshotConcurrentLimiterEngineProbeWarnings(t *testing.T) {
	for _, tc := range []struct {
		name       string
		state      longhorn.InstanceState
		dataEngine longhorn.DataEngineType
		purgeCalls int
		cloneCalls int
		warnings   []string
	}{
		{
			name:       "stopped engine is not probed",
			state:      longhorn.InstanceStateStopped,
			dataEngine: longhorn.DataEngineTypeV1,
		},
		{
			name:       "running V1 engine warns on purge and image lookup failures",
			state:      longhorn.InstanceStateRunning,
			dataEngine: longhorn.DataEngineTypeV1,
			purgeCalls: 1,
			warnings: []string{
				"Failed to get purge status for engine test-engine",
				"Failed to get CLI API version for engine test-engine",
			},
		},
		{
			name:       "running V2 engine warns on purge and clone failures",
			state:      longhorn.InstanceStateRunning,
			dataEngine: longhorn.DataEngineTypeV2,
			purgeCalls: 1,
			cloneCalls: 1,
			warnings: []string{
				"Failed to get purge status for engine test-engine",
				"Failed to get clone status for engine test-engine",
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			engine := &longhorn.Engine{}
			engine.Name = "test-engine"
			engine.Status.CurrentState = tc.state
			engine.Spec.DataEngine = tc.dataEngine

			proxy := &failingSnapshotEngineProxy{err: errors.New("engine RPC unavailable")}
			logger, logs := test.NewNullLogger()
			limiter := &SnapshotConcurrentLimiter{logger: logger}

			require.False(t, limiter.checkEngineBusy(engine, proxy, &datastore.DataStore{}))
			require.Equal(t, tc.purgeCalls, proxy.purgeCalls)
			require.Equal(t, tc.cloneCalls, proxy.cloneCalls)
			entries := logs.AllEntries()
			require.Len(t, entries, len(tc.warnings))
			for i, warning := range tc.warnings {
				require.Equal(t, logrus.WarnLevel, entries[i].Level)
				require.Equal(t, warning, entries[i].Message)
				require.Error(t, entries[i].Data[logrus.ErrorKey].(error))
			}
		})
	}
}
