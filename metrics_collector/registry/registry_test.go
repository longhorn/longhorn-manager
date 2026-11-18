package registry

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRuntimeAndProcessMetricsRegistered(t *testing.T) {
	mfs, err := longhornCustomRegistry.Gather()
	require.NoError(t, err)

	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}

	expected := []string{
		"go_goroutines",
		"go_threads",
		"go_gc_duration_seconds",
		"go_memstats_heap_alloc_bytes",
		"go_info",
	}
	if runtime.GOOS == "linux" {
		expected = append(expected,
			"process_cpu_seconds_total",
			"process_resident_memory_bytes",
			"process_open_fds",
			"process_max_fds",
			"process_start_time_seconds",
		)
	}
	for _, name := range expected {
		require.True(t, names[name], "metric %s is not registered", name)
	}
}
