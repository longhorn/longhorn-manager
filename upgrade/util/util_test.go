package util

import (
	"sync"
	"testing"
)

func TestProgressMonitorInc(t *testing.T) {
	expectedTargetValue := 1000
	expectedValue := 737
	expectedCurrentProgressInPercentage := 73.0

	pm := NewProgressMonitor("Test PM", 0, expectedTargetValue)

	var wg sync.WaitGroup

	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			var count int
			switch id {
			case 0:
				count = 250
			case 1:
				count = 247
			case 2:
				count = 240
			}
			for j := 0; j < count; j++ {
				pm.Inc()
			}
		}(i)
	}

	wg.Wait()

	currentValue, targetValue, currentProgressInPercentage := pm.GetCurrentProgress()
	if currentValue != expectedValue {
		t.Fatalf(`currentValue = %v, currentedValue = %v`, currentValue, expectedValue)
	}
	if currentProgressInPercentage != expectedCurrentProgressInPercentage {
		t.Fatalf(`currentProgressInPercentage = %v, expectedCurrentProgressInPercentage = %v`, currentProgressInPercentage, expectedCurrentProgressInPercentage)
	}
	if targetValue != expectedTargetValue {
		t.Fatalf(`targetValue = %v, expectedTargetValue = %v`, targetValue, expectedTargetValue)
	}
}
