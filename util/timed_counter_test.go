package util

import (
	"testing"
	"time"
)

func TestGetCount(t *testing.T) {
	counter := NewTimedCounter(1 * time.Minute)

	counter.IncreaseCount("item1")
	if count := counter.GetCount("item1"); count != 1 {
		t.Errorf("expected count 1, got %d", count)
	}

	counter.IncreaseCount("item1")
	if count := counter.GetCount("item1"); count != 2 {
		t.Errorf("expected count 2, got %d", count)
	}

	if count := counter.GetCount("nonexistent"); count != 0 {
		t.Errorf("expected count 0 for nonexistent item, got %d", count)
	}
}

func TestDeleteEntry(t *testing.T) {
	counter := NewTimedCounter(1 * time.Minute)

	counter.IncreaseCount("item1")
	counter.DeleteEntry("item1")

	if numEntries := counter.GetTotalEntries(); numEntries != 0 {
		t.Errorf("expected 0 entry after deletion, got %d", numEntries)
	}

	if count := counter.GetCount("item1"); count != 0 {
		t.Errorf("expected count 0 after deletion, got %d", count)
	}
}

func TestGC(t *testing.T) {
	counter := NewTimedCounter(200 * time.Millisecond)

	counter.IncreaseCount("item1")
	counter.IncreaseCount("item2")

	time.Sleep(400 * time.Millisecond) // Wait for entries to expire

	counter.gc() // Manually trigger garbage collection

	if numEntries := counter.GetTotalEntries(); numEntries != 0 {
		t.Errorf("expected 0 entry after gc, got %d", numEntries)
	}
}

func TestRunGC(t *testing.T) {
	counter := NewTimedCounter(200 * time.Millisecond)

	counter.IncreaseCount("item1")
	counter.IncreaseCount("item2")

	stopCh := make(chan struct{})
	go counter.RunGC(300*time.Millisecond, stopCh)

	time.Sleep(400 * time.Millisecond) // Wait for GC to run

	if numEntries := counter.GetTotalEntries(); numEntries != 0 {
		t.Errorf("expected 0 entry, got %d", numEntries)
	}

	close(stopCh)
}
