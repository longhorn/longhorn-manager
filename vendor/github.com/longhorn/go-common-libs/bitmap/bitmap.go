package bitmap

import (
	"fmt"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

type Bitmap struct {
	base  int32
	size  int32
	start int32
	data  *roaring.Bitmap
	lock  *sync.Mutex
}

// NewBitmap allocate a bitmap range from [start, end], notice the end is included
func NewBitmap(start, end int32) (*Bitmap, error) {
	if end < start {
		return nil, fmt.Errorf("invalid range, end (%v) cannot be less than start (%v)", end, start)
	}

	size := end - start + 1
	data := roaring.New()
	if size > 0 {
		data.AddRange(0, uint64(size))
	}
	return &Bitmap{
		base:  start,
		size:  size,
		start: int32(0),
		data:  data,
		lock:  &sync.Mutex{},
	}, nil
}

func (b *Bitmap) findAt(count, iStart int32) (int32, int32, error) {
	i := b.data.Iterator()
	i.AdvanceIfNeeded(uint32(iStart))
	bStart := iStart
	for bStart <= b.size {
		last := int32(-1)
		remains := count
		for i.HasNext() && remains > 0 {
			// first element
			if last < 0 {
				last = int32(i.Next())
				bStart = last
				remains--
				continue
			}
			next := int32(i.Next())
			// failed to find the available range
			if next-last > 1 {
				break
			}
			last = next
			remains--
		}
		if remains == 0 {
			break
		}
		if !i.HasNext() {
			return 0, 0, fmt.Errorf("cannot find an empty port range")
		}
	}
	bEnd := bStart + count - 1
	return bStart, bEnd, nil
}

func (b *Bitmap) AllocateRange(count int32) (int32, int32, error) {
	if count <= 0 {
		return 0, 0, fmt.Errorf("invalid request for non-positive counts: %v", count)
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	// Avoid quick reuse after free.  Start looking where we last stopped.
	rStart, rEnd, err := b.findAt(count, b.start)
	if err != nil {
		// Look again from the bottom.
		rStart, rEnd, err = b.findAt(count, int32(0))
	}
	if err != nil {
		return 0, 0, fmt.Errorf("cannot find an empty port range")
	}

	b.data.RemoveRange(uint64(rStart), uint64(rEnd)+1)
	b.start = rEnd + 1

	return b.base + rStart, b.base + rEnd, nil
}

func (b *Bitmap) ReleaseRange(start, end int32) error {
	if end < start {
		return fmt.Errorf("invalid range: %v-%v", start, end)
	}

	b.lock.Lock()
	defer b.lock.Unlock()

	if start == end && end == 0 {
		return nil
	}
	bStart := start - b.base
	bEnd := end - b.base
	if bStart < 0 || bEnd >= b.size {
		return fmt.Errorf("exceed range: %v-%v (%v-%v)", start, end, bStart, bEnd)
	}
	b.data.AddRange(uint64(bStart), uint64(bEnd)+1)
	return nil
}
