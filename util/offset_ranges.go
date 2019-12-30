package util

import (
	"container/list"
	"fmt"
)

type offsetRange struct {
	// Start offset
	s int64

	// End offset
	e int64
}

// OffsetRanges contains used offset ranges.
// A sorted double linked list is used internally to add ranges.
type OffsetRanges struct {
	maxOffset int64
	l         *list.List
}

// NewOffsetRanges creates a new offset ranges
func NewOffsetRanges(maxOffset int64) *OffsetRanges {
	return &OffsetRanges{
		maxOffset: maxOffset,
		l:         list.New(),
	}
}

// Add adds a range
func (o *OffsetRanges) Add(start int64, end int64) error {
	if end > o.maxOffset {
		return fmt.Errorf("End range is higher than maximum offset")
	}

	for e := o.l.Front(); e != nil; e = e.Next() {
		r := e.Value.(*offsetRange)
		if start >= r.s && start <= r.e {
			if end >= r.s && end <= r.e {
				return nil
			}
			r.e = o.cleanInnerRanges(e, end)
			return nil
		} else if end >= r.s && end <= r.e {
			r.s = start
			return nil
		} else if start < r.s && end > r.e {
			r.s = start
			r.e = o.cleanInnerRanges(e, end)
			return nil
		} else if end < r.s {
			o.l.InsertBefore(&offsetRange{s: start, e: end}, e)
			return nil
		}
	}
	o.l.PushBack(&offsetRange{s: start, e: end})
	return nil
}

// MustAdd adds a range and panics if it returns an error. If not, it returns the initial object
func (o *OffsetRanges) MustAdd(start int64, end int64) *OffsetRanges {
	if err := o.Add(start, end); err != nil {
		panic("Add returned error")
	}
	return o
}

// IsFull checks if offset ranges is already full
func (o *OffsetRanges) IsFull() bool {
	if o.l.Len() == 1 {
		r := o.l.Front().Value.(*offsetRange)
		return r.s == 0 && r.e == o.maxOffset
	}
	return false
}

// GetMaxValidOffset checks if there is only one offset range that
// starts by 0 and returns the end of this offset. It returns -1 in
// other cases
func (o *OffsetRanges) GetMaxValidOffset() int64 {
	if o.l.Len() == 1 {
		r := o.l.Front().Value.(*offsetRange)
		if r.s == 0 {
			return r.e
		}
	}
	return -1
}

func (o *OffsetRanges) cleanInnerRanges(e *list.Element, end int64) int64 {
	for e = e.Next(); e != nil; {
		rn := e.Value.(*offsetRange)
		if end < rn.s {
			return end
		}
		del := e
		e = e.Next()
		o.l.Remove(del)
		if end <= rn.e {
			return rn.e
		}
	}
	return end
}
