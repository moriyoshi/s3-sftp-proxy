package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOffsetRangesIsFullDirectly(t *testing.T) {
	o := NewOffsetRanges(10)
	assert.Equal(t, false, o.IsFull())
	o.Add(0, 10)
	assert.Equal(t, true, o.IsFull())
}

func TestOffsetRangesAddMergeSameRange(t *testing.T) {
	assertOnlyContainsRange(t, NewOffsetRanges(10).MustAdd(0, 5).MustAdd(0, 3), 0, 5)
	assertOnlyContainsRange(t, NewOffsetRanges(10).MustAdd(0, 5).MustAdd(2, 5), 0, 5)
	assertOnlyContainsRange(t, NewOffsetRanges(10).MustAdd(2, 5).MustAdd(0, 3), 0, 5)
	assertOnlyContainsRange(t, NewOffsetRanges(10).MustAdd(0, 5).MustAdd(5, 10), 0, 10)
	assertOnlyContainsRange(t, NewOffsetRanges(10).MustAdd(2, 5).MustAdd(0, 10), 0, 10)
}

func TestOffsetRangesAddMergeSeveralRanges(t *testing.T) {
	assertOnlyContainsRange(t, NewOffsetRanges(10).MustAdd(7, 10).MustAdd(0, 3).MustAdd(2, 8), 0, 10)
	assertOnlyContainsRange(t, NewOffsetRanges(100).MustAdd(7, 10).MustAdd(0, 3).MustAdd(2, 12), 0, 12)
	assertOnlyContainsRange(t, NewOffsetRanges(100).MustAdd(7, 10).MustAdd(2, 5).MustAdd(0, 8), 0, 10)
	assertOnlyContainsRange(t, NewOffsetRanges(100).MustAdd(7, 10).MustAdd(2, 5).MustAdd(0, 12), 0, 12)
	assertOnlyContainsRange(t, NewOffsetRanges(10).MustAdd(7, 8).MustAdd(0, 3).MustAdd(3, 7), 0, 8)
}

func TestOffsetRangesAddNewRange(t *testing.T) {
	assertRanges(t, NewOffsetRanges(10).MustAdd(7, 8).MustAdd(0, 5), []*offsetRange{&offsetRange{0, 5}, &offsetRange{7, 8}})
	assertRanges(t, NewOffsetRanges(10).MustAdd(0, 5).MustAdd(7, 8), []*offsetRange{&offsetRange{0, 5}, &offsetRange{7, 8}})
	assertRanges(t, NewOffsetRanges(10).MustAdd(2, 3).MustAdd(5, 6).MustAdd(8, 9).MustAdd(0, 7), []*offsetRange{&offsetRange{0, 7}, &offsetRange{8, 9}})
	assertRanges(t, NewOffsetRanges(10).MustAdd(0, 3).MustAdd(5, 6).MustAdd(8, 9).MustAdd(1, 7), []*offsetRange{&offsetRange{0, 7}, &offsetRange{8, 9}})
}

func TestOffsetRangesGetMaxValidOffset(t *testing.T) {
	assert.Equal(t, NewOffsetRanges(10).MustAdd(3, 8).GetMaxValidOffset(), int64(-1))
	assert.Equal(t, NewOffsetRanges(10).MustAdd(3, 8).MustAdd(0, 2).GetMaxValidOffset(), int64(-1))
	assert.Equal(t, NewOffsetRanges(10).MustAdd(0, 3).GetMaxValidOffset(), int64(3))
}

func assertOnlyContainsRange(t *testing.T, o *OffsetRanges, s int64, e int64) {
	assert.Equal(t, 1, o.l.Len(), "Expected only 1 range")
	assertRange(t, o.l.Front().Value, s, e)
}

func assertRange(t *testing.T, elem interface{}, s int64, e int64) {
	r := elem.(*offsetRange)
	if r.s != s || r.e != e {
		assert.Failf(t, "Range mismatches", "Expected [%d,%d] and found [%d,%d]", s, e, r.s, r.e)
	}
}

func assertRanges(t *testing.T, o *OffsetRanges, ranges []*offsetRange) {
	assert.Equal(t, len(ranges), o.l.Len(), "Ranges length mismatch")
	i := 0
	for e := o.l.Front(); e != nil && i < len(ranges); e = e.Next() {
		assertRange(t, e.Value, ranges[i].s, ranges[i].e)
		i++
	}
}
