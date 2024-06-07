package runtime

// This file implements compiler builtins for slices: append() and copy().

import (
	"unsafe"
)

// Builtin append(src, elements...) function: append elements to src and return
// the modified (possibly expanded) slice.
func sliceAppend(srcBuf, elemsBuf unsafe.Pointer, srcLen, srcCap, elemsLen, elemSize uintptr) (unsafe.Pointer, uintptr, uintptr) {
	newLen := srcLen + elemsLen

	if elemsLen != 0 {
		// Allocate a new slice with capacity for elemsLen more elements, if necessary;
		// otherwise, reuse the passed slice.
		srcBuf, srcLen, srcCap = sliceGrow(srcBuf, srcLen, srcCap, newLen, elemSize)

		// Append the new elements in-place.
		memmove(unsafe.Add(srcBuf, srcLen*elemSize), elemsBuf, elemsLen*elemSize)
	}

	return srcBuf, newLen, srcCap
}

// Builtin copy(dst, src) function: copy bytes from dst to src.
func sliceCopy(dst, src unsafe.Pointer, dstLen, srcLen uintptr, elemSize uintptr) int {
	// n = min(srcLen, dstLen)
	n := srcLen
	if n > dstLen {
		n = dstLen
	}
	memmove(dst, src, n*elemSize)
	return int(n)
}

// sliceGrow returns a new slice with space for at least newCap elements
func sliceGrow(oldBuf unsafe.Pointer, oldLen, oldCap, newCap, elemSize uintptr) (unsafe.Pointer, uintptr, uintptr) {
	if oldCap >= newCap {
		// No need to grow, return the input slice.
		return oldBuf, oldLen, oldCap
	}

	// Mildly over-allocate using a similar integer sequence to what CPython uses when
	// allocating lists.  This still helps amortizing repeated appends to this slice,
	// but is much more conservative than doubling the capacity every time a reallocation
	// is necessary.
	// Sequence goes as follows: 0, 8, 16, 24, 32, 40, 48, 56, 64, 74, 80, ...
	newCap = (newCap + (newCap >> 3) + 6) & 0xfffffff8

	// Account for nil slices.
	if newCap == 0 {
		newCap++
	}

	buf := alloc(newCap*elemSize, nil)
	if oldLen > 0 {
		// copy any data to new slice
		memmove(buf, oldBuf, oldLen*elemSize)
	}

	return buf, oldLen, newCap
}
