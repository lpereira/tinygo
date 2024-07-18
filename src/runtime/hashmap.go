package runtime

// This is a hashmap implementation for the map[T]T type.

import (
	"math/bits"
	"reflect"
	"unsafe"
)

// The underlying hashmap structure for Go.
//
// This structure deviates quite a bit from the one used in the Big Go implementation,
// and is more suitable for enviroments with severe memory constraints.
//
// Although the hash function calculates 32-bit of data, only the topmost 8 bits are used.
// This is known as the tophash. Of these 8 bits, the lower 3 bits are used to index a bucket
// chain, and the upper 4 bits are used in a bitmap in each chain node to denote if a particular
// slot has a key with that particular hash.
//
// A hashmap contains an array of pointers to 8 buckets.  Each bucket is part of a singly-linked
// linked list, and contains a 32-bit bitmap with the most significant nibble of the tophash.
// Buckets are indexed by the 3 least significant bits from the tophash, so using only 4 bits
// per element in the bucket bitmap not only saves memory, but also allows us to implement
// this scheme without resorting to 64-bit math, that might not be available in all targets.
// (1 bit from the tophash is currently unused, but might be used later in an additional 8-bit
// per-bucket bitmap.)
//
// A bucket may be partially filled (if that particular bucket chain doesn't have items to fill
// all buckets), but a new one will only be allocated if there are no other buckets in the chain
// with space for the new item. Bucket usage is controlled by an 8-bit bitmap.
//
// This structure offers near-constant time lookups, all the while maintaining a high load factor
// and low memory utilization.
type hashmap struct {
	bucket    [8]*hashmapBucket // indexed by tophash&7
	pool      *hashmapBucket
	count     uintptr
	keySize   uintptr // maybe this can store the key type as well? E.g. keysize == 5 means string?
	valueSize uintptr
	keyEqual  func(x, y unsafe.Pointer, n uintptr) bool
	keyHash   func(key unsafe.Pointer, size uintptr) uint32
}

type hashmapAlgorithm uint8

const (
	hashmapAlgorithmBinary hashmapAlgorithm = iota
	hashmapAlgorithmString
	hashmapAlgorithmInterface
)

// Bucket slots aren't indexed by the hash of a key, so there's no need to carry
// a seed per hashmap instance to avoid issues such as the one described by Crosby
// and Wallach[1].
//
// A bucket will have 100% utilization before we need another one, and we use a
// fast nibble lookup to know which bucket corresponds to a particular tophash.
// There's no rehashing when the hashmap grows either, which is done to avoid
// generating too much garbage and reusing memory allocations as much as possible.
//
// [1] https://www.usenix.org/legacy/events/sec03/tech/full_papers/crosby/crosby.pdf
var randomSeed = uintptr(fastrand())

// A hashmap bucket. A bucket is a container of 8 key/value pairs: first the
// following three entries, then the 8 keys, then the 8 values. This somewhat odd
// ordering is to make sure the keys and values are well aligned when one of
// them is smaller than the system word size.
type hashmapBucket struct {
	next *hashmapBucket

	nibbleHashes uint32 // upper 4 bits of each tophash
	usedSlots    uint8

	// Followed by the actual keys, and then the actual values. These are
	// allocated but as they're of variable size they can't be shown here.
}

type hashmapIterator struct {
	bucket      *hashmapBucket
	usedSlots   uint8
	bucketIndex uint8
}

func hashmapNewIterator() unsafe.Pointer {
	return unsafe.Pointer(new(hashmapIterator))
}

// Get the topmost 8 bits of the hash, without using a special value (like 0).
func shrinkHash(hash uint32) (uint8, uint8) {
	tophash := uint8(hash >> 24)
	nibblehash := tophash >> 4
	return tophash, nibblehash
}

// Create a new hashmap with the given keySize and valueSize.
func hashmapMake(keySize, valueSize uintptr, sizeHint uintptr, alg uint8) *hashmap {
	hashAlg := hashmapAlgorithm(alg)
	ret := &hashmap{
		keySize:   keySize,
		valueSize: valueSize,
		keyEqual:  hashmapKeyEqualAlg(hashAlg),
		keyHash:   hashmapKeyHashAlg(hashAlg),
	}

	if sizeHint != 0 {
		// Calculate the number of buckets by computing iceil(sizeHint / 8).
		nBuckets := 1 + ((sizeHint - 1) >> 3)
		bucketBufSize := hashmapBucketSize(ret)
		// FIXME: this should be a single large allocation but the compiler crashes when we do that!
		for i := uintptr(0); i < nBuckets; i++ {
			bucket := (*hashmapBucket)(alloc(bucketBufSize, nil))
			bucket.next = ret.pool
			ret.pool = bucket
		}
	}

	return ret
}

func hashmapNewBucket(m *hashmap) *hashmapBucket {
	if m.pool != nil {
		b := m.pool
		m.pool = b.next
		return b
	}

	return (*hashmapBucket)(alloc(hashmapBucketSize(m), nil))
}

func hashmapMakeUnsafePointer(keySize, valueSize uintptr, sizeHint uintptr, alg uint8) unsafe.Pointer {
	return (unsafe.Pointer)(hashmapMake(keySize, valueSize, sizeHint, alg))
}

// Remove all entries from the map, without actually deallocating the space for
// it. This is used for the clear builtin, and can be used to reuse a map (to
// avoid extra heap allocations).
func hashmapClear(m *hashmap) {
	if m == nil {
		// Nothing to do. According to the spec:
		// > If the map or slice is nil, clear is a no-op.
		return
	}

	m.count = 0
	sizeToZero := hashmapBucketSize(m)
	for _, bucket := range m.bucket {
		for node := bucket; node != nil; node = node.next {
			memzero(unsafe.Pointer(node), sizeToZero)
			node.next = m.pool
			m.pool = node
		}
	}
}

func hashmapKeyEqualAlg(alg hashmapAlgorithm) func(x, y unsafe.Pointer, n uintptr) bool {
	switch alg {
	case hashmapAlgorithmBinary:
		return memequal
	case hashmapAlgorithmString:
		return hashmapStringEqual
	case hashmapAlgorithmInterface:
		return hashmapInterfaceEqual
	default:
		// compiler bug :(
		return nil
	}
}

func hashmapKeyHashAlg(alg hashmapAlgorithm) func(key unsafe.Pointer, n uintptr) uint32 {
	switch alg {
	case hashmapAlgorithmBinary:
		return hashmapHash32
	case hashmapAlgorithmString:
		return hashmapStringPtrHash
	case hashmapAlgorithmInterface:
		return hashmapInterfacePtrHash
	default:
		// compiler bug :(
		return nil
	}
}

// Return the number of entries in this hashmap, called from the len builtin.
// A nil hashmap is defined as having length 0.
//
//go:inline
func hashmapLen(m *hashmap) int {
	if m == nil {
		return 0
	}
	return int(m.count)
}

func hashmapLenUnsafePointer(m unsafe.Pointer) int {
	return hashmapLen((*hashmap)(m))
}

//go:inline
func hashmapBucketSize(m *hashmap) uintptr {
	return unsafe.Sizeof(hashmapBucket{}) + uintptr(m.keySize)*8 + uintptr(m.valueSize)*8
}

//go:inline
func hashmapSlotKey(m *hashmap, bucket *hashmapBucket, slot uint8) unsafe.Pointer {
	slotKeyOffset := unsafe.Sizeof(hashmapBucket{}) + uintptr(m.keySize)*uintptr(slot)
	slotKey := unsafe.Add(unsafe.Pointer(bucket), slotKeyOffset)
	return slotKey
}

//go:inline
func hashmapSlotValue(m *hashmap, bucket *hashmapBucket, slot uint8) unsafe.Pointer {
	slotValueOffset := unsafe.Sizeof(hashmapBucket{}) + uintptr(m.keySize)*8 + uintptr(m.valueSize)*uintptr(slot)
	slotValue := unsafe.Add(unsafe.Pointer(bucket), slotValueOffset)
	return slotValue
}

// From "Bit Twiddling Hacks": determine if a word has a byte equal to N, adapted to
// search for nibbles by ryg (thanks!).    https://graphics.stanford.edu/~seander/bithacks.html

//go:inline
func uint32HasZeroNibble(v uint32) uint32 {
	// https://mastodon.gamedev.place/@rygorous/112621911878969502
	return (v - 0x11111111) & ^v & 0x88888888
}

//go:inline
func uint32HasNibble(v uint32, b uint8) uint32 {
	return uint32HasZeroNibble(v ^ (0x11111111 * uint32(b&0xf)))
}

// Set a specified key to a given value. Grow the map if necessary.
//
//go:nobounds
func hashmapSet(m *hashmap, key unsafe.Pointer, value unsafe.Pointer, hash uint32) {
	var bucketWithEmptySlot *hashmapBucket
	var emptySlot uint8

	tophash, nibblehash := shrinkHash(hash)

	// See whether the key already exists somewhere.
	for bucket := m.bucket[tophash&7]; bucket != nil; bucket = bucket.next {
		if bucketWithEmptySlot == nil && bucket.usedSlots != 0xff {
			// Found an empty slot, store it for if we couldn't find an
			// existing slot.  (We grow the hash table if this loop ends
			// without reaching this place.)
			if bucket.usedSlots == 0 {
				emptySlot = 0
			} else {
				emptySlot = uint8(bits.TrailingZeros8(^bucket.usedSlots))
			}
			bucketWithEmptySlot = bucket
		}

		hasNibbleHash := uint32HasNibble(bucket.nibbleHashes, nibblehash)
		for hasNibbleHash != 0 {
			index := bits.TrailingZeros32(hasNibbleHash)
			slot := uint8(index >> 2)

			// Is this an existing key whose value we're replacing?
			if bucket.usedSlots&1<<slot == 0 || !m.keyEqual(key, hashmapSlotKey(m, bucket, slot), m.keySize) {
				// No, just a collision. Try the next bucket that matches this tophash, if possible.
				hasNibbleHash ^= 1 << index
				continue
			}

			// Found it! Update the value.
			memcpy(hashmapSlotValue(m, bucket, slot), value, m.valueSize)
			return
		}
	}

	if bucketWithEmptySlot == nil {
		// No space for this item, so make another bucket.
		bucketWithEmptySlot = hashmapNewBucket(m)
		bucketWithEmptySlot.next = m.bucket[tophash&7]
		m.bucket[tophash&7] = bucketWithEmptySlot
		emptySlot = 0
	}

	println("insert slot:", emptySlot)

	m.count++
	memcpy(hashmapSlotKey(m, bucketWithEmptySlot, emptySlot), key, m.keySize)
	memcpy(hashmapSlotValue(m, bucketWithEmptySlot, emptySlot), value, m.valueSize)
	bucketWithEmptySlot.nibbleHashes |= uint32(nibblehash) << (emptySlot << 2)
	bucketWithEmptySlot.usedSlots |= 1 << emptySlot
}

func hashmapSetUnsafePointer(m unsafe.Pointer, key unsafe.Pointer, value unsafe.Pointer, hash uint32) {
	hashmapSet((*hashmap)(m), key, value, hash)
}

// Get the value of a specified key, or zero the value if not found.
//
//go:nobounds
func hashmapGet(m *hashmap, key, value unsafe.Pointer, valueSize uintptr, hash uint32) bool {
	if m == nil {
		// Getting a value out of a nil map is valid. From the spec:
		// > if the map is nil or does not contain such an entry, a[x] is the
		// > zero value for the element type of M
		memzero(value, uintptr(valueSize))
		return false
	}

	tophash, nibblehash := shrinkHash(hash)

	// Try to find the key.
	for bucket := m.bucket[tophash&7]; bucket != nil; bucket = bucket.next {
		hasNibbleHash := uint32HasNibble(bucket.nibbleHashes, nibblehash)
		for hasNibbleHash != 0 {
			index := bits.TrailingZeros32(hasNibbleHash)
			slot := uint8(index >> 2)

			// Is this what we're looking for?
			if bucket.usedSlots&1<<slot == 0 || !m.keyEqual(key, hashmapSlotKey(m, bucket, slot), m.keySize) {
				// It's not, try another slot in this bucket that might have a key with
				// the same tophash.
				hasNibbleHash ^= 1 << index
				continue
			}

			// Found the key, copy it.
			memcpy(value, hashmapSlotValue(m, bucket, slot), m.valueSize)
			return true
		}
	}

	// Did not find the key.
	memzero(value, m.valueSize)
	return false
}

func hashmapGetUnsafePointer(m unsafe.Pointer, key, value unsafe.Pointer, valueSize uintptr, hash uint32) bool {
	return hashmapGet((*hashmap)(m), key, value, valueSize, hash)
}

// Delete a given key from the map. No-op when the key does not exist in the
// map.
//
//go:nobounds
func hashmapDelete(m *hashmap, key unsafe.Pointer, hash uint32) {
	if m == nil {
		// The delete builtin is defined even when the map is nil. From the spec:
		// > If the map m is nil or the element m[k] does not exist, delete is a
		// > no-op.
		return
	}

	tophash, nibblehash := shrinkHash(hash)

	// Try to find the key.
	for bucket := m.bucket[tophash&7]; bucket != nil; bucket = bucket.next {
		hasNibbleHash := uint32HasNibble(bucket.nibbleHashes, nibblehash)
		for hasNibbleHash != 0 {
			index := bits.TrailingZeros32(hasNibbleHash)
			slot := uint8(index >> 2)

			if bucket.usedSlots&1<<slot == 0 {
				hasNibbleHash ^= 1 << index
				continue
			}

			slotKey := hashmapSlotKey(m, bucket, slot)
			// This could be the key we're looking for.
			if !m.keyEqual(key, slotKey, m.keySize) {
				hasNibbleHash ^= 1 << index
				continue
			}

			// Found the key, delete it.

			println("delete slot:", slot)

			bucket.usedSlots ^= 1 << slot
			bucket.nibbleHashes &= ^(uint32(0xf) << (slot << 2))
			// FIXME: put this bucket back into the pool if bucket.nibbleHashes becomes 0

			// Zero out the key and value so garbage collector doesn't pin the allocations.
			memzero(slotKey, m.keySize)
			slotValue := hashmapSlotValue(m, bucket, slot)
			memzero(slotValue, m.valueSize)

			m.count--
			return
		}
	}
}

// Iterate over a hashmap.
//
//go:nobounds
func hashmapNext(m *hashmap, it *hashmapIterator, key, value unsafe.Pointer) bool {
	if m == nil {
		// From the spec: If the map is nil, the number of iterations is 0.
		return false
	}

again:
	for it.bucket == nil {
		it.bucket = m.bucket[it.bucketIndex]
		if it.bucket == nil {
			it.bucketIndex = (it.bucketIndex + 1) & 7
			if it.bucketIndex == 0 {
				return false
			}
		} else {
			it.usedSlots = it.bucket.usedSlots
			break
		}
	}

	for it.usedSlots == 0 {
		it.bucket = it.bucket.next
		if it.bucket == nil {
			it.bucketIndex = (it.bucketIndex + 1) & 7
			if it.bucketIndex == 0 {
				return false
			}
			goto again
		}
		it.usedSlots = it.bucket.usedSlots
	}

	slot := bits.TrailingZeros8(it.usedSlots)
	it.usedSlots ^= 1 << slot

	memcpy(key, hashmapSlotKey(m, it.bucket, uint8(slot)), m.keySize)
	memcpy(value, hashmapSlotValue(m, it.bucket, uint8(slot)), m.valueSize)

	return true
}

func hashmapNextUnsafePointer(m unsafe.Pointer, it unsafe.Pointer, key, value unsafe.Pointer) bool {
	return hashmapNext((*hashmap)(m), (*hashmapIterator)(it), key, value)
}

//go:inline
func hashmapHash32(key unsafe.Pointer, n uintptr) uint32 {
	return hash32(key, n, randomSeed)
}

// Hashmap with plain binary data keys (not containing strings etc.).
func hashmapBinarySet(m *hashmap, key, value unsafe.Pointer) {
	if m == nil {
		nilMapPanic()
	}
	hash := hashmapHash32(key, m.keySize)
	hashmapSet(m, key, value, hash)
}

func hashmapBinarySetUnsafePointer(m unsafe.Pointer, key, value unsafe.Pointer) {
	hashmapBinarySet((*hashmap)(m), key, value)
}

func hashmapBinaryGet(m *hashmap, key, value unsafe.Pointer, valueSize uintptr) bool {
	if m == nil {
		memzero(value, uintptr(valueSize))
		return false
	}
	hash := hashmapHash32(key, m.keySize)
	return hashmapGet(m, key, value, valueSize, hash)
}

func hashmapBinaryGetUnsafePointer(m unsafe.Pointer, key, value unsafe.Pointer, valueSize uintptr) bool {
	return hashmapBinaryGet((*hashmap)(m), key, value, valueSize)
}

func hashmapBinaryDelete(m *hashmap, key unsafe.Pointer) {
	if m == nil {
		return
	}
	hash := hashmapHash32(key, m.keySize)
	hashmapDelete(m, key, hash)
}

func hashmapBinaryDeleteUnsafePointer(m unsafe.Pointer, key unsafe.Pointer) {
	hashmapBinaryDelete((*hashmap)(m), key)
}

// Hashmap with string keys (a common case).

func hashmapStringEqual(x, y unsafe.Pointer, n uintptr) bool {
	return *(*string)(x) == *(*string)(y)
}

func hashmapStringHash(s string) uint32 {
	_s := (*_string)(unsafe.Pointer(&s))
	return hashmapHash32(unsafe.Pointer(_s.ptr), uintptr(_s.length))
}

func hashmapStringPtrHash(sptr unsafe.Pointer, size uintptr) uint32 {
	_s := *(*_string)(sptr)
	return hashmapHash32(unsafe.Pointer(_s.ptr), uintptr(_s.length))
}

func hashmapStringSet(m *hashmap, key string, value unsafe.Pointer) {
	if m == nil {
		nilMapPanic()
	}
	hash := hashmapStringHash(key)
	hashmapSet(m, unsafe.Pointer(&key), value, hash)
}

func hashmapStringSetUnsafePointer(m unsafe.Pointer, key string, value unsafe.Pointer) {
	hashmapStringSet((*hashmap)(m), key, value)
}

func hashmapStringGet(m *hashmap, key string, value unsafe.Pointer, valueSize uintptr) bool {
	if m == nil {
		memzero(value, uintptr(valueSize))
		return false
	}
	hash := hashmapStringHash(key)
	return hashmapGet(m, unsafe.Pointer(&key), value, valueSize, hash)
}

func hashmapStringGetUnsafePointer(m unsafe.Pointer, key string, value unsafe.Pointer, valueSize uintptr) bool {
	return hashmapStringGet((*hashmap)(m), key, value, valueSize)
}

func hashmapStringDelete(m *hashmap, key string) {
	if m == nil {
		return
	}
	hash := hashmapStringHash(key)
	hashmapDelete(m, unsafe.Pointer(&key), hash)
}

func hashmapStringDeleteUnsafePointer(m unsafe.Pointer, key string) {
	hashmapStringDelete((*hashmap)(m), key)
}

// Hashmap with interface keys (for everything else).

// This is a method that is intentionally unexported in the reflect package. It
// is identical to the Interface() method call, except it doesn't check whether
// a field is exported and thus allows circumventing the type system.
// The hash function needs it as it also needs to hash unexported struct fields.
//
//go:linkname valueInterfaceUnsafe reflect.valueInterfaceUnsafe
func valueInterfaceUnsafe(v reflect.Value) interface{}

func hashmapFloat32Hash(ptr unsafe.Pointer) uint32 {
	f := *(*uint32)(ptr)
	if f == 0x80000000 {
		// convert -0 to 0 for hashing
		f = 0
	}
	return hashmapHash32(unsafe.Pointer(&f), 4)
}

func hashmapFloat64Hash(ptr unsafe.Pointer) uint32 {
	f := *(*uint64)(ptr)
	if f == 0x8000000000000000 {
		// convert -0 to 0 for hashing
		f = 0
	}
	return hashmapHash32(unsafe.Pointer(&f), 8)
}

func hashmapInterfaceHash(itf interface{}) uint32 {
	x := reflect.ValueOf(itf)
	if x.RawType() == nil {
		return 0 // nil interface
	}

	value := (*_interface)(unsafe.Pointer(&itf)).value
	ptr := value
	if x.RawType().Size() <= unsafe.Sizeof(uintptr(0)) {
		// Value fits in pointer, so it's directly stored in the pointer.
		ptr = unsafe.Pointer(&value)
	}

	switch x.RawType().Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return hashmapHash32(ptr, x.RawType().Size())
	case reflect.Bool, reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return hashmapHash32(ptr, x.RawType().Size())
	case reflect.Float32:
		// It should be possible to just has the contents. However, NaN != NaN
		// so if you're using lots of NaNs as map keys (you shouldn't) then hash
		// time may become exponential. To fix that, it would be better to
		// return a random number instead:
		// https://research.swtch.com/randhash
		return hashmapFloat32Hash(ptr)
	case reflect.Float64:
		return hashmapFloat64Hash(ptr)
	case reflect.Complex64:
		rptr, iptr := ptr, unsafe.Add(ptr, 4)
		return hashmapFloat32Hash(rptr) ^ hashmapFloat32Hash(iptr)
	case reflect.Complex128:
		rptr, iptr := ptr, unsafe.Add(ptr, 8)
		return hashmapFloat64Hash(rptr) ^ hashmapFloat64Hash(iptr)
	case reflect.String:
		return hashmapStringHash(x.String())
	case reflect.Chan, reflect.Ptr, reflect.UnsafePointer:
		// It might seem better to just return the pointer, but that won't
		// result in an evenly distributed hashmap. Instead, hash the pointer
		// like most other types.
		return hashmapHash32(ptr, x.RawType().Size())
	case reflect.Array:
		var hash uint32
		for i := 0; i < x.Len(); i++ {
			hash ^= hashmapInterfaceHash(valueInterfaceUnsafe(x.Index(i)))
		}
		return hash
	case reflect.Struct:
		var hash uint32
		for i := 0; i < x.NumField(); i++ {
			hash ^= hashmapInterfaceHash(valueInterfaceUnsafe(x.Field(i)))
		}
		return hash
	default:
		runtimePanic("comparing un-comparable type")
		return 0 // unreachable
	}
}

func hashmapInterfacePtrHash(iptr unsafe.Pointer, size uintptr) uint32 {
	_i := *(*interface{})(iptr)
	return hashmapInterfaceHash(_i)
}

func hashmapInterfaceEqual(x, y unsafe.Pointer, n uintptr) bool {
	return *(*interface{})(x) == *(*interface{})(y)
}

func hashmapInterfaceSet(m *hashmap, key interface{}, value unsafe.Pointer) {
	if m == nil {
		nilMapPanic()
	}
	hash := hashmapInterfaceHash(key)
	hashmapSet(m, unsafe.Pointer(&key), value, hash)
}

func hashmapInterfaceSetUnsafePointer(m unsafe.Pointer, key interface{}, value unsafe.Pointer) {
	hashmapInterfaceSet((*hashmap)(m), key, value)
}

func hashmapInterfaceGet(m *hashmap, key interface{}, value unsafe.Pointer, valueSize uintptr) bool {
	if m == nil {
		memzero(value, uintptr(valueSize))
		return false
	}
	hash := hashmapInterfaceHash(key)
	return hashmapGet(m, unsafe.Pointer(&key), value, valueSize, hash)
}

func hashmapInterfaceGetUnsafePointer(m unsafe.Pointer, key interface{}, value unsafe.Pointer, valueSize uintptr) bool {
	return hashmapInterfaceGet((*hashmap)(m), key, value, valueSize)
}

func hashmapInterfaceDelete(m *hashmap, key interface{}) {
	if m == nil {
		return
	}
	hash := hashmapInterfaceHash(key)
	hashmapDelete(m, unsafe.Pointer(&key), hash)
}

func hashmapInterfaceDeleteUnsafePointer(m unsafe.Pointer, key interface{}) {
	hashmapInterfaceDelete((*hashmap)(m), key)
}
