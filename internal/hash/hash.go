// Package hash places a key on the number line that the ranges cover.
//
// The line runs from 0 up to, but not including, Space. A key's place is a
// 32-bit FNV-1a hash of the tenant, a zero byte, and the key. The zero byte
// keeps tenant "ab" + key "c" distinct from tenant "a" + key "bc".
package hash

// Space is the count of places on the line. The last valid hash is Space-1.
const Space int64 = 1 << 32

// Key returns the place of this tenant and key, in the range 0 .. Space-1.
func Key(tenant, key string) uint32 {
	const (
		offset = 2166136261
		prime  = 16777619
	)
	h := uint32(offset)
	h = mix(h, tenant)
	h ^= 0
	h *= prime
	h = mix(h, key)
	return h
}

func mix(h uint32, s string) uint32 {
	const prime = 16777619
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= prime
	}
	return h
}

// Parts cuts [0, Space) into n half-open spans that meet and cover the line.
// The last span takes any remainder, so nothing is left over.
func Parts(n int) [][2]int64 {
	if n < 1 {
		return nil
	}
	base := Space / int64(n)
	out := make([][2]int64, n)
	var start int64
	for i := 0; i < n; i++ {
		end := start + base
		if i == n-1 {
			end = Space
		}
		out[i] = [2]int64{start, end}
		start = end
	}
	return out
}
