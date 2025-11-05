package main

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
)

func KeyFor(m Member) string { return fmt.Sprintf("%s:%d:%s", m.IP, m.Port, m.Timestamp) }

func GetRingId(s string) uint64 {
	sum := sha1.Sum([]byte(s))              // 160-bit hash (20 bytes)
	return binary.BigEndian.Uint64(sum[:8]) // take the first 8 bytes as a 64-bit number
}

func SnapshotMembers(omitFailed bool) map[string]Member {
	out := make(map[string]Member)
	membershipList.Range(func(k, v any) bool {
		if omitFailed && v.(Member).Status == Failed {
			return true
		}
		out[k.(string)] = v.(Member)
		return true
	})
	return out
}

func SelectKMembers(members map[string]Member, k int) []Member {
	slice := make([]Member, 0, len(members))
	for _, m := range members {
		slice = append(slice, m)
	}

	// Fewer than k members
	if len(slice) <= k {
		return slice
	}

	rand.Shuffle(len(slice), func(i, j int) {
		slice[i], slice[j] = slice[j], slice[i]
	})

	return slice[:k]
}
