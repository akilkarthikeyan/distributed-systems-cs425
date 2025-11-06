// ring.go
// This file builds and queries the "consistent hashing ring" used for placement.
//
// IMPORTANT IDEA (simple words):
// - Each running process announces itself to membership as "IP:port:epoch".
//   That identity changes on every reboot (new epoch).
// - But for PLACEMENT on the ring, we want stability. So we hash ONLY "IP:port".
//   That keeps a machine in the same spot on the ring after a restart.
//   Result: when a node comes back, rebalancing naturally brings its files back
//   to the same range, with minimal data movement.

package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// ID is a 160-bit SHA-1 digest (20 bytes). We use it to order things on the ring.
type ID [20]byte

// sha1ID returns the SHA-1 hash of a string as our 20-byte ID.
// We always treat the bytes as big-endian so lexicographic compare == numeric compare.
func sha1ID(s string) ID {
	sum := sha1.Sum([]byte(s))
	return sum // [20]byte
}

// compare160 compares two IDs as big-endian byte arrays.
// Returns -1 if a<b, 0 if a==b, +1 if a>b.
func compare160(a, b ID) int {
	return bytes.Compare(a[:], b[:])
}

// shortID prints the low 16 bits of an ID (for human-friendly logs only).
// NOTE: Never use this for ordering/placement; it’s just a tiny label.
func shortID(id ID) string {
	return hex.EncodeToString(id[len(id)-2:])
}

// Membership identity (in your server.go) is "IP:port:epoch".
// That changes every reboot (good for fencing, shows it's a fresh process).
//
// Ring token (here) is the stable "IP:port" hash.
// That stays the same across reboots (good for keeping placement steady).

// ringTokenID returns the STABLE placement token: SHA1("IP:port").
// We do NOT include epoch here on purpose.
func ringTokenID(ip string, port int) ID {
	return sha1ID(fmt.Sprintf("%s:%d", ip, port))
}

// makeFileID hashes a filename to place it on the ring.
// We trim whitespace so accidental spaces don’t move the file.
func makeFileID(filename string) ID {
	return sha1ID(strings.TrimSpace(filename))
}

// Ring is an immutable snapshot of the cluster ordered by ring token.
// All slices are parallel (same length, aligned by index).
type Ring struct {
	IDs   []ID     // ring tokens = SHA1("IP:port") in ascending order
	Keys  []string // membership keys, e.g., "IP:port:epoch" (useful for logs)
	Nodes []Member // full Member records (IP, Port, Epoch/Timestamp, Status, etc.)
}

// Len returns how many entries are currently on the ring.
func (r *Ring) Len() int {
	return len(r.IDs)
}

// UpperBound returns the smallest index i with IDs[i] >= target.
// If all IDs are < target, it returns Len() (caller can wrap to 0).
// Think: "first place on the ring at or after this target ID".
func (r *Ring) UpperBound(target ID) int {
	lo, hi := 0, len(r.IDs)
	for lo < hi {
		mid := (lo + hi) / 2
		if compare160(r.IDs[mid], target) >= 0 {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	return lo
}

// Predecessor returns the index of the node immediately before idx, wrapping around.
// Useful when you want the primary interval (pred, self].
func (r *Ring) Predecessor(idx int) int {
	if r.Len() == 0 {
		return -1
	}
	if idx == 0 {
		return r.Len() - 1
	}
	return idx - 1
}

// DebugString returns a readable dump of the ring order.
// It prints the full membership key (IP:port:epoch) and a short hash tag.
func (r *Ring) DebugString() string {
	if r == nil || r.Len() == 0 {
		return "(ring empty)"
	}
	var b strings.Builder
	b.WriteString("Ring order (by stable token SHA1(\"IP:port\")):\n")
	for i := range r.Nodes {
		m := r.Nodes[i]
		b.WriteString(fmt.Sprintf("  %s  [token:%s]\n", keyFor(m), shortID(r.IDs[i])))
	}
	return b.String()
}

//
// ─────────────────────────────── Building the ring ────────────────────────────
//

// BuildRing builds a sorted ring snapshot from the current membership map.
// - members: map from membership key -> Member (this is what you already have).
// - omitFailed: if true, we skip entries marked Failed (they shouldn’t hold data).
//
// Steps (simple):
// 1) For each alive member, compute token = SHA1("IP:port").
// 2) Sort by token ascending to get the clockwise order.
// 3) Store parallel slices so lookups are fast and simple.
func BuildRing(members map[string]Member, omitFailed bool) *Ring {
	type row struct {
		id   ID
		key  string
		node Member
	}

	rows := make([]row, 0, len(members))
	for k, m := range members {
		if omitFailed && m.Status == Failed {
			continue // failed nodes are not part of placement
		}
		rows = append(rows, row{
			id:   ringTokenID(m.IP, m.Port), // STABLE: based only on IP:port
			key:  k,                         // membership key "IP:port:epoch"
			node: m,
		})
	}

	// Sort clockwise by the token so the ring is deterministic.
	sort.Slice(rows, func(i, j int) bool { return compare160(rows[i].id, rows[j].id) < 0 })

	r := &Ring{
		IDs:   make([]ID, len(rows)),
		Keys:  make([]string, len(rows)),
		Nodes: make([]Member, len(rows)),
	}
	for i := range rows {
		r.IDs[i] = rows[i].id
		r.Keys[i] = rows[i].key
		r.Nodes[i] = rows[i].node
	}
	return r
}

// OwnersForFile returns the n owners (primary + successors) for a file name,
// simply walking clockwise from the file’s position.
// This version allows multiple owners on the same IP if your cluster has
// multiple processes on one VM. If you want "distinct VMs", use the function below.
func (r *Ring) OwnersForFile(filename string, n int) []Member {
	if r.Len() == 0 || n <= 0 {
		return nil
	}
	fid := makeFileID(filename) // where the file "lands" on the ring
	i := r.UpperBound(fid)      // first node with token >= file ID
	if i == r.Len() {
		i = 0
	} // wrap to start if we passed the end
	out := make([]Member, 0, min(n, r.Len()))
	for k := 0; k < n && k < r.Len(); k++ {
		out = append(out, r.Nodes[(i+k)%r.Len()])
	}
	return out
}

// OwnersForFileDistinctHosts returns up to n owners on DISTINCT IPs.
// Use this if you might run multiple nodes (processes) on the same VM
// and you want true machine-level fault tolerance.
// It skips over nodes that share an IP with an already chosen owner.
func (r *Ring) OwnersForFileDistinctHosts(filename string, n int) []Member {
	if r.Len() == 0 || n <= 0 {
		return nil
	}
	fid := makeFileID(filename)
	i := r.UpperBound(fid)
	if i == r.Len() {
		i = 0
	}

	out := make([]Member, 0, min(n, r.Len()))
	seenIP := make(map[string]struct{})
	idx := i

	// Walk clockwise until we either pick n owners or looped the whole ring.
	for len(out) < n && len(out) < r.Len() {
		m := r.Nodes[idx]
		if _, dup := seenIP[m.IP]; !dup {
			out = append(out, m)
			seenIP[m.IP] = struct{}{}
		}
		idx = (idx + 1) % r.Len()
		if idx == i { // made a full circle
			break
		}
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
