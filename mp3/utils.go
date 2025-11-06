package main

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/google/uuid"
)

func GetUUID() string {
	return strings.ReplaceAll(uuid.New().String(), "-", "")
}

func KeyFor(m Member) string { return fmt.Sprintf("%s:%d:%s", m.IP, m.Port, m.Timestamp) }

func GetRingId(s string) uint64 {
	sum := sha1.Sum([]byte(s))              // 160-bit hash (20 bytes)
	return binary.BigEndian.Uint64(sum[:8]) // take the first 8 bytes as a 64-bit number
}

func GetRingSuccessor(ringId uint64) Member {
	var successor Member
	minDiff := ^uint64(0) // max uint64

	MembershipList.Range(func(_, v any) bool {
		m := v.(Member)
		if m.Status != Alive || m.RingID == ringId {
			return true
		}

		diff := m.RingID - ringId // wraps automatically for uint64
		if diff < minDiff {
			minDiff = diff
			successor = m
		}
		return true
	})

	return successor
}

func SnapshotMembers(omitFailed bool) map[string]Member {
	out := make(map[string]Member)
	MembershipList.Range(func(k, v any) bool {
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

func EncodeFileToBase64(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(data), nil
}

func DecodeBase64ToBytes(dataB64 string) ([]byte, error) {
	data, err := base64.StdEncoding.DecodeString(dataB64)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func GetHyDFSCompliantFilename(filename string) string {
	return strings.ReplaceAll(filename, "/", "_")
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// MakeFileID hashes a filename to get a consistent file ID.
// Trims whitespace so accidental spaces don't move the file.
// Returns the full 20-byte SHA-1 hash.
func MakeFileID(filename string) ID {
	return sha1ID(strings.TrimSpace(filename))
}

// sha1ID returns the SHA-1 hash of a string as a 20-byte ID.
func sha1ID(s string) ID {
	sum := sha1.Sum([]byte(s))
	return sum // [20]byte
}

// FileIDToHex converts a file ID to a hex string (useful for directory names).
func FileIDToHex(id ID) string {
	return hex.EncodeToString(id[:])
}

// ShortID prints the low 16 bits of an ID (for human-friendly logs/debugging only).
// NOTE: Never use this for ordering/placement; it's just a tiny label.
func ShortID(id ID) string {
	return hex.EncodeToString(id[len(id)-2:])
}

// ============================================================================
// File Path Utilities
// ============================================================================

// GetFileChunkPath returns the full path for a file chunk.
// Format: hydfs/<compliant_filename>_<chunkID>
func GetFileChunkPath(filename string, chunkID string) string {
	compliantName := GetHyDFSCompliantFilename(filename)
	return filepath.Join("hydfs", fmt.Sprintf("%s_%s", compliantName, chunkID))
}

// EnsureHyDFSDir ensures the hydfs directory exists.
func EnsureHyDFSDir() error {
	return os.MkdirAll("hydfs", 0755)
}

// Len returns how many entries are currently on the ring.
func (r *Ring) Len() int {
	return len(r.IDs)
}

// compare160 compares two IDs as big-endian byte arrays.
// Returns -1 if a<b, 0 if a==b, +1 if a>b.
func compare160(a, b ID) int {
	return bytes.Compare(a[:], b[:])
}

func RingTokenID(ip string, port int) ID {
	return sha1ID(fmt.Sprintf("%s:%d", ip, port))
}

// BuildRing builds a sorted ring snapshot from the current membership map.
// - members: map from membership key -> Member (this is what you already have).
// - omitFailed: if true, we skip entries marked Failed (they shouldn't hold data).
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
			id:   RingTokenID(m.IP, m.Port), // STABLE: based only on IP:port
			key:  k,                         // membership key "IP:port:timestamp"
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
// It prints the full membership key (IP:port:timestamp) and a short hash tag.
func (r *Ring) DebugString() string {
	if r == nil || r.Len() == 0 {
		return "(ring empty)"
	}
	var b strings.Builder
	b.WriteString("Ring order (by stable token SHA1(\"IP:port\")):\n")
	for i := range r.Nodes {
		m := r.Nodes[i]
		b.WriteString(fmt.Sprintf("  %s  [token:%s]\n", KeyFor(m), ShortID(r.IDs[i])))
	}
	return b.String()
}

// OwnersForFile returns the n owners (primary + successors) for a file name,
// simply walking clockwise from the file's position.
// This version allows multiple owners on the same IP if your cluster has
// multiple processes on one VM. If you want "distinct VMs", use the function below.
func (r *Ring) OwnersForFile(filename string, n int) []Member {
	if r.Len() == 0 || n <= 0 {
		return nil
	}
	fid := MakeFileID(filename) // where the file "lands" on the ring
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
	fid := MakeFileID(filename)
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
