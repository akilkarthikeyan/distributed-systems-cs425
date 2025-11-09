package main

import (
	"encoding/json"
	"time"
)

// Status represents the current state of a node in the membership protocol.
type Status string

const (
	Alive  Status = "alive"
	Failed Status = "failed"
)

type MessageType string

const (
	Gossip             MessageType = "gossip"
	JoinReq            MessageType = "join-req"
	JoinReply          MessageType = "join-reply"
	CreateHyDFSFile    MessageType = "create-hydfs-file"
	AppendHyDFSFile    MessageType = "append-hydfs-file"
	MergeHyDFSFile     MessageType = "merge-hydfs-file"
	GetChunksHyDFSFile MessageType = "get-chunks-hydfs-file"
)

type ACKType string

const (
	ACK  ACKType = "ack"
	NACK ACKType = "nack"
)

// Member represents a node in the distributed system.
// Contains both membership protocol information and ring placement data.
type Member struct {
	// Gossip-related fields
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Timestamp   string `json:"timestamp"`
	RingID      uint64 `json:"ringId"`
	Heartbeat   int    `json:"heartbeat"`
	LastUpdated int    `json:"lastUpdated"`
	Status      Status `json:"status"`
}

// Message is the base structure for all network messages.
// Messages can be sent over UDP (membership) or TCP (file operations).
type Message struct {
	MessageType MessageType     `json:"messageType"`
	From        *Member         `json:"self,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

// GossipPayload contains membership list information exchanged during gossip protocol.
// Map of membership key -> Member
type GossipPayload struct {
	Members map[string]Member `json:"Members"`
}

// FilePayload represents a file chunk being transmitted over the network.
// DataB64 contains the base64-encoded chunk data, ID is a unique chunk identifier.
type FilePayload struct {
	Filename string `json:"filename"`
	DataB64  string `json:"dataB64"`
	ID       string `json:"id"` // chunk ID
}

const (
	SelfPort       = 1234
	IntroducerHost = "fa25-cs425-9501.cs.illinois.edu"
	IntroducerPort = 1234
	Tfail          = 5
	Tcleanup       = 5
	K              = 3
	TimeUnit       = time.Second * 5
)

// HyDFSFile represents a file stored in HyDFS with its metadata.
// Chunks array contains FilePayload entries; DataB64 is populated only during network transmission.
type HyDFSFile struct {
	Filename               string
	HyDFSCompliantFilename string        // without slashes
	Chunks                 []FilePayload // will contain DataB64 only during transport
}

// ID is a 160-bit SHA-1 digest (20 bytes). Used for consistent file hashing and ring placement.
type ID [20]byte

// Ring represents a consistent hash ring for file placement.
// All three slices are parallel arrays, sorted by ring token (IDs) in ascending order.
type Ring struct {
	IDs   []ID     // ring tokens = SHA1("IP:port") in ascending order
	Keys  []string // membership keys, e.g., "IP:port:timestamp" (useful for logs)
	Nodes []Member // full Member records (IP, Port, Timestamp, Status, etc.)
}

// ChunkID represents a chunk identifier with client and global sequencing.
// Useful for implementing per-client ordering and global ordering.
type ChunkID struct {
	ClientID  string // Who appended
	ClientSeq uint64 // Client's per-file counter (0,1,2,...)
	GlobalSeq uint64 // Primary-assigned, file-wide (1,2,3,...)
	Checksum  uint64 // xxhash64 over bytes (for integrity)
}

// ChunkMeta contains metadata about a stored chunk.
type ChunkMeta struct {
	ID   ChunkID
	Size int64  // Bytes
	Path string // Local path: data/chunks/<fileID>/<chunkID>.data
}

// ChunksPayload contains chunk metadata for a file (used in GetChunksHyDFSFile requests).
type ChunksPayload struct {
	Filename string        `json:"filename"`
	Chunks   []FilePayload `json:"chunks"` // List of chunk IDs
}

// MergePayload contains ordered chunks with data for merging file versions across replicas.
type MergePayload struct {
	Filename string        `json:"filename"`
	Chunks   []FilePayload `json:"chunks"` // Ordered list of chunks with data
}

// FileManifest represents comprehensive metadata for a file.
// Useful for advanced features like version vectors and anti-entropy.
type FileManifest struct {
	FileID          string      // SHA1(filename)
	Filename        string      // Human-readable name
	ManifestVersion uint64      // Bump on each write/merge
	PrimaryReplica  string      // Node token of current primary
	Replicas        []string    // N owner tokens (for debugging)
	Chunks          []ChunkMeta // Ordered by GlobalSeq
	// Per-client tracking
	HighestSeq map[string]uint64 // clientID -> last acked ClientSeq
	// Anti-entropy tracking
	VersionVector map[string]map[string]uint64 // replica -> clientID -> last seq
}

// AppendRequest represents a request to append data to a file.
// Useful for implementing more sophisticated append operations.
type AppendRequest struct {
	Filename  string
	ClientID  string
	ClientSeq uint64
	Data      []byte
}

// AppendAck represents an acknowledgment of an append operation.
type AppendAck struct {
	Filename  string
	ClientID  string
	ClientSeq uint64
	GlobalSeq uint64
	Success   bool
}
