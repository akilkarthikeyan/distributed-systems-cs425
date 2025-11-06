// fileops.go
// File operations: create, get, append, merge
// File storage: chunks, manifests, helper functions

package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
)

// ============================================================================
// File Operation Types
// ============================================================================

type AppendRequest struct {
	Filename  string
	ClientID  string
	ClientSeq uint64
	Data      []byte
}

type AppendAck struct {
	Filename  string
	ClientID  string
	ClientSeq uint64
	GlobalSeq uint64
	Success   bool
}

type ChunkID struct {
	ClientID  string // Who appended
	ClientSeq uint64 // Client's per-file counter (0,1,2,...)
	GlobalSeq uint64 // Primary-assigned, file-wide (1,2,3,...)
	Checksum  uint64 // xxhash64 over bytes (for integrity)
	// LogicalClock int64  // Optional: for tracing/debugging only
}

type ChunkMeta struct {
	ID   ChunkID
	Size int64  // Bytes
	Path string // Local path: data/chunks/<fileID>/<chunkID>.data
	// Replicas field removed - truth comes from ring + VersionVector
}

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

// ============================================================================
// Append Operations
// ============================================================================

func handleAppendRequest(conn *net.UDPConn, hyDFSFile string, data []byte) error {
	RingMutex.Lock()
	ring := currentRing
	RingMutex.Unlock()

	if ring == nil {
		return fmt.Errorf("ring not initialized")
	}

	replicas := ring.OwnersForFileDistinctHosts(hyDFSFile, 3)
	if len(replicas) == 0 {
		return fmt.Errorf("no replicas found for file")
	}

	primary := replicas[0]

	clientSeq := getNextClientSeq(hyDFSFile)

	appendReq := AppendRequest{
		Filename:  hyDFSFile,
		ClientID:  selfId,
		ClientSeq: clientSeq,
		Data:      data,
	}

	primaryAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", primary.IP, primary.Port))

	v, _ := membershipList.Load(selfId)
	selfMember := v.(Member)

	payloadBytes, _ := json.Marshal(appendReq)
	msg := Message{
		MessageType: "Append",
		From:        &selfMember,
		Payload:     payloadBytes,
	}

	sendUDP(conn, primaryAddr, &msg)
	fmt.Printf("Append request sent to primary %s:%d\n", primary.IP, primary.Port)

	return nil
}

func handleAppendMessage(conn *net.UDPConn, msg *Message) error {
	var req AppendRequest
	if err := json.Unmarshal(msg.Payload, &req); err != nil {
		log.Printf("append unmarshal error: %v", err)
		return err
	}
	log.Printf("Received append request for file %s from client %s", req.Filename, req.ClientID)

	RingMutex.Lock()
	ring := currentRing
	RingMutex.Unlock()

	if ring == nil {
		return fmt.Errorf("ring not initialized")
	}

	replicas := ring.OwnersForFileDistinctHosts(req.Filename, 3)
	if len(replicas) == 0 || replicas[0].IP != selfHost || replicas[0].Port != SelfPort {
		primaryAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", replicas[0].IP, replicas[0].Port))
		sendUDP(conn, primaryAddr, msg)
		return nil
	}

	manifest := loadFileManifest(req.Filename)

	if req.ClientSeq <= manifest.HighestSeq[req.ClientID] {
		log.Printf("Duplicate append request, ignoring")
		return nil
	}

	globalSeq := uint64(len(manifest.Chunks)) + 1

	chunkID := ChunkID{
		ClientID:  req.ClientID,
		ClientSeq: req.ClientSeq,
		GlobalSeq: globalSeq,
		Checksum:  0, // TODO: implement xxhash64 or use crypto/sha256
	}

	chunkPath := storeChunk(req.Filename, chunkID, req.Data)
	manifest.Chunks = append(manifest.Chunks, ChunkMeta{
		ID:   chunkID,
		Size: int64(len(req.Data)),
		Path: chunkPath,
	})

	manifest.HighestSeq[req.ClientID] = req.ClientSeq
	manifest.ManifestVersion++

	saveFileManifest(req.Filename, manifest)

	log.Printf("Append successful: GlobalSeq=%d, ClientSeq=%d", globalSeq, req.ClientSeq)

	replicateChunk(conn, req.Filename, chunkID, req.Data, replicas[1:])

	ack := AppendAck{
		Filename:  req.Filename,
		ClientID:  req.ClientID,
		ClientSeq: req.ClientSeq,
		GlobalSeq: globalSeq,
		Success:   true,
	}

	v, _ := membershipList.Load(selfId)
	selfMember := v.(Member)

	ackBytes, _ := json.Marshal(ack)
	reply := Message{
		MessageType: "AppendAck",
		From:        &selfMember,
		Payload:     ackBytes,
	}

	clientAddr, _ := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", msg.From.IP, msg.From.Port))
	sendUDP(conn, clientAddr, &reply)

	fmt.Printf("Append completed: file=%s, GlobalSeq=%d\n", req.Filename, globalSeq)

	return nil
}

// ============================================================================
// Helper Functions (to be implemented)
// ============================================================================

// All file-related helper functions:
// - getNextClientSeq()
// - loadFileManifest()
// - saveFileManifest()
// - storeChunk()
// - replicateChunk()
// - handleAppendReplicate()
// - handleCreateRequest()
// - handleGetRequest()
// - handleMergeRequest()

var clientSequences sync.Map
var fileManifests sync.Map

func getNextClientSeq(filename string) uint64 {
	key := fmt.Sprintf("%s:%s", filename, selfId)

	// Load existing sequence
	val, exists := clientSequences.Load(key)

	if !exists {
		// First time this client appends to this file
		clientSequences.Store(key, uint64(1))
		return 1
	}

	// Increment and store
	nextSeq := val.(uint64) + 1
	clientSequences.Store(key, nextSeq)
	return nextSeq
}

func loadFileManifest(filename string) FileManifest {
	// Step 1: Get the file ID (SHA1 hash of filename)
	// makeFileID() returns ID type which is [20]byte
	fileIDBytes := makeFileID(filename)

	// Step 2: Convert bytes to hex string (for use in filename)
	// Example: [0xa1, 0xb2, ...] becomes "a1b2..."
	fileIDHex := hex.EncodeToString(fileIDBytes[:])

	// Step 3: Build the path where manifest should be stored
	// Example: "data/manifests/a1b2c3d4e5f6...json"
	manifestPath := fmt.Sprintf("data/manifests/%s.json", fileIDHex)

	// Step 4: Try to read the file from disk
	data, err := os.ReadFile(manifestPath)

	// Step 5: If file doesn't exist, return empty manifest
	if err != nil {
		return FileManifest{
			FileID:          fileIDHex,
			Filename:        filename,
			ManifestVersion: 0,
			Chunks:          []ChunkMeta{},                      // Empty list
			HighestSeq:      make(map[string]uint64),            // Empty map
			VersionVector:   make(map[string]map[string]uint64), // Empty map
			Replicas:        []string{},                         // Empty list
		}
	}

	// Step 6: Parse JSON into FileManifest struct
	var manifest FileManifest
	json.Unmarshal(data, &manifest)

	// Step 7: Make sure maps are initialized (JSON might have null)
	if manifest.HighestSeq == nil {
		manifest.HighestSeq = make(map[string]uint64)
	}

	// Step 8: Return the loaded manifest
	return manifest
}

func saveFileManifest(filename string, manifest FileManifest) error {
	// Step 1: Get the file ID (same as in loadFileManifest)
	fileIDBytes := makeFileID(filename)
	fileIDHex := hex.EncodeToString(fileIDBytes[:])

	// Step 2: Make sure the manifest has correct FileID and Filename
	manifest.FileID = fileIDHex
	manifest.Filename = filename

	// Step 3: Create directory if it doesn't exist
	// MkdirAll creates all parent directories too
	manifestDir := "data/manifests"
	os.MkdirAll(manifestDir, 0755) // 0755 = rwxr-xr-x permissions

	// Step 4: Build the file path
	manifestPath := fmt.Sprintf("%s/%s.json", manifestDir, fileIDHex)

	// Step 5: Convert manifest struct to JSON
	// MarshalIndent makes it pretty (with indentation)
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to convert manifest to JSON: %v", err)
	}

	// Step 6: Write JSON to disk
	// 0644 = rw-r--r-- permissions (readable by all, writable by owner)
	err = os.WriteFile(manifestPath, data, 0644)
	if err != nil {
		return fmt.Errorf("failed to write manifest file: %v", err)
	}

	return nil
}
