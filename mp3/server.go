package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// Global state variables
var Tick int                // Current tick counter (incremented every TimeUnit)
var MembershipList sync.Map // Thread-safe map: membership key -> Member
var HyDFSFiles sync.Map     // Thread-safe map: filename -> HyDFSFile

var selfHost string // This node's hostname
var selfId string   // This node's membership key (IP:Port:Timestamp)

var udpConn *net.UDPConn // UDP connection for membership protocol messages

// ============================================================================
// UDP Communication (Membership Protocol)
// ============================================================================

// sendUDP sends a message over UDP for membership protocol (gossip, join requests/replies).
func sendUDP(addr *net.UDPAddr, msg *Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		fmt.Printf("send udp marshal error: %v", err)
		return
	}
	if _, err := udpConn.WriteToUDP(data, addr); err != nil {
		fmt.Printf("send udp write error: %v", err)
	}
}

// listenUDP continuously listens for incoming UDP messages and handles them (runs in goroutine).
func listenUDP() {
	buf := make([]byte, 4096)
	for {
		n, raddr, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("listen udp read error: %v", err)
			continue
		}
		var msg Message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			fmt.Printf("listen udp unmarshal from %v error: %v", raddr, err)
			continue
		}
		handleMessage(&msg, nil)
	}
}

// ============================================================================
// TCP Communication (File Operations)
// ============================================================================

// sendTCP sends a message over TCP and waits for response (for file operations requiring reliable delivery).
func sendTCP(addr string, msg *Message) (*Message, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	// Send request
	if err := encoder.Encode(msg); err != nil {
		return nil, err
	}

	// Wait for response
	var resp Message
	if err := decoder.Decode(&resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

// listenTCP accepts incoming TCP connections and spawns goroutines to handle them (runs in goroutine).
func listenTCP(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Printf("listen tcp accept error: %v", err)
			continue
		}
		// Handle each client in a separate goroutine
		go handleTCPClient(conn)
	}
}

// handleTCPClient handles a single TCP client connection, processing multiple messages until client closes.
func handleTCPClient(conn net.Conn) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				// Connection closed by client, so return
			} else {
				fmt.Printf("handle tcp decode from %v error: %v\n", conn.RemoteAddr(), err)
			}
			return
		}
		handleMessage(&msg, encoder)
	}
}

// ============================================================================
// Membership Protocol
// ============================================================================

// mergeMembershipList merges incoming membership info: adds new members, updates existing if heartbeat is higher.
func mergeMembershipList(members map[string]Member) {
	for id, m := range members {
		v, ok := MembershipList.Load(id)
		if !ok { // New member
			newMember := Member{
				IP:          m.IP,
				Port:        m.Port,
				Timestamp:   m.Timestamp,
				RingID:      m.RingID,
				Heartbeat:   m.Heartbeat,
				LastUpdated: Tick,
				Status:      m.Status,
			}
			MembershipList.Store(id, newMember)
		} else { // Existing member
			existing := v.(Member)
			if m.Heartbeat > existing.Heartbeat {
				existing.Heartbeat = m.Heartbeat
				existing.Status = m.Status
				existing.LastUpdated = Tick
				MembershipList.Store(id, existing)
			}
		}
	}
}

// gossip runs at intervals: increments own heartbeat, checks for failed nodes, sends membership list to K random members.
func gossip(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		Tick++

		// Increment self heartbeat
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		self.Status = Alive
		self.Heartbeat++
		self.LastUpdated = Tick
		MembershipList.Store(selfId, self)

		// Check for failed/suspected members
		MembershipList.Range(func(k, v any) bool {
			m := v.(Member)
			elapsed := Tick - m.LastUpdated

			// Mark as failed if no heartbeat update for Tfail ticks
			if m.Status == Alive && elapsed >= Tfail {
				m.Status = Failed
				m.LastUpdated = Tick
				MembershipList.Store(k.(string), m)
				// fmt.Printf("[FAIL] %s marked failed at tick %d\n", k.(string), tick)
			} else if m.Status == Failed && elapsed >= Tcleanup {
				// Remove failed node after Tcleanup ticks
				MembershipList.Delete(k.(string))
				// fmt.Printf("[DELETE] %s removed from membership list at tick %d\n", k.(string), tick)
			}

			return true
		})

		// Select K random members to gossip to (exclude self)
		members := SnapshotMembers(true)
		delete(members, selfId)
		targets := SelectKMembers(members, K)
		members[selfId] = self // add self back

		// Send gossip messages to selected targets
		for _, target := range targets {
			targetAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", target.IP, target.Port))
			if err != nil {
				fmt.Printf("gossip resolve target error: %v", err)
				continue
			}
			payloadBytes, _ := json.Marshal(GossipPayload{Members: members})
			msg := Message{
				MessageType: Gossip,
				From:        &self,
				Payload:     payloadBytes,
			}
			sendUDP(targetAddr, &msg)
			log.Printf("sent %s to %s", msg.MessageType, KeyFor(target))
		}
	}
}

// ============================================================================
// Message Handling
// ============================================================================

// handleMessage routes incoming messages to appropriate handlers (encoder only present for TCP messages).
func handleMessage(msg *Message, encoder *json.Encoder) {
	log.Printf("recv %s from %s", msg.MessageType, KeyFor(*msg.From))

	switch msg.MessageType {
	// UDP message - Membership protocol
	case Gossip:
		var gp GossipPayload
		if err := json.Unmarshal(msg.Payload, &gp); err != nil {
			fmt.Printf("gossip payload unmarshal error: %v", err)
			return
		}
		mergeMembershipList(gp.Members)

	// UDP message - Join request from a new node
	case JoinReq:
		// Send JoinReply with current membership list to help new node join
		members := SnapshotMembers(true)
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		payloadBytes, _ := json.Marshal(GossipPayload{Members: members})
		reply := Message{
			MessageType: JoinReply,
			From:        &self,
			Payload:     payloadBytes,
		}
		senderAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", msg.From.IP, msg.From.Port))
		if err != nil {
			fmt.Printf("resolve sender error: %v", err)
			return
		}
		sendUDP(senderAddr, &reply)
		log.Printf("sent %s to %s", reply.MessageType, KeyFor(*msg.From))

	// UDP message - Join reply received by new node
	case JoinReply:
		// Merge received membership list into local membership
		var gp GossipPayload
		if err := json.Unmarshal(msg.Payload, &gp); err != nil {
			fmt.Printf("gossip payload unmarshal error: %v", err)
			return
		}
		mergeMembershipList(gp.Members)

	// TCP message - Create a new file in HyDFS
	// Returns ACK if successful, NACK if file already exists
	case CreateHyDFSFile:
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		var fp FilePayload
		if err := json.Unmarshal(msg.Payload, &fp); err != nil {
			fmt.Printf("file payload unmarshal error: %v", err)
			return
		}

		// Check if file already exists
		_, ok := HyDFSFiles.Load(fp.Filename)
		if ok {
			payloadBytes, _ := json.Marshal(NACK)
			encoder.Encode(&Message{
				MessageType: CreateHyDFSFile,
				From:        &self,
				Payload:     payloadBytes,
			})
			return
		}

		// Store file metadata
		HyDFSFiles.Store(fp.Filename, HyDFSFile{
			Filename:               fp.Filename,
			HyDFSCompliantFilename: GetHyDFSCompliantFilename(fp.Filename),
			Chunks: []FilePayload{
				{
					Filename: fp.Filename,
					ID:       fp.ID,
				},
			},
		})

		// Decode and write chunk data to disk
		data, err := DecodeBase64ToBytes(fp.DataB64)
		if err != nil {
			fmt.Printf("file decode error: %v", err)
			return
		}

		targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(fp.Filename)+"_"+fp.ID)
		if err := os.WriteFile(targetPath, data, 0644); err != nil {
			fmt.Printf("failed to write file: %v\n", err)
			return
		}

		// Send ACK response
		payloadBytes, _ := json.Marshal(ACK)
		encoder.Encode(&Message{
			MessageType: CreateHyDFSFile,
			From:        &self,
			Payload:     payloadBytes,
		})

	// TCP message - Append a chunk to an existing file
	// Returns ACK if successful, NACK if file does not exist
	case AppendHyDFSFile:
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		var fp FilePayload
		if err := json.Unmarshal(msg.Payload, &fp); err != nil {
			fmt.Printf("file payload unmarshal error: %v", err)
			return
		}

		// Check if file exists
		w, ok := HyDFSFiles.Load(fp.Filename)
		if !ok {
			payloadBytes, _ := json.Marshal(NACK)
			encoder.Encode(&Message{
				MessageType: AppendHyDFSFile,
				From:        &self,
				Payload:     payloadBytes,
			})
			return
		}

		// Append new chunk to file metadata
		hyDFSFile := w.(HyDFSFile)
		hyDFSFile.Chunks = append(hyDFSFile.Chunks, FilePayload{
			Filename: fp.Filename,
			ID:       fp.ID,
		})
		HyDFSFiles.Store(fp.Filename, hyDFSFile)

		// Decode and write chunk data to disk
		data, err := DecodeBase64ToBytes(fp.DataB64)
		if err != nil {
			fmt.Printf("file decode error: %v", err)
			return
		}

		targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(fp.Filename)+"_"+fp.ID)
		if err := os.WriteFile(targetPath, data, 0644); err != nil {
			fmt.Printf("failed to write file: %v\n", err)
			return
		}

		// Send ACK response
		payloadBytes, _ := json.Marshal(ACK)
		encoder.Encode(&Message{
			MessageType: AppendHyDFSFile,
			From:        &self,
			Payload:     payloadBytes,
		})
	}
}

// ============================================================================
// File Operations
// ============================================================================

// writeHyDFSFile writes a file to HyDFS with replication to 3 distinct hosts (create=true for new, false for append, requires all 3 ACKs).
func writeHyDFSFile(localfilename string, hyDFSfilename string, create bool) bool {
	// Build the ring from current membership to find replica locations
	members := SnapshotMembers(true)
	ring := BuildRing(members, true)

	// Find 3 distinct replicas (on distinct IPs) for this file using consistent hashing
	targets := ring.OwnersForFileDistinctHosts(hyDFSfilename, 3)

	if len(targets) < 3 {
		fmt.Printf("Warning: Only found %d replicas (need 3)\n", len(targets))
		return false
	}

	// Encode file content to base64 for transmission
	fileContent, err := EncodeFileToBase64(localfilename)
	if err != nil {
		fmt.Printf("file encode error: %v", err)
	}

	v, _ := MembershipList.Load(selfId)
	self := v.(Member)

	// Create file payload with unique chunk ID
	payloadBytes, _ := json.Marshal(FilePayload{
		Filename: hyDFSfilename,
		DataB64:  fileContent,
		ID:       GetUUID(),
	})

	// Determine message type based on operation
	var msgType MessageType
	if create {
		msgType = CreateHyDFSFile
	} else {
		msgType = AppendHyDFSFile
	}

	message := Message{
		MessageType: msgType,
		From:        &self,
		Payload:     payloadBytes,
	}

	// Send to all replicas concurrently and collect responses
	var wg sync.WaitGroup
	results := make(chan *Message, len(targets))
	wg.Add(len(targets))

	for _, t := range targets {
		go func() {
			defer wg.Done()
			addr := fmt.Sprintf("%s:%d", t.IP, t.Port)
			resp, err := sendTCP(addr, &message)
			if err != nil {
				// fmt.Printf("send to %s failed: %v\n", addr, err)
				return
			}
			results <- resp
		}()
	}

	wg.Wait()
	close(results)

	// Collect all responses
	var all []*Message
	for r := range results {
		all = append(all, r)
	}

	// Count successful ACKs
	successes := 0
	for _, message := range all {
		var ack ACKType
		if err := json.Unmarshal(message.Payload, &ack); err != nil {
			fmt.Printf("ack unmarshal error: %v", err)
			continue
		}
		if ack == ACK {
			successes++
		}
	}

	// Require all 3 replicas to succeed (strict quorum)
	return successes == 3
}

// ============================================================================
// Main Entry Point
// ============================================================================

// main initializes the node, sets up network listeners, joins system, starts gossip protocol, then processes stdin commands.
func main() {
	// Set up logging to machine.log file
	f, err := os.OpenFile("machine.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("log file open error: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)

	// Initialize HyDFS storage directory
	os.RemoveAll("hydfs")
	os.Mkdir("hydfs", 0755)

	// Get self hostname
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("get hostname error: %v", err)
	}
	selfHost = hostname

	// Initialize self as a member and add to membership list
	self := Member{
		IP:        selfHost,
		Port:      SelfPort,
		Timestamp: GetUUID(), // unique identifier for this node instance
		Heartbeat: 0,
		Status:    Alive,
	}
	selfId = KeyFor(self)
	self.RingID = GetRingId(KeyFor(self))
	MembershipList.Store(selfId, self)

	// Set up UDP listener for membership protocol messages
	listenAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", SelfPort))
	if err != nil {
		fmt.Printf("resolve listenAddr error: %v", err)
	}
	udpConn, err = net.ListenUDP("udp", listenAddr)
	if err != nil {
		fmt.Printf("udp error: %v", err)
	}
	defer udpConn.Close()

	go listenUDP()

	// Set up TCP listener for file operations
	tcpLn, err := net.Listen("tcp", fmt.Sprintf(":%d", SelfPort))
	if err != nil {
		fmt.Printf("tcp error: %v", err)
	}

	go listenTCP(tcpLn)

	// Join the system by sending join request to introducer (if not the introducer itself)
	if !(selfHost == IntroducerHost && SelfPort == IntroducerPort) {
		introducerAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", IntroducerHost, IntroducerPort))
		if err != nil {
			fmt.Printf("resolve introducerAddr error: %v", err)
		}
		initial := Message{
			MessageType: JoinReq,
			From:        &self,
		}
		sendUDP(introducerAddr, &initial)
		log.Printf("sent %s to %s:%d", initial.MessageType, IntroducerHost, IntroducerPort)
	}

	// Start gossip protocol in background
	go gossip(TimeUnit)

	// Process user commands from stdin
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		handleCommand(line)
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("stdin error: %v\n", err)
	}
}

// ============================================================================
// Command Line Interface
// ============================================================================

// handleCommand processes user commands from stdin (list_mem, list_self, create, append).
func handleCommand(line string) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return
	}

	switch fields[0] {
	case "list_mem":
		// Display current membership list (including failed nodes)
		members := SnapshotMembers(false)
		fmt.Printf("Membership List:\n")
		for id, m := range members {
			fmt.Printf("%s - Status: %s, Heartbeat: %d, LastUpdated: %d\n", id, m.Status, m.Heartbeat, m.LastUpdated)
		}

	case "list_self":
		// Display this node's membership key
		fmt.Println(selfId)

	case "create":
		// Create a new file in HyDFS
		if len(fields) != 3 {
			fmt.Println("Usage: create <localfilename> <HyDFSfilename>")
		}
		localfilename := fields[1]
		hyDFSfilename := fields[2]
		success := writeHyDFSFile(localfilename, hyDFSfilename, true)
		if success {
			fmt.Printf("%s written to HyDFS!\n", hyDFSfilename)
		} else {
			fmt.Printf("Failed to write to HyDFS\n")
		}

	case "append":
		// Append data to an existing file in HyDFS
		if len(fields) != 3 {
			fmt.Println("Usage: append <localfilename> <HyDFSfilename>")
		}
		localfilename := fields[1]
		hyDFSfilename := fields[2]
		success := writeHyDFSFile(localfilename, hyDFSfilename, false)
		if success {
			fmt.Printf("Appended to HyDFS file %s!\n", hyDFSfilename)
		} else {
			fmt.Printf("Failed to append to HyDFS\n")
		}

	default:
		fmt.Println("Unknown command:", line)
	}
}

// mergeHyDFSFile merges file versions across all replicas
func mergeHyDFSFile(hyDFSfilename string) bool {
	log.Printf("[MERGE] Starting merge for %s", hyDFSfilename)

	// Find all replicas
	members := SnapshotMembers(true)
	ring := BuildRing(members, true)
	targets := ring.OwnersForFileDistinctHosts(hyDFSfilename, 3)

	if len(targets) < 3 {
		fmt.Printf("Warning: Only found %d replicas (need 3)\n", len(targets))
		return false
	}

	v, _ := MembershipList.Load(selfId)
	self := v.(Member)

	// Step 1: Get chunks from all replicas
	var allChunks []ChunkInfo
	chunkMap := make(map[string]ChunkInfo) // chunkID -> ChunkInfo

	var wg sync.WaitGroup
	chunksChan := make(chan []ChunkInfo, len(targets))

	for _, target := range targets {
		wg.Add(1)
		go func(t Member) {
			defer wg.Done()
			addr := fmt.Sprintf("%s:%d", t.IP, t.Port)

			// Request chunks from this replica
			payloadBytes, _ := json.Marshal(FilePayload{Filename: hyDFSfilename})
			msg := Message{
				MessageType: GetChunksHyDFSFile,
				From:        &self,
				Payload:     payloadBytes,
			}

			resp, err := sendTCP(addr, &msg)
			if err != nil {
				log.Printf("[MERGE] Failed to get chunks from %s: %v", addr, err)
				return
			}

			var cp ChunksPayload
			if err := json.Unmarshal(resp.Payload, &cp); err != nil {
				log.Printf("[MERGE] Failed to unmarshal chunks from %s: %v", addr, err)
				return
			}

			// Read actual chunk data
			chunks := make([]ChunkInfo, 0, len(cp.Chunks))
			for _, chunkMeta := range cp.Chunks {
				chunkPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(hyDFSfilename)+"_"+chunkMeta.ID)
				data, err := os.ReadFile(chunkPath)
				if err != nil {
					// Try to get from the replica
					data = getChunkDataFromReplica(t, hyDFSfilename, chunkMeta.ID)
				}
				if data != nil {
					chunks = append(chunks, ChunkInfo{
						ID:   chunkMeta.ID,
						Data: data,
					})
				}
			}
			chunksChan <- chunks
		}(target)
	}

	wg.Wait()
	close(chunksChan)

	// Collect all chunks
	for chunks := range chunksChan {
		for _, chunk := range chunks {
			// Deduplicate by chunk ID, keep the first one we see
			if _, exists := chunkMap[chunk.ID]; !exists {
				chunkMap[chunk.ID] = chunk
			}
		}
	}

	// Convert to ordered list
	allChunks = make([]ChunkInfo, 0, len(chunkMap))
	for _, chunk := range chunkMap {
		allChunks = append(allChunks, chunk)
	}

	// TODO: Sort chunks properly once we have sequencing metadata
	// For now, sort by chunk ID for determinism
	sort.Slice(allChunks, func(i, j int) bool {
		return allChunks[i].ID < allChunks[j].ID
	})

	log.Printf("[MERGE] Collected %d unique chunks", len(allChunks))

	// Step 2: Write merged chunks back to all replicas
	// Build merge payload with all chunks
	mergeChunks := make([]FilePayload, len(allChunks))
	for i, chunk := range allChunks {
		mergeChunks[i] = FilePayload{
			Filename: hyDFSfilename,
			DataB64:  base64.StdEncoding.EncodeToString(chunk.Data),
			ID:       chunk.ID,
		}
	}

	mergePayload := MergePayload{
		Filename: hyDFSfilename,
		Chunks:   mergeChunks,
	}

	payloadBytes, _ := json.Marshal(mergePayload)
	mergeMsg := Message{
		MessageType: MergeHyDFSFile,
		From:        &self,
		Payload:     payloadBytes,
	}

	// Write to all replicas
	successCount := 0
	for _, target := range targets {
		addr := fmt.Sprintf("%s:%d", target.IP, target.Port)
		resp, err := sendTCP(addr, &mergeMsg)
		if err != nil {
			log.Printf("[MERGE] Failed to write merged chunks to %s: %v", addr, err)
			continue
		}

		var ack ACKType
		if err := json.Unmarshal(resp.Payload, &ack); err == nil && ack == ACK {
			successCount++
		}
	}

	if successCount == len(targets) {
		log.Printf("[MERGE] Successfully merged %s across %d replicas", hyDFSfilename, successCount)
		return true
	}

	log.Printf("[MERGE] Partial success: merged to %d/%d replicas", successCount, len(targets))
	return successCount >= 2 // Quorum
}

// ChunkInfo holds chunk data for merging
type ChunkInfo struct {
	ID   string
	Data []byte
}

// getChunkDataFromReplica fetches chunk data from a specific replica
func getChunkDataFromReplica(target Member, filename string, chunkID string) []byte {
	// This would need a new message type to request specific chunk data
	// For now, return nil - chunks should be available locally after GetChunks
	return nil
}
