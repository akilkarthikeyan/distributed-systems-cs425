package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

var Tick int
var MembershipList sync.Map
var HyDFSFiles sync.Map

var selfHost string
var selfId string

var udpConn *net.UDPConn

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

			if m.Status == Alive && elapsed >= Tfail {
				m.Status = Failed
				m.LastUpdated = Tick
				MembershipList.Store(k.(string), m)
				// fmt.Printf("[FAIL] %s marked failed at tick %d\n", k.(string), tick)
			} else if m.Status == Failed && elapsed >= Tcleanup {
				MembershipList.Delete(k.(string))
				// fmt.Printf("[DELETE] %s removed from membership list at tick %d\n", k.(string), tick)
			}

			return true
		})

		// Select K random members to gossip to (exlude self)
		members := SnapshotMembers(true)
		delete(members, selfId)
		targets := SelectKMembers(members, K)
		members[selfId] = self // add self back

		// Gossip
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

func handleMessage(msg *Message, encoder *json.Encoder) { // encoder can only be present for TCP messages
	log.Printf("recv %s from %s", msg.MessageType, KeyFor(*msg.From))

	switch msg.MessageType {
	// UDP message
	case Gossip:
		var gp GossipPayload
		if err := json.Unmarshal(msg.Payload, &gp); err != nil {
			fmt.Printf("gossip payload unmarshal error: %v", err)
			return
		}
		mergeMembershipList(gp.Members)

	// UDP message
	case JoinReq:
		// Send JoinReply with current membership list
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

	// UDP message
	case JoinReply:
		var gp GossipPayload
		if err := json.Unmarshal(msg.Payload, &gp); err != nil {
			fmt.Printf("gossip payload unmarshal error: %v", err)
			return
		}
		mergeMembershipList(gp.Members)

	// TCP message
	case CreateHyDFSFile: // Returns ACK, or NACK if file already exists
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		var fp FilePayload
		if err := json.Unmarshal(msg.Payload, &fp); err != nil {
			fmt.Printf("file payload unmarshal error: %v", err)
			return
		}

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

		payloadBytes, _ := json.Marshal(ACK)
		encoder.Encode(&Message{
			MessageType: CreateHyDFSFile,
			From:        &self,
			Payload:     payloadBytes,
		})

	// TCP message
	case AppendHyDFSFile: // Returns ACK, or NACK if file does not exist
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		var fp FilePayload
		if err := json.Unmarshal(msg.Payload, &fp); err != nil {
			fmt.Printf("file payload unmarshal error: %v", err)
			return
		}

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

		hyDFSFile := w.(HyDFSFile)
		hyDFSFile.Chunks = append(hyDFSFile.Chunks, FilePayload{
			Filename: fp.Filename,
			ID:       fp.ID,
		})
		HyDFSFiles.Store(fp.Filename, hyDFSFile)

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

		payloadBytes, _ := json.Marshal(ACK)
		encoder.Encode(&Message{
			MessageType: AppendHyDFSFile,
			From:        &self,
			Payload:     payloadBytes,
		})

	// TCP message
	case GetHyDFSFile: // Returns file payloads if file exists, else NACK
		v, _ := MembershipList.Load(selfId)
		self := v.(Member)

		var filename string
		if err := json.Unmarshal(msg.Payload, &filename); err != nil {
			fmt.Printf("get hydfs file payload unmarshal error: %v", err)
			return
		}

		w, ok := HyDFSFiles.Load(filename)
		if !ok {
			payloadBytes, _ := json.Marshal(GetHyDFSFilePayload{
				Ack: NACK,
			})
			encoder.Encode(&Message{
				MessageType: GetHyDFSFile,
				From:        &self,
				Payload:     payloadBytes,
			})
			return
		}

		filePayloads := make([]FilePayload, 0)
		hyDFSFile := w.(HyDFSFile)
		for _, chunk := range hyDFSFile.Chunks {
			targetPath := filepath.Join("hydfs", GetHyDFSCompliantFilename(chunk.Filename)+"_"+chunk.ID)
			data, err := EncodeFileToBase64(targetPath)
			if err != nil {
				fmt.Printf("file encode error: %v", err)
				return
			}
			filePayloads = append(filePayloads, FilePayload{
				Filename: chunk.Filename,
				DataB64:  data,
				ID:       chunk.ID,
			})
		}

		payloadBytes, _ := json.Marshal(GetHyDFSFilePayload{
			Ack:          ACK,
			FilePayloads: filePayloads,
		})
		encoder.Encode(&Message{
			MessageType: GetHyDFSFile,
			From:        &self,
			Payload:     payloadBytes,
		})
	}
}

func writeHyDFSFile(localfilename string, hyDFSfilename string, create bool) bool {
	// Writes to a quorum of 3 replicas
	targets := make([]Member, 0, 3)

	targets = append(targets, GetRingSuccessor(GetRingId(hyDFSfilename)))
	targets = append(targets, GetRingSuccessor(GetRingId(KeyFor(targets[0]))))
	targets = append(targets, GetRingSuccessor(GetRingId(KeyFor(targets[1]))))

	fileContent, err := EncodeFileToBase64(localfilename)
	if err != nil {
		fmt.Printf("file encode error: %v", err)
	}

	v, _ := MembershipList.Load(selfId)
	self := v.(Member)

	payloadBytes, _ := json.Marshal(FilePayload{
		Filename: hyDFSfilename,
		DataB64:  fileContent,
		ID:       GetUUID(),
	})

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

	var all []*Message
	for r := range results {
		all = append(all, r)
	}

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

	return successes == 3
}

func readHyDFSFile(hyDFSfilename string, localfilename string) bool {
	// Read, but read quorum is just 1
	targets := make([]Member, 0, 3)

	targets = append(targets, GetRingSuccessor(GetRingId(hyDFSfilename)))
	targets = append(targets, GetRingSuccessor(GetRingId(KeyFor(targets[0]))))
	targets = append(targets, GetRingSuccessor(GetRingId(KeyFor(targets[1]))))

	v, _ := MembershipList.Load(selfId)
	self := v.(Member)

	payloadBytes, _ := json.Marshal(hyDFSfilename)
	req := Message{
		MessageType: GetHyDFSFile,
		From:        &self,
		Payload:     payloadBytes,
	}

	result := make(chan GetHyDFSFilePayload, len(targets))

	for _, t := range targets {
		go func() {
			addr := fmt.Sprintf("%s:%d", t.IP, t.Port)
			resp, err := sendTCP(addr, &req)
			if err != nil {
				result <- GetHyDFSFilePayload{
					Ack: NACK,
				}
				return
			}

			var ghfp GetHyDFSFilePayload
			if err := json.Unmarshal(resp.Payload, &ghfp); err != nil {
				result <- GetHyDFSFilePayload{
					Ack: NACK,
				}
				return
			}

			result <- ghfp
		}()
	}

	// Consume up to len(targets) replies in completion order; stop on first ACK
	for i := 0; i < len(targets); i++ {
		ghfp := <-result
		if ghfp.Ack == ACK {
			var combinedB64 string
			for _, fp := range ghfp.FilePayloads {
				combinedB64 += fp.DataB64
			}

			data, err := DecodeBase64ToBytes(combinedB64)
			if err != nil {
				fmt.Printf("file decode error: %v", err)
				return false
			}

			if err := os.WriteFile(localfilename, data, 0644); err != nil {
				fmt.Printf("failed to write file: %v\n", err)
				return false
			}
			return true
		}
	}

	return false
}

func main() {
	f, err := os.OpenFile("machine.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("log file open error: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)

	// Erase previous hydfs files and make dir
	os.RemoveAll("hydfs")
	os.Mkdir("hydfs", 0755)

	// Get self hostname
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("get hostname error: %v", err)
	}
	selfHost = hostname

	// Add self to membership list
	self := Member{
		IP:        selfHost,
		Port:      SelfPort,
		Timestamp: GetUUID(), // unique
		Heartbeat: 0,
		Status:    Alive,
	}
	selfId = KeyFor(self)
	self.RingID = GetRingId(KeyFor(self))
	MembershipList.Store(selfId, self)

	// Listen for UDP messages
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

	// Listen for TCP messages
	tcpLn, err := net.Listen("tcp", fmt.Sprintf(":%d", SelfPort))
	if err != nil {
		fmt.Printf("tcp error: %v", err)
	}

	go listenTCP(tcpLn)

	// Send join to introducer iff we are not the introducer
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

	go gossip(TimeUnit)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		handleCommand(line)
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("stdin error: %v\n", err)
	}
}

func handleCommand(line string) {
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return
	}

	switch fields[0] {
	case "list_mem":
		members := SnapshotMembers(false)
		fmt.Printf("Membership List:\n")
		for id, m := range members {
			fmt.Printf("%s - Status: %s, Heartbeat: %d, LastUpdated: %d\n", id, m.Status, m.Heartbeat, m.LastUpdated)
		}

	case "list_self":
		fmt.Println(selfId)

	case "create":
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

	case "get":
		if len(fields) != 3 {
			fmt.Println("Usage: get <HyDFSfilename> <localfilename>")
		}
		hyDFSfilename := fields[1]
		localfilename := fields[2]
		success := readHyDFSFile(hyDFSfilename, localfilename)
		if success {
			fmt.Printf("Retrieved HyDFS file %s to %s!\n", hyDFSfilename, localfilename)
		} else {
			fmt.Printf("Failed to retrieve HyDFS file\n")
		}

	default:
		fmt.Println("Unknown command:", line)
	}
}
