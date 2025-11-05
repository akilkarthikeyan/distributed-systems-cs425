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
	"strings"
	"sync"
	"time"
)

var tick int
var membershipList sync.Map

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

func mergeMembershipList(members map[string]Member) {
	for id, m := range members {
		v, ok := membershipList.Load(id)
		if !ok { // New member
			newMember := Member{
				IP:          m.IP,
				Port:        m.Port,
				Timestamp:   m.Timestamp,
				Heartbeat:   m.Heartbeat,
				LastUpdated: tick,
				Status:      m.Status,
			}
			membershipList.Store(id, newMember)
		} else { // Existing member
			existing := v.(Member)
			if m.Heartbeat > existing.Heartbeat {
				existing.Heartbeat = m.Heartbeat
				existing.Status = m.Status
				existing.LastUpdated = tick
				membershipList.Store(id, existing)
			}
		}
	}
}

func gossip(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		tick++

		// Increment self heartbeat
		v, _ := membershipList.Load(selfId)
		self := v.(Member)

		self.Status = Alive
		self.Heartbeat++
		self.LastUpdated = tick
		membershipList.Store(selfId, self)

		// Check for failed/suspected members
		membershipList.Range(func(k, v any) bool {
			m := v.(Member)
			elapsed := tick - m.LastUpdated

			if m.Status == Alive && elapsed >= Tfail {
				m.Status = Failed
				m.LastUpdated = tick
				membershipList.Store(k.(string), m)
				// fmt.Printf("[FAIL] %s marked failed at tick %d\n", k.(string), tick)
			} else if m.Status == Failed && elapsed >= Tcleanup {
				membershipList.Delete(k.(string))
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
		v, _ := membershipList.Load(selfId)
		self := v.(Member)

		payloadBytes, _ := json.Marshal(GossipPayload{Members: members})
		reply := Message{
			MessageType: JoinReply,
			From:        &self,
			Payload:     payloadBytes,
		}
		senderAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", msg.From.IP, msg.From.Port))
		if err != nil {
			fmt.Printf("handle udp resolve sender error: %v", err)
			return
		}
		sendUDP(senderAddr, &reply)
		log.Printf("sent %s to %s", reply.MessageType, KeyFor(*msg.From))

	// UDP message
	case JoinReply:
		var gp GossipPayload
		if err := json.Unmarshal(msg.Payload, &gp); err != nil {
			fmt.Printf("handle udp gossip payload unmarshal error: %v", err)
			return
		}
		mergeMembershipList(gp.Members)

	// DELETE
	case TCPTest:
		v, _ := membershipList.Load(selfId)
		self := v.(Member)

		var temp string
		if err := json.Unmarshal(msg.Payload, &temp); err != nil {
			fmt.Printf("Shit: %v", err)
			return
		}
		fmt.Printf("Contents: %v\n", temp)

		reply := Message{
			MessageType: TCPTest,
			From:        &self,
			Payload:     json.RawMessage(`"Hello back from server!"`),
		}
		if err := encoder.Encode(&reply); err != nil {
			fmt.Printf("Shit 2: %v", err)
		}
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

func main() {
	f, err := os.OpenFile("machine.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("log file open error: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)

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
	membershipList.Store(selfId, self)

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
		// log.Printf("sent %s to %s:%d", initial.MessageType, IntroducerHost, IntroducerPort)
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

	default:
		fmt.Println("Unknown command:", line)
	}
}
