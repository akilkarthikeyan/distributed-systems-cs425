package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

var tick int
var membershipList sync.Map

// Init these
var selfHost string
var selfId string

func sendUDP(conn *net.UDPConn, addr *net.UDPAddr, msg *Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("marshal: %v", err)
		return
	}
	if _, err := conn.WriteToUDP(data, addr); err != nil {
		log.Printf("write: %v", err)
	}
}

func listenUDP(conn *net.UDPConn) {
	buf := make([]byte, 4096)
	for {
		n, raddr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("read: %v", err)
			continue
		}
		var msg Message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			log.Printf("unmarshal from %v: %v", raddr, err)
			continue
		}
		handleMessage(conn, &msg)
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

func gossip(conn *net.UDPConn, interval time.Duration) {
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
				log.Printf("resolve target: %v", err)
				continue
			}
			msg := Message{
				MessageType: Gossip,
				From:        &self,
				Payload: map[string]any{
					"Members": members,
				},
			}
			sendUDP(conn, targetAddr, &msg)
			log.Printf("sent %s to %s", msg.MessageType, KeyFor(target))
		}
	}
}

func handleMessage(conn *net.UDPConn, msg *Message) {
	log.Printf("recv %s from %s", msg.MessageType, KeyFor(*msg.From))

	switch msg.MessageType {
	case Gossip:
		mergeMembershipList(msg.Payload["Members"].(map[string]Member))

	case JoinReq:
		// Send JoinReply with current membership list
		members := SnapshotMembers(true)
		v, _ := membershipList.Load(selfId)
		self := v.(Member)

		reply := Message{
			MessageType: JoinReply,
			From:        &self,
			Payload: map[string]any{
				"Members": members,
			},
		}
		senderAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", msg.From.IP, msg.From.Port))
		if err != nil {
			log.Printf("resolve sender: %v", err)
			return
		}
		sendUDP(conn, senderAddr, &reply)
		log.Printf("sent %s to %s", reply.MessageType, KeyFor(*msg.From))

	case JoinReply:
		mergeMembershipList(msg.Payload["Members"].(map[string]Member))
	}
}

func main() {
	f, err := os.OpenFile("machine.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("error opening log file: %v", err)
	}
	defer f.Close()
	log.SetOutput(f)

	// Get self hostname
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("get hostname: %v", err)
	}
	selfHost = hostname

	// Bind locally on all interfaces: SelfPort
	listenAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", SelfPort))
	if err != nil {
		log.Fatalf("resolve listen: %v", err)
	}
	conn, err := net.ListenUDP("udp", listenAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	defer conn.Close()

	// Add self to membership list
	self := Member{
		IP:        selfHost,
		Port:      SelfPort,
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Heartbeat: 0,
		Status:    Alive,
	}
	selfId = KeyFor(self)
	membershipList.Store(selfId, self)

	// Listen for messages
	go listenUDP(conn)

	// Send join to introducer iff we are not the introducer
	if !(selfHost == IntroducerHost && SelfPort == IntroducerPort) {
		introducerAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", IntroducerHost, IntroducerPort))
		if err != nil {
			log.Fatalf("resolve introducer: %v", err)
		}
		initial := Message{
			MessageType: JoinReq,
			From:        &self,
		}
		sendUDP(conn, introducerAddr, &initial)
		log.Printf("sent %s to %s:%d", initial.MessageType, IntroducerHost, IntroducerPort)
	}

	go gossip(conn, TimeUnit)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		line := scanner.Text()
		handleCommand(line, conn)
	}
	if err := scanner.Err(); err != nil {
		fmt.Printf("stdin error: %v\n", err)
	}
}

func handleCommand(line string, conn *net.UDPConn) {
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

	default:
		fmt.Println("Unknown command:", line)
	}
}
