package main

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"strings"
	"sync"
	"time"
)

type MessageType string

const (
	HeartbeatMsg MessageType = "heartbeat"
	GossipMsg    MessageType = "gossip"
	PingMsg      MessageType = "ping"
	AckMsg       MessageType = "ack"
	JoinMsg      MessageType = "join"
	LeaveMsg     MessageType = "leave"
)

type MembershipRequest struct {
	Hostname string
	Port     string
}

type Message struct {
	Type      MessageType
	serverId  string
	address   string
	Heartbeat int
}

type MemberStatus struct {
	serverId    string
	address     string
	lastSeen    time.Time
	lastSuspect time.Time
	alive       bool
	suspect     bool
}

type MembershipList struct {
	serverId   string
	address    string
	introducer string
	members    []*MemberStatus
}

var (
	servers            []string
	localServerAddress string

	localMembership = MembershipList{
		members: make([]*MemberStatus, 0),
	}
	pendingPings     = make(map[string]time.Time)
	pendingPingMutex sync.Mutex
)

const (
	probeInterval  = 2 * time.Second
	ackTimeout     = 3 * time.Second
	suspectTimeout = 10 * time.Second
)

func init() {
	data, err := os.ReadFile("sources.json")
	if err != nil {
		panic(fmt.Sprintf("Failed to read sources.json: %v", err))
	}

	err = json.Unmarshal(data, &servers)
	if err != nil {
		panic(fmt.Sprintf("Failed to parse sources.json: %v", err))
	}

}

func (ml *MembershipList) markSuspect(serverId, address string) *MemberStatus {
	member := ml.ensureMember(serverId, address)
	member.suspect = true
	member.lastSuspect = time.Now()
	member.alive = false
	return member
}
func (ml *MembershipList) markDead(serverId, address string) *MemberStatus {
	member := ml.ensureMember(serverId, address)
	member.alive = false
	member.suspect = false
	return member
}

func (ml *MembershipList) markAlive(serverId, address string) *MemberStatus {
	member := ml.ensureMember(serverId, address)
	member.alive = true
	member.suspect = false
	member.lastSeen = time.Now()
	return member
}
func sendUDPRequest(address string, request interface{}, response interface{}) error {
	conn, err := net.Dial("udp", address)
	if err != nil {
		return fmt.Errorf("failed to dial: %v", err)
	}
	defer conn.Close()

	reqData, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("failed to marshal request: %v", err)
	}

	if _, err = conn.Write(reqData); err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return fmt.Errorf("timeout writing request: %v", err)
		}
		return fmt.Errorf("failed to write request: %v", err)
	}

	if response == nil {
		return nil
	}

	conn.SetReadDeadline(time.Now().Add(ackTimeout))

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return fmt.Errorf("timeout reading response: %v", err)
		}
		return fmt.Errorf("failed to read response: %v", err)
	}

	if err = json.Unmarshal(buf[:n], response); err != nil {
		return fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return nil

}

func handleUDPMessages(conn *net.UDPConn) {
	buf := make([]byte, 4096)

	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("Failed to read UDP message: %v\n", err)
			continue
		}

		var message Message
		if err := json.Unmarshal(buf[:n], &message); err != nil {
			fmt.Printf("Failed to unmarshal UDP message: %v\n", err)
			continue
		}

		handleMessage(message)
	}

}

func startPingLoop() {
	ticker := time.NewTicker(probeInterval)
	defer ticker.Stop()

	for range ticker.C {
		if localServerAddress == "" {
			continue
		}

		target := chooseRandomServer()
		if target == "" {
			continue
		}

		ping := Message{
			Type:     PingMsg,
			serverId: localMembership.serverId,
			address:  localServerAddress,
		}

		if err := sendUDPRequest(target, ping, nil); err != nil {
			fmt.Printf("Failed to send PING to %s: %v\n", target, err)
		}
	}
}

func checkPendingPings() {
	now := time.Now()
	pendingPingMutex.Lock()

}

func sendMembershipRequest(request MembershipRequest, response *MembershipList) {
	return sendUDPRequest("vm9501:1234", request, response)
}

func sendMessage(message Message, response *Message) error {
	return sendUDPRequest("vm9501:1234", message, response)
}

func chooseRandomServer() string {
	if len(servers) == 0 {
		return ""
	}

	hostname, _ := os.Hostname()

	for {
		picked := servers[rand.IntN(len(servers))]
		if !strings.Contains(picked, hostname) {
			return picked
		}
	}
}

func (ml *MembershipList) ensureMember(serverID, address string) *MemberStatus {
	for _, member := range ml.members {
		if member.serverId == serverID {
			if address != "" {
				member.address = address
			}
			return member
		}
	}

	member := &MemberStatus{
		serverId: serverID,
		address:  address,
	}
	ml.members = append(ml.members, member)
	return member
}

func handleMessage(message Message) {
	var response Message
	var shouldRespond bool = false

	hostname, _ := os.Hostname()

	switch message.Type {
	case PingMsg:
		// Handle ping - respond with ACK
		fmt.Printf("Handling PING from %s\n", message.serverId)

		member := localMembership.markAlive(message.serverId, message.address)
		fmt.Printf("Membership refreshed: %+v\n", *member)

		response = Message{
			Type:     AckMsg,
			serverId: hostname,
			address:  message.address,
		}
		shouldRespond = true

	case AckMsg:
		// Handle ack - update failure detection status
		fmt.Printf("Handling ACK from %s (ServerId: %s)\n", message.address, message.serverId)
		// Mark server as alive in membership data structure
		member := localMembership.markAlive(message.serverId, message.address)
		fmt.Printf("Membership refreshed: %+v\n", *member)

	case JoinMsg:
		// Handle join request - respond with JOIN_OK
		fmt.Printf("Handling JOIN from %s (ServerId: %s)\n", message.address, message.serverId)
		response = Message{
			Type:     AckMsg, // Using AckMsg as JOIN_OK equivalent
			serverId: hostname,
			address:  message.address,
		}
		shouldRespond = true
		// Add new member to membership list here

	case HeartbeatMsg:
		// Handle heartbeat message
		fmt.Printf("Handling HEARTBEAT from %s (Heartbeat: %d)\n", message.serverId, message.Heartbeat)
		// Update heartbeat counter and timestamp for this server

	case GossipMsg:
		// Handle gossip message
		fmt.Printf("Handling GOSSIP from %s\n", message.serverId)
		// Update membership information based on gossip data

	case LeaveMsg:
		// Handle leave message
		fmt.Printf("Handling LEAVE from %s\n", message.serverId)
		// Remove server from membership list

	default:
		fmt.Printf("Unknown message type: %s\n", message.Type)
	}

	// Send response if needed
	if shouldRespond {
		err := sendUDPRequest(message.address, response, nil)
		if err != nil {
			fmt.Printf("Failed to send response to %s: %v\n", message.address, err)
		} else {
			fmt.Printf("Sent %s response to %s\n", response.Type, message.address)
		}
	}
}
