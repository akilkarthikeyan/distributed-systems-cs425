package main

import (
	"encoding/json"
	"fmt"
	"net"
	"os"
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
	serverId string
	address  string
	time     time.Time
	status   bool
}

type MembershipList struct {
	serverId   string
	address    string
	introducer string
	members    []*MemberStatus
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

	_, err = conn.Write(reqData)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return fmt.Errorf("timeout writing request: %v", err)
		}
		return fmt.Errorf("failed to write request: %v", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))

	buf := make([]byte, 1024)
	n, err := conn.Read(buf)
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return fmt.Errorf("timeout reading response: %v", err)
		}
		return fmt.Errorf("failed to read response: %v", err)
	}

	err = json.Unmarshal(buf[:n], response)
	if err != nil {
		return fmt.Errorf("failed to unmarshal response: %v", err)
	}

	return nil
}

func sendMembershipRequest(request MembershipRequest, response *MembershipList) {
	return sendUDPRequest("vm9501:1234", request, response)
}

func sendMessage(message Message, response *Message) error {
	return sendUDPRequest("vm9501:1234", message, response)
}

func rand() string {
	return fmt.Sprintf("%02d", rand.Intn(10)+1)
}

func chooseRandomServer() string {
	randServer := rand()
	return "vm" + randServer + ":1234"
}

func handleMessage(message Message) {
	var response Message
	var shouldRespond bool = false

	hostname, _ := os.Hostname()

	switch message.Type {
	case PingMsg:
		// Handle ping - respond with ACK
		fmt.Printf("Handling PING from %s\n", message.serverId)
		response = Message{
			Type:     AckMsg,
			serverId: hostname,
			address:  message.address,
		}
		shouldRespond = true

	case AckMsg:
		// Handle ack - update failure detection status
		fmt.Printf("Handling ACK from %s (ServerId: %s)\n", message.address, message.serverId)
		// Mark server as alive in your failure detection data structures

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
