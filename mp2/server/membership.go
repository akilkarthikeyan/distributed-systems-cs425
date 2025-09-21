package main

import (
	"encoding/json"
	"fmt"
	"net"
	"time"
)

type MembershipRequest struct {
	Hostname string
	Port     string
}

type Message struct {
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

func sendMembershipRequest(MembershipRequest request, response *MembershipList) {
	conn, err := net.Dial("udp", "vm9501:1234")

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
