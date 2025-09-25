// main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type Status string

const (
	Alive     Status = "alive"
	Dead      Status = "dead"
	Suspected Status = "suspected"
)

type MessageType string

const (
	Gossip MessageType = "gossip"
	Join   MessageType = "join"
)

type Member struct {
	IP        string `json:"ip"`
	Port      int    `json:"port"`
	Timestamp string `json:"timestamp"`
	Heartbeat int    `json:"heartbeat"`
	Status    Status `json:"status"`
}

type Message struct {
	MessageType MessageType       `json:"messageType"`
	Self        *Member           `json:"self,omitempty"`
	Members     map[string]Member `json:"members,omitempty"`
}

const (
	ListenPort     = 1234
	SelfHost       = "fa25-cs425-9501.cs.illinois.edu"
	IntroducerHost = "fa25-cs425-9501.cs.illinois.edu"
	IntroducerPort = 1234
)

var membershipList sync.Map

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
		log.Printf("recv %s from %s: %+v", msg.MessageType, raddr.String(), msg)

		// TODO: merge membership, update heartbeats, etc.
		// Example response (disabled):
		// resp := Message{MessageType: Gossip, Self: currentSelf(), Members: snapshotMembers()}
		// go sendUDP(conn, raddr, &resp)
	}
}

func keyFor(m Member) string { return fmt.Sprintf("%s:%d:%s", m.IP, m.Port, m.Timestamp) }

func snapshotMembers() map[string]Member {
	out := make(map[string]Member)
	membershipList.Range(func(k, v any) bool {
		out[k.(string)] = v.(Member)
		return true
	})
	return out
}

func main() {
	// Bind locally on all interfaces :ListenPort
	listenAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", ListenPort))
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
		IP:        SelfHost,
		Port:      ListenPort,
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Heartbeat: 0,
		Status:    Alive,
	}
	membershipList.Store(keyFor(self), self)

	// Listen for messages
	go listenUDP(conn)

	// Send JOIN to introducer iff we are not the introducer
	if !(SelfHost == IntroducerHost && ListenPort == IntroducerPort) {
		introducerAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", IntroducerHost, IntroducerPort))
		if err != nil {
			log.Fatalf("resolve introducer: %v", err)
		}
		initial := Message{
			MessageType: Join,
			Self:        &self,
			Members:     nil, // omitted due to ,omitempty
		}
		sendUDP(conn, introducerAddr, &initial)
		log.Printf("sent JOIN to %s", introducerAddr.String())
	} else {
		log.Printf("running as introducer on :%d", ListenPort)
	}

	select {}
}
