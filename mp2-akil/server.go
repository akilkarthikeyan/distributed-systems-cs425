// main.go
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"
	"math/rand"
)

type Status string

const (
	Alive     Status = "alive"
	Failed      Status = "failed"
	Suspected Status = "suspected"
)

type MessageType string

const (
	Gossip 		MessageType = "gossip"
	Join   		MessageType = "join"
	JoinReply 	MessageType = "join-reply"
)

type Member struct {
	IP        	string 	`json:"ip"`
	Port      	int    	`json:"port"`
	Timestamp 	string 	`json:"timestamp"`
	Heartbeat 	int    	`json:"heartbeat"`
	LastUpdated int  	`json:"lastUpdated"`
	Status    	Status 	`json:"status"`
}

type Message struct {
	MessageType MessageType       `json:"messageType"`
	Self        *Member           `json:"self,omitempty"`
	Members     map[string]Member `json:"members,omitempty"`
}

const (
	SelfPort     = 1234
	SelfHost       = "fa25-cs425-9501.cs.illinois.edu"
	IntroducerHost = "fa25-cs425-9501.cs.illinois.edu"
	IntroducerPort = 1234
	Tsuspect      = 5
	Tfail         = 10
	Tcleanup      = 10
)

var membershipList sync.Map
var selfId string
var tick int

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

	}
}

func keyFor(m Member) string { return fmt.Sprintf("%s:%d:%s", m.IP, m.Port, m.Timestamp) }

func snapshotMembers(omitFailed bool) map[string]Member {
	out := make(map[string]Member)
	membershipList.Range(func(k, v any) bool {
		if omitFailed && v.(Member).Status == Failed {
			return true
		}
		out[k.(string)] = v.(Member)
		return true
	})
	return out
}

func selectKMembers(members map[string]Member, k int) []Member {
    slice := make([]Member, 0, len(members))
    for _, m := range members {
        slice = append(slice, m)
    }

    // Fewer than k members
    if len(slice) <= k {
        return slice
    }

    rand.Shuffle(len(slice), func(i, j int) {
        slice[i], slice[j] = slice[j], slice[i]
    })

    return slice[:k]
}

func gossip(conn *net.UDPConn, interval time.Duration) {
    ticker := time.NewTicker(interval)
    defer ticker.Stop()

    for range ticker.C {
		tick++

		// Increment self heartbeat
		v, _ := membershipList.Load(selfId)
		self := v.(Member)

		self.Heartbeat++
		self.LastUpdated = tick
		membershipList.Store(selfId, self)

		// Check for failed/suspected members
		membershipList.Range(func(k, v any) bool {
			m := v.(Member)
			elapsed := tick - m.LastUpdated
			if m.Status == Alive && elapsed >= Tsuspect {
				m.Status = Suspected
				membershipList.Store(k.(string), m)
			} else if m.Status == Suspected && elapsed >= Tfail {
				m.Status = Failed
				membershipList.Store(k.(string), m)
			} else if m.Status == Failed && elapsed >= Tcleanup {
				membershipList.Delete(k.(string))
			}
			return true
		})

		// Select 3 random members to gossip to (exlude self)
		members := snapshotMembers(true)
		temp := members[selfId]
		delete(members, selfId)
		targets := selectKMembers(members, 3)
		members[selfId] = temp // add self back

		// Gossip
		for _, target := range targets {
			targetAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", target.IP, target.Port))
			if err != nil {
				log.Printf("resolve target: %v", err)
				continue
			}
			msg := Message{
				MessageType: Gossip,
				Self:        &self,
				Members:     members,
			}
			sendUDP(conn, targetAddr, &msg)
			log.Printf("sent GOSSIP to %s", targetAddr.String())
		}
    }
}


func main() {
	// Set seed for random
	rand.Seed(time.Now().UnixNano())

	// Bind locally on all interfaces :SelfPort
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
		IP:        SelfHost,
		Port:      SelfPort,
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Heartbeat: 0,
		Status:    Alive,
	}
	selfId = keyFor(self)
	membershipList.Store(selfId, self)

	// Listen for messages
	go listenUDP(conn)

	// Send JOIN to introducer iff we are not the introducer
	if !(SelfHost == IntroducerHost && SelfPort == IntroducerPort) {
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
	}

	// Gossip
	go gossip(conn, 5*time.Second)

	select {}
}
