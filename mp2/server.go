// main.go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Status string

const (
	Alive     Status = "alive"
	Failed    Status = "failed"
	Suspected Status = "suspected"
)

type MessageType string

const (
	Gossip    MessageType = "gossip"
	JoinReq   MessageType = "join-req"
	JoinReply MessageType = "join-reply"
	Ping      MessageType = "ping"
	Ack       MessageType = "ack"
	Switch    MessageType = "switch"
)

type ProtocolAndSuspectMode struct {
	Protocol    string `json:"protocol"`
	SuspectMode string `json:"suspectMode"`
}

type Member struct {
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Timestamp   string `json:"timestamp"`
	Heartbeat   int    `json:"heartbeat"`
	LastUpdated int    `json:"lastUpdated"`
	Status      Status `json:"status"`
}

type Message struct {
	MessageType MessageType             `json:"messageType"`
	Self        *Member                 `json:"self,omitempty"`
	Members     map[string]Member       `json:"members,omitempty"`
	SwitchTo    *ProtocolAndSuspectMode `json:"switchTo,omitempty"`
}

const (
	SelfPort = 1234
	// SelfHost       = "fa25-cs425-9501.cs.illinois.edu"
	IntroducerHost = "fa25-cs425-9501.cs.illinois.edu"
	IntroducerPort = 1234
	Tsuspect       = 5
	Tfail          = 5
	Tcleanup       = 5
	K              = 3
	TimeUnit       = time.Second
)

var SelfHost string
var Protocol atomic.Value    // "ping "or "gossip"
var SuspectMode atomic.Value // "suspect" or "nosuspect"
var FailureRate float64 = 0.0

var membershipList sync.Map
var ackList sync.Map
var selfId string
var tick int

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

func mergeMembershipList(members map[string]Member) {
	for id, m := range members {
		v, ok := membershipList.Load(id)
		if !ok { // New member
			// If suspect mode is nosuspect, add as alive
			if SuspectMode.Load() == "nosuspect" {
				m.Status = Alive
			}
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
			if m.Heartbeat == existing.Heartbeat {
				if SuspectMode.Load() == "suspect" && m.Status == Suspected && existing.Status == Alive {
					existing.Status = Suspected
					existing.LastUpdated = tick
					membershipList.Store(id, existing)
					fmt.Printf("[SUSPECT - gossip] %s marked suspected at tick %d\n", id, tick)
				}
			}
		}
	}
}

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

func broadcastSwitch(conn *net.UDPConn) {
	members := snapshotMembers(true)
	delete(members, selfId)

	proto := Protocol.Load()
	mode := SuspectMode.Load()

	v, _ := membershipList.Load(selfId)
	self := v.(Member)

	msg := Message{
		MessageType: Switch,
		Self:        &self,
		SwitchTo: &ProtocolAndSuspectMode{
			Protocol:    proto.(string),
			SuspectMode: mode.(string),
		},
	}

	for _, target := range members {
		targetAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", target.IP, target.Port))
		if err != nil {
			log.Printf("resolve target: %v", err)
			continue
		}
		sendUDP(conn, targetAddr, &msg)
		log.Printf("sent %s to %s", msg.MessageType, keyFor(target))
	}
}

func gossip(conn *net.UDPConn, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	startMode := SuspectMode.Load() // mode at start of goroutine

	for range ticker.C {
		// End goroutine if protocol changes or suspect mode changes
		if Protocol.Load() == "ping" || startMode != SuspectMode.Load() {
			if Protocol.Load() == "ping" {
				fmt.Println("[GOSSIP STOP] Switching to ping protocol")
			} else {
				fmt.Println("[GOSSIP STOP] Suspect mode changed, restarting goroutine")
			}
			return
		}
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
			if startMode == "suspect" {
				if m.Status == Alive && elapsed >= Tsuspect {
					m.Status = Suspected
					m.LastUpdated = tick
					membershipList.Store(k.(string), m)
					fmt.Printf("[SUSPECT - timeout] %s marked suspected at tick %d\n", k.(string), tick)
				} else if m.Status == Suspected && elapsed >= Tfail {
					m.Status = Failed
					m.LastUpdated = tick
					membershipList.Store(k.(string), m)
				} else if m.Status == Failed && elapsed >= Tcleanup {
					membershipList.Delete(k.(string))
				}
			} else {
				if m.Status == Alive && elapsed >= Tfail {
					m.Status = Failed
					m.LastUpdated = tick
					membershipList.Store(k.(string), m)
				} else if m.Status == Failed && elapsed >= Tcleanup {
					membershipList.Delete(k.(string))
				}
			}
			return true
		})

		// Select K random members to gossip to (exlude self)
		members := snapshotMembers(true)
		delete(members, selfId)
		targets := selectKMembers(members, K)
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
				Self:        &self,
				Members:     members,
			}
			sendUDP(conn, targetAddr, &msg)
			log.Printf("sent %s to %s", msg.MessageType, keyFor(target))
		}
	}
}

func ping(conn *net.UDPConn, interval time.Duration) {
	// In each round,
	// 1. Shuffle nodes
	// 2. Wait for some amount of time for ACK to come back
	// 3. If that time passes, ping K other nodes simultaneously
	// 4. If atleast one ACK comes back before protocol period ends, mark that node as alive, else suspect
	// 5. Do this for all nodes in the shuffled list
	// 6. Next round
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	startMode := SuspectMode.Load() // mode at start of goroutine

	for { // each round
		// Shuffle nodes
		members := snapshotMembers(true)
		delete(members, selfId)

		slice := make([]Member, 0, len(members))
		for _, m := range members {
			slice = append(slice, m)
		}

		rand.Shuffle(len(slice), func(i, j int) {
			slice[i], slice[j] = slice[j], slice[i]
		})

		for _, target := range slice { // each protocol period
			// Wait for tick
			<-ticker.C
			// End goroutine if protocol changes or suspect mode changes
			if Protocol.Load() == "gossip" || startMode != SuspectMode.Load() {
				if Protocol.Load() == "gossip" {
					fmt.Println("[PING STOP] Switching to gossip protocol")
				} else {
					fmt.Println("[PING STOP] Suspect mode changed, restarting goroutine")
				}
				return
			}
			tick++
			v, _ := membershipList.Load(selfId)
			self := v.(Member)
			self.Heartbeat++
			self.LastUpdated = tick // Not really needed, but whatever
			membershipList.Store(selfId, self)

			// Send ping to target
			targetAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", target.IP, target.Port))
			if err != nil {
				log.Printf("resolve target: %v", err)
				continue
			}
			msg := Message{
				MessageType: Ping,
				Self:        &self,
				Members:     nil, // omitted due to ,omitempty
			}
			sendUDP(conn, targetAddr, &msg)
			log.Printf("sent %s to %s", msg.MessageType, keyFor(target))

			acked := false

			if startMode == "suspect" {
				for i := 1; i < Tsuspect; i++ { // Wait Tsuspect for direct ACK from target
					<-ticker.C
					// End goroutine if protocol changes or suspect mode changes
					if Protocol.Load() == "gossip" || startMode != SuspectMode.Load() {
						if Protocol.Load() == "gossip" {
							fmt.Println("[PING STOP] Switching to gossip protocol")
						} else {
							fmt.Println("[PING STOP] Suspect mode changed, restarting goroutine")
						}
						return
					}
					tick++

					if temp, ok := ackList.Load(keyFor(target)); ok {
						value := temp.(Member)
						if value.Heartbeat > target.Heartbeat {
							acked = true
							target.Heartbeat = value.Heartbeat
							target.Status = Alive
							target.LastUpdated = tick
							membershipList.Store(keyFor(target), target)
							break
						}
					}
				}

				if !acked {
					target.Status = Suspected
					target.LastUpdated = tick
					membershipList.Store(keyFor(target), target)
					fmt.Printf("[SUSPECT - ack timeout] %s marked suspected at tick %d\n", keyFor(target), tick)
				}
			}

			if !acked {
				for i := 1; i < Tfail; i++ { // Wait for Tfail more ticks before deleting target from membershipList
					<-ticker.C
					// End goroutine if protocol changes or suspect mode changes
					if Protocol.Load() == "gossip" || startMode != SuspectMode.Load() {
						if Protocol.Load() == "gossip" {
							fmt.Println("[PING STOP] Switching to gossip protocol")
						} else {
							fmt.Println("[PING STOP] Suspect mode changed, restarting goroutine")
						}
						return
					}
					tick++

					if temp, ok := ackList.Load(keyFor(target)); ok {
						value := temp.(Member)
						if value.Heartbeat > target.Heartbeat {
							acked = true
							target.Heartbeat = value.Heartbeat
							target.Status = Alive
							target.LastUpdated = tick
							membershipList.Store(keyFor(target), target)
							break
						}
					}
				}

				if !acked {
					membershipList.Delete(keyFor(target))
				}
			}
		}
	}
}

func handleMessage(conn *net.UDPConn, msg *Message) {
	// Simulate message drop
	if rand.Float64() < FailureRate {
		return
	}

	log.Printf("recv %s from %s", msg.MessageType, keyFor(*msg.Self))

	switch msg.MessageType {
	case JoinReq:
		// Send JoinReply with current membership list
		members := snapshotMembers(true)
		v, _ := membershipList.Load(selfId)
		self := v.(Member)

		reply := Message{
			MessageType: JoinReply,
			Self:        &self,
			Members:     members,
		}
		senderAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", msg.Self.IP, msg.Self.Port))
		if err != nil {
			log.Printf("resolve sender: %v", err)
			return
		}
		sendUDP(conn, senderAddr, &reply)
		log.Printf("sent %s to %s", reply.MessageType, keyFor(*msg.Self))

	case Gossip:
		mergeMembershipList(msg.Members)

	case JoinReply:
		mergeMembershipList(msg.Members)

	case Ping:
		// See if pinger is in membership list, if not add
		mergeMembershipList(map[string]Member{keyFor(*msg.Self): *msg.Self})
		// Also add in ackList, because we got a direct ping from them, but only if heartbeat is higher
		if v, ok := ackList.Load(keyFor(*msg.Self)); ok {
			value := v.(Member)
			if msg.Self.Heartbeat > value.Heartbeat {
				ackList.Store(keyFor(*msg.Self), *msg.Self)
			}
		} else {
			ackList.Store(keyFor(*msg.Self), *msg.Self)
		}

		v, _ := membershipList.Load(selfId)
		self := v.(Member)
		self.Heartbeat++
		self.LastUpdated = tick
		membershipList.Store(selfId, self)

		// Send Ack
		ack := Message{
			MessageType: Ack,
			Self:        &self,
			Members:     nil, // omitted due to ,omitempty
		}
		senderAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", msg.Self.IP, msg.Self.Port))
		if err != nil {
			log.Printf("resolve sender: %v", err)
			return
		}
		sendUDP(conn, senderAddr, &ack)
		log.Printf("sent %s to %s", ack.MessageType, keyFor(*msg.Self))

	case Ack:
		// Add to ackList, but only if heartbeat is higher
		if v, ok := ackList.Load(keyFor(*msg.Self)); ok {
			value := v.(Member)
			if msg.Self.Heartbeat > value.Heartbeat {
				ackList.Store(keyFor(*msg.Self), *msg.Self)
			}
		} else {
			ackList.Store(keyFor(*msg.Self), *msg.Self)
		}

	case Switch: // Lowkey, not the best (because we don't use heartbeats or anything), but it'll work
		// If protocol or suspect mode is different, switch and broadcast
		oldProto := Protocol.Load()
		oldMode := SuspectMode.Load()
		Protocol.Store(msg.SwitchTo.Protocol)
		SuspectMode.Store(msg.SwitchTo.SuspectMode)

		if oldProto != msg.SwitchTo.Protocol || oldMode != msg.SwitchTo.SuspectMode {
			go broadcastSwitch(conn)

			if oldMode == "suspect" && msg.SwitchTo.SuspectMode == "nosuspect" {
				// Promote suspected members to alive
				membershipList.Range(func(k, v any) bool {
					m := v.(Member)
					if m.Status == Suspected {
						m.Status = Alive
						m.LastUpdated = tick
						membershipList.Store(k.(string), m)
					}
					return true
				})
			}

			// Restart goroutine even if mode changes
			if msg.SwitchTo.Protocol == "gossip" {
				go gossip(conn, TimeUnit)
			} else {
				go ping(conn, TimeUnit)
			}
		}
	}
}

func main() {
	// Set log file for output
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
	SelfHost = hostname

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

	// Set initial Protocol, SuspectMode and FailureRate
	args := os.Args
	args = args[1:]

	if len(args) >= 1 {
		FailureRate, err = strconv.ParseFloat(args[0], 64)
		if err != nil || FailureRate < 0.0 || FailureRate > 1.0 {
			fmt.Printf("invalid failure rate: %v\n", args[0])
		}
	}

	Protocol.Store("ping")
	SuspectMode.Store("nosuspect")

	if len(args) == 3 {
		if (args[1] == "gossip" || args[1] == "ping") && (args[2] == "suspect" || args[2] == "nosuspect") {
			Protocol.Store(args[1])
			SuspectMode.Store(args[2])
		} else {
			fmt.Printf("invalid protocol or suspect mode: %v %v\n", args[1], args[2])
		}
	}

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

	// Send join to introducer iff we are not the introducer
	if !(SelfHost == IntroducerHost && SelfPort == IntroducerPort) {
		introducerAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", IntroducerHost, IntroducerPort))
		if err != nil {
			log.Fatalf("resolve introducer: %v", err)
		}
		initial := Message{
			MessageType: JoinReq,
			Self:        &self,
			Members:     nil, // omitted due to ,omitempty
		}
		sendUDP(conn, introducerAddr, &initial)
		log.Printf("sent %s to %s:%d", initial.MessageType, IntroducerHost, IntroducerPort)
	}

	// Gossip
	// go gossip(conn, TimeUnit)

	// Ping/Ack
	// go ping(conn, TimeUnit)

	if Protocol.Load() == "gossip" {
		go gossip(conn, TimeUnit)
	} else {
		go ping(conn, TimeUnit)
	}

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
		members := snapshotMembers(false)
		fmt.Printf("Membership List:\n")
		for id, m := range members {
			fmt.Printf("%s - Status: %s, Heartbeat: %d, LastUpdated: %d\n", id, m.Status, m.Heartbeat, m.LastUpdated)
		}

	case "list_self":
		fmt.Println(selfId)

	case "switch":
		if len(fields) != 3 {
			fmt.Println("Usage: switch {gossip|ping} {suspect|nosuspect}")
			return
		}
		proto := fields[1]
		mode := fields[2]
		if proto != "gossip" && proto != "ping" {
			fmt.Println("Invalid protocol:", proto)
			return
		}
		if mode != "suspect" && mode != "nosuspect" {
			fmt.Println("Invalid mode:", mode)
			return
		}

		oldProto := Protocol.Load()
		oldMode := SuspectMode.Load()
		Protocol.Store(proto)
		SuspectMode.Store(mode)

		if oldProto != proto || oldMode != mode {
			go broadcastSwitch(conn)

			if oldMode == "suspect" && mode == "nosuspect" {
				// Promote suspected members to alive
				membershipList.Range(func(k, v any) bool {
					m := v.(Member)
					if m.Status == Suspected {
						m.Status = Alive
						m.LastUpdated = tick
						membershipList.Store(k.(string), m)
					}
					return true
				})
			}

			// Restart goroutine even if mode changes
			if proto == "gossip" {
				go gossip(conn, TimeUnit)
			} else {
				go ping(conn, TimeUnit)
			}
		}

	case "display_protocol":
		fmt.Printf("%s %s\n", Protocol.Load(), SuspectMode.Load())

	case "display_suspects":
		members := snapshotMembers(false)
		fmt.Printf("Suspected Members:\n")
		for id, m := range members {
			if m.Status == Suspected {
				fmt.Printf("%s - Status: %s, Heartbeat: %d, LastUpdated: %d\n", id, m.Status, m.Heartbeat, m.LastUpdated)
			}
		}

	default:
		fmt.Println("Unknown command:", line)
	}
}
