package main

import (
	"encoding/json"
	"time"
)

type Status string

const (
	Alive  Status = "alive"
	Failed Status = "failed"
)

type MessageType string

const (
	Gossip    MessageType = "gossip"
	JoinReq   MessageType = "join-req"
	JoinReply MessageType = "join-reply"
	TCPTest   MessageType = "tcp-test" // DELETE
)

type Member struct {
	// Gossip-related fields
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Timestamp   string `json:"timestamp"`
	Heartbeat   int    `json:"heartbeat"`
	LastUpdated int    `json:"lastUpdated"`
	Status      Status `json:"status"`
}

type Message struct {
	MessageType MessageType     `json:"messageType"`
	From        *Member         `json:"self,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

type GossipPayload struct {
	Members map[string]Member `json:"Members"`
}

const (
	SelfPort       = 1234
	IntroducerHost = "fa25-cs425-9501.cs.illinois.edu"
	IntroducerPort = 1234
	Tfail          = 5
	Tcleanup       = 5
	K              = 3
	TimeUnit       = time.Second * 5
)

type HyDFSFile struct {
	Filename     string
	FileRingId   uint64
	Chunks       []string
	ChunkFileMap map[string]string
}
