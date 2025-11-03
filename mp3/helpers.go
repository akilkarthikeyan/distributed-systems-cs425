package main

import (
	"fmt"
	"math/rand/v2"
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
)

type Member struct {
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Timestamp   string `json:"timestamp"`
	Heartbeat   int    `json:"heartbeat"`
	LastUpdated int    `json:"lastUpdated"`
	Status      Status `json:"status"`
}

type Message struct {
	MessageType MessageType    `json:"messageType"`
	From        *Member        `json:"self,omitempty"`
	Payload     map[string]any `json:"payload,omitempty"`
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

func KeyFor(m Member) string { return fmt.Sprintf("%s:%d:%s", m.IP, m.Port, m.Timestamp) }

func SnapshotMembers(omitFailed bool) map[string]Member {
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

func SelectKMembers(members map[string]Member, k int) []Member {
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
