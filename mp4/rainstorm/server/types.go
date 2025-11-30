package main

import "encoding/json"

type MessageType string

var selfHost string

const (
	Join MessageType = "join" // sent to leader to join RainStorm
// Gossip          MessageType = "gossip"
// JoinReq         MessageType = "join-req"
// JoinReply       MessageType = "join-reply"
// CreateHyDFSFile MessageType = "create-hydfs-file"
// AppendHyDFSFile MessageType = "append-hydfs-file"
// GetHyDFSFiles   MessageType = "get-hydfs-file"
// Merge           MessageType = "merge"
// MultiAppend     MessageType = "multi-append"
)

type Message struct {
	MessageType MessageType `json:"messageType"`
	// From        *Member         `json:"self,omitempty"`
	Payload json.RawMessage `json:"payload,omitempty"`
}
