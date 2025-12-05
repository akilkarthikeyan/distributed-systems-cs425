package main

import (
	"encoding/json"
	"time"
)

const (
	LeaderHost        = "fa25-cs425-9501.cs.illinois.edu"
	LeaderPort        = 9000
	HeartBeatTimeUnit = time.Second
	ForwardTimeUnit   = 100 * time.Millisecond
	StreamTimeUnit    = time.Second
)

type MessageType string

const (
	HeartBeat      MessageType = "heartbeat"
	StartTransfer  MessageType = "startTransfer"
	ChangeTransfer MessageType = "changeTransfer"
	Tuple          MessageType = "tuple"
	Ack            MessageType = "ack"
)

type OpType string

const (
	SourceOp OpType = "source"
	SinkOp   OpType = "sink"
	OtherOp  OpType = "other"
)

type ProcessType string

const (
	Node ProcessType = "node"
	Task ProcessType = "task"
)

type Process struct {
	WhoAmI ProcessType `json:"whoAmI"`
	IP     string      `json:"ip"`
	Port   int         `json:"port"`
}

type Message struct {
	MessageType MessageType     `json:"messageType"`
	From        *Process        `json:"self,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

type HeartBeatPayload struct {
	Stage     int `json:"stage"`
	TaskIndex int `json:"taskIndex"`
}

type TransferPayload struct {
	Successors map[int]Process `json:"successors"`
}

type TuplePayload struct {
	Key   string `json:"key"`
	Value string `json:"value"` // assume this is a string without newline
}

type AckPayload struct {
	Key string `json:"key"`
}

var SelfHost string
var SelfTask Process
