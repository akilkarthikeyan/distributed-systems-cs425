package main

import (
	"encoding/json"
	"time"
)

const (
	LeaderHost = "fa25-cs425-9501.cs.illinois.edu"
	LeaderPort = 9000
	TimeUnit   = time.Second
)

type MessageType string

const (
	HeartBeat MessageType = "heartbeat"
	Ack       MessageType = "ack"
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

type AckPayload struct {
	TupleID string `json:"tupleID"`
}

var SelfHost string
var SelfTask Process
