package main

import (
	"encoding/json"
	"time"
)

type MessageType string

var SelfHost string
var SelfNode Process

const (
	Join MessageType = "join"
	Ack  MessageType = "ack"
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

type TaskInfo struct {
	PID         int    `json:"pid"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	NodeIP      string `json:"nodeIp"`
	NodePort    int    `json:"nodePort"`
	LastUpdated int    `json:"lastUpdated"`
}

type Message struct {
	MessageType MessageType     `json:"messageType"`
	From        *Process        `json:"self,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

const (
	SelfPort   = 9000
	LeaderHost = "fa25-cs425-9501.cs.illinois.edu"
	LeaderPort = 9000
	Tfail      = 5
	TimeUnit   = time.Second
)

type AckPayload struct {
	TupleID string `json:"tupleID"`
}

// Used if this process is the leader

var AutoScaleEnabled bool
var TuplesPerSecond int
var LW int
var HW int
var HyDFSSourceFile string
var HyDFSDestFile string

var Nodes []Process
var Tasks []TaskInfo
