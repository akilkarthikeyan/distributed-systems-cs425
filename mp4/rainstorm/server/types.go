package main

import (
	"encoding/json"
	"time"
)

type MessageType string

var SelfHost string
var SelfNode Process

const (
	Join              MessageType = "join"
	Ack               MessageType = "ack"
	SpawnTaskRequest  MessageType = "spawn_task_request"
	SpawnTaskResponse MessageType = "spawn_task_response"
)

type ProcessType string

const (
	Node ProcessType = "node"
	Task ProcessType = "task"
)

type OpType string

const (
	SourceOp OpType = "source"
	SinkOp   OpType = "sink"
	OtherOp  OpType = "other"
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

type SpawnTaskRequestPayload struct {
	// opExe path
	// opExe args
	// op type
	// HyDFS source file --> if task is a source task
	// HyDFS dest file --> if task is part of last stage
	// Input rate --> if source task
	// Also send LW, HW if autoscale is enabled
	// AutoScaleEnabled --> bool
	// ExactlyOnce --> bool
	OpPath           string   `json:"opPath"`
	OpArgs           []string `json:"opArgs"`
	OpType           string   `json:"opType"`
	InputRate        int      `json:"inputRate,omitempty"`
	HyDFSSourceFile  string   `json:"hydfsSourceFile,omitempty"`
	HyDFSDestFile    string   `json:"hydfsDestFile,omitempty"`
	AutoScaleEnabled bool     `json:"autoScaleEnabled,omitempty"`
	ExactlyOnce      bool     `json:"exactlyOnce,omitempty"`
	LW               int      `json:"lw,omitempty"`
	HW               int      `json:"hw,omitempty"`
}

type SpawnTaskResponsePayload struct {
	// PID
	// IP
	// Port
	Success bool   `json:"success"`
	PID     int    `json:"pid"`
	IP      string `json:"ip"`
	Port    int    `json:"port"`
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
