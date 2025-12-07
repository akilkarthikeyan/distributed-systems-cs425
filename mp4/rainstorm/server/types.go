package main

import (
	"encoding/json"
	"time"
)

const (
	SelfPort          = 9000
	LeaderHost        = "fa25-cs425-9501.cs.illinois.edu"
	LeaderPort        = 9000
	Tfail             = 3
	TimeUnit          = time.Second
	AutoscaleInterval = 3 * time.Second // spec said within 5 seconds
)

// Message types
type MessageType string

const (
	Join              MessageType = "join"
	Ack               MessageType = "ack"
	SpawnTaskRequest  MessageType = "spawn_task_request"
	SpawnTaskResponse MessageType = "spawn_task_response"
	StartTransfer     MessageType = "startTransfer"
	ChangeTransfer    MessageType = "changeTransfer"
	StopTransfer      MessageType = "stopTransfer"
	Terminate         MessageType = "terminate"
	HeartBeat         MessageType = "heartbeat"
	Kill              MessageType = "kill" // this will do a kill -9 pid
)

// Process types (node vs task)
type ProcessType string

const (
	Node ProcessType = "node"
	Task ProcessType = "task"
)

// Operation types for tasks
type OpType string

const (
	SourceOp OpType = "source"
	SinkOp   OpType = "sink"
	OtherOp  OpType = "other"
)

// Process identifies a node or task endpoint
type Process struct {
	WhoAmI ProcessType `json:"whoAmI"`
	IP     string      `json:"ip"`
	Port   int         `json:"port"`
}

// TaskInfo holds metadata about a task
type TaskInfo struct {
	Stage       int    `json:"stage"`
	TaskIndex   int    `json:"taskIndex"`
	PID         int    `json:"pid"`
	TaskType    OpType `json:"taskType"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	NodeIP      string `json:"nodeIp"`
	NodePort    int    `json:"nodePort"`
	LastUpdated int    `json:"lastUpdated"`
}

// Message is the generic envelope used between processes
type Message struct {
	MessageType MessageType     `json:"messageType"`
	From        *Process        `json:"self,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

// Payloads for specific message types
type HeartBeatPayload struct {
	Stage           int `json:"stage"`
	TaskIndex       int `json:"taskIndex"`
	TuplesPerSecond int `json:"tuplesPerSecond,omitempty"`
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
	OpPath string `json:"opPath"`
	OpArgs string `json:"opArgs"` // space-separated args
	OpType string `json:"opType"`
	// -- The next 2 are the identity of the task being spawned --
	Stage            int    `json:"stage,omitempty"`
	TaskIndex        int    `json:"taskIndex,omitempty"`
	InputRate        int    `json:"inputRate,omitempty"`
	HyDFSSourceFile  string `json:"hydfsSourceFile,omitempty"`
	HyDFSDestFile    string `json:"hydfsDestFile,omitempty"`
	AutoScaleEnabled bool   `json:"autoScaleEnabled,omitempty"`
	ExactlyOnce      bool   `json:"exactlyOnce,omitempty"`
	LW               int    `json:"lw,omitempty"`
	HW               int    `json:"hw,omitempty"`
	RunID            string `json:"runId,omitempty"` // for unique HyDFS files
}

type SpawnTaskResponsePayload struct {
	// PID
	// IP
	// Port
	PID  int    `json:"pid"`
	IP   string `json:"ip"`
	Port int    `json:"port"`
}

type TransferPayload struct {
	Successors map[int]Process `json:"successors"`
}

type TerminatePayload struct {
	Stage     int `json:"stage"`
	TaskIndex int `json:"taskIndex"`
}

type KillPayload struct {
	PID int `json:"pid"`
}

type AckPayload struct {
	Success bool `json:"success"`
}

// Global state for this process
var SelfHost string
var SelfNode Process
var Tick int

var Nodes []Process
var Tasks map[int]map[int]TaskInfo                 // stage -> taskIndex -> TaskInfo, protected by tasksMu
var StageInputRates map[int]map[int]*InputRateData // stage -> taskIndex -> InputRateData, protected by stageRatesMu

// Leader / application configuration (used if this process is the leader)
var AmILeader bool
var Nstages int
var NtasksPerStage int
var OpPaths []string
var OpArgsList []string
var ExactlyOnce bool
var AutoScaleEnabled bool
var InputRate int
var LW int
var HW int
var HyDFSSourceFile string
var HyDFSDestFile string

// For autoscaling
type InputRateData struct {
	rates []int // circular buffer of last 3 rates
	index int   // current write position
	count int   // number of entries we have
}
