package main

import "encoding/json"

type MessageType string

const (
	Ack MessageType = "ack"
)

type Task struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
	PID  int    `json:"pid"`
}

type Message struct {
	MessageType MessageType     `json:"messageType"`
	From        *Task           `json:"self,omitempty"`
	Payload     json.RawMessage `json:"payload,omitempty"`
}

type AckPayload struct {
	TupleID string `json:"tupleID"`
}
