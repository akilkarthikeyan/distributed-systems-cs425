package main

import (
	"fmt"
	"math/rand/v2"
)

func GetProcessAddress(node *Process) string {
	return fmt.Sprintf("%s:%d", node.IP, node.Port)
}

func AssignNode(stage int, taskIndex int, numNodes int) int {
	return rand.IntN(numNodes)
}

func GenerateRunID() string {
	const charset = "abcdefghijklmnopqrstuvwxyz"
	b := make([]byte, 5)
	for i := range b {
		b[i] = charset[rand.IntN(len(charset))]
	}
	return string(b)
}
