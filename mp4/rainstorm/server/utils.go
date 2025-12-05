package main

import (
	"fmt"
	"math/rand"
)

func GetProcessAddress(node *Process) string {
	return fmt.Sprintf("%s:%d", node.IP, node.Port)
}

func AssignNode(stage int, taskIndex int, numNodes int) int {
	return rand.Intn(numNodes)
}
