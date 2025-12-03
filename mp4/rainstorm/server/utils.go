package main

import (
	"fmt"
	"math/rand"
	"strconv"
)

func GetProcessAddress(node *Process) string {
	return fmt.Sprintf("%s:%d", node.IP, node.Port)
}

func AssignNode(stage int, taskIndex int, numNodes int) int {
	return rand.Intn(numNodes)
}

func GetTaskKey(stage int, taskIndex int) string {
	return strconv.Itoa(stage) + "_" + strconv.Itoa(taskIndex)
}
