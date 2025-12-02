package main

import (
	"fmt"
	"hash/fnv"
	"strconv"
)

func GetProcessAddress(node *Process) string {
	return fmt.Sprintf("%s:%d", node.IP, node.Port)
}

func AssignNode(stage int, taskIndex int, numNodes int) int {
	// 1. Create a Unique Key string
	key := GetTaskKey(stage, taskIndex)

	// 2. Apply a Strong Hash (FNV-1a 64-bit is fast and standard)
	h := fnv.New64a()
	h.Write([]byte(key))
	hashValue := h.Sum64()

	// 3. Modulo Assignment
	return int(hashValue % uint64(numNodes))
}

func GetTaskKey(stage int, taskIndex int) string {
	return strconv.Itoa(stage) + "_" + strconv.Itoa(taskIndex)
}
