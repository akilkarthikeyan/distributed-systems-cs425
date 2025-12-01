package main

import "fmt"

func GetProcessAddress(node *Process) string {
	return fmt.Sprintf("%s:%d", node.IP, node.Port)
}
