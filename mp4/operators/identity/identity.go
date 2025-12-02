package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		inputTuple := scanner.Text()
		outputTuple := identity(inputTuple)
		fmt.Printf("%s %s %s\n", outputTuple, os.Args[0], os.Args[1])
	}
}

func identity(input string) string {
	return input
}
