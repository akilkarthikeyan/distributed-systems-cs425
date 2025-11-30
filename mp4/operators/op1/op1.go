package main

import (
	"bufio"
	"fmt"
	"os"
)

func main() {
	// Create a scanner to read from stdin (the pipe from the Task Process)
	scanner := bufio.NewScanner(os.Stdin)

	// Loop over every line/tuple sent to stdin
	for scanner.Scan() {
		inputTuple := scanner.Text()

		// The transformation logic: Append a string
		outputTuple := identity(inputTuple)

		// Write the result to stdout (the pipe read by the Task Process)
		fmt.Println(outputTuple)
	}

	// Important: The program exits when stdin is closed by the Task Process,
	// which also closes stdout, signaling the Task Process that output is complete.
}

func identity(input string) string {
	return input
}
