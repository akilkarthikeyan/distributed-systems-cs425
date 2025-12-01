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
		fmt.Println(outputTuple)
	}
}

func identity(input string) string {
	return input
}
