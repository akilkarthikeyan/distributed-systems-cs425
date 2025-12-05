package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const delimiter = "&"

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		inputTuple := scanner.Text()
		parts := strings.SplitN(inputTuple, delimiter, 2)
		key := parts[0]
		value := parts[1]
		outputTuple := identity(value)
		fmt.Printf("%s%s%s\n", key, delimiter, outputTuple)
	}
}

func identity(input string) string {
	return input
}
