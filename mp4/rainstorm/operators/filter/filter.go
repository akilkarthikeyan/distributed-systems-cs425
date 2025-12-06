package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

const delimiter = "&"

func main() {
	pattern := os.Args[1]

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		inputTuple := scanner.Text()
		parts := strings.SplitN(inputTuple, delimiter, 2)
		key := parts[0]
		value := parts[1]
		processed := filter(value, pattern)
		fmt.Printf("%s%s%s\n", key, delimiter, processed)
	}
}

func filter(input string, pattern string) string { // outputs PASS&value or FAIL&value
	if strings.Contains(input, pattern) {
		return "PASS&" + input
	}
	return "FAIL&" + input
}
