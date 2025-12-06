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
		processed := transform(value)
		fmt.Printf("%s%s%s\n", key, delimiter, processed)
	}
}

func transform(input string) string { // outputs fields 1-3
	fields := strings.Split(input, ",")
	if len(fields) >= 3 {
		return strings.Join(fields[:3], ",")
	}
	return input // Return as-is if fewer than 3 fields
}
