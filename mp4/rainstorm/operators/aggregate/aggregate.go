package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const delimiter = "&"

var counts map[string]int

func main() {
	n, _ := strconv.Atoi(os.Args[1]) // nth column
	counts = make(map[string]int)

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		inputTuple := scanner.Text()

		// Check for END signal
		if inputTuple == "---END---" {
			// Print results: nthcolumn,count
			for key, count := range counts {
				value := fmt.Sprintf("%s,%d", key, count)
				fmt.Printf("%s%s%s\n", key, delimiter, value)
			}
		} else {
			parts := strings.SplitN(inputTuple, delimiter, 2)
			// key := parts[0] // we can discard the key and have nth column as key
			value := parts[1]
			aggregate(value, n)
		}
	}
}

func aggregate(input string, n int) { // 1-based nth column
	fields := strings.Split(input, ",")
	if n > 0 && n <= len(fields) {
		key := fields[n-1] // Convert to 0-based index
		counts[key]++
	}
}
