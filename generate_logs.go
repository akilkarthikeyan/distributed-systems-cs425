package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/exec"
	"time"
)

// canonical logs (kept for weighted base generation)
var logTemplates = []string{
	"2025-09-10 12:00:00 ERROR Database connection lost",
	"2025-09-10 12:00:00 INFO Service started",
	"2025-09-10 12:00:00 WARN Disk almost full",
	"2025-09-10 12:00:00 DEBUG Cache miss",
	"2025-09-10 12:00:00 INFO User login",
	"2025-09-10 12:00:00 ERROR Timeout while calling API",
	"2025-09-10 12:00:00 INFO File uploaded",
	"2025-09-10 12:00:00 DEBUG Session expired",
	"2025-09-10 12:00:00 WARN High memory usage",
	"2025-09-10 12:00:00 INFO Healthcheck passed",
}

// weights for frequency distribution (kept)
var weights = []int{
	5,                      // frequent
	3,                      // somewhat frequent
	1, 1, 1, 1, 1, 1, 1, 1, // rare
}

func randomToken(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

var hosts = []string{
	"fa25-cs425-9501.cs.illinois.edu",
	"fa25-cs425-9502.cs.illinois.edu",
	"fa25-cs425-9503.cs.illinois.edu",
	"fa25-cs425-9504.cs.illinois.edu",
	"fa25-cs425-9505.cs.illinois.edu",
	"fa25-cs425-9506.cs.illinois.edu",
	"fa25-cs425-9507.cs.illinois.edu",
	"fa25-cs425-9508.cs.illinois.edu",
	"fa25-cs425-9509.cs.illinois.edu",
	"fa25-cs425-9510.cs.illinois.edu",
}

// generate one log file with weighted canonical lines, extra random noise, and seeded known tokens
func generateLogFile(filename string, baseLines int, randLines int, idx int, tokens struct{ all, some, one, freq string }, someHosts int, freqEven int, freqOdd int) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	// 1) Weighted canonical lines
	total := 0
	for _, w := range weights {
		total += w
	}
	for i := 0; i < baseLines; i++ {
		r := rand.Intn(total)
		sum := 0
		var line string
		for j, w := range weights {
			sum += w
			if r < sum {
				line = logTemplates[j]
				break
			}
		}
		fmt.Fprintln(f, line)
	}

	// 2) Extra random noise lines appended after
	for i := 0; i < randLines; i++ {
		fmt.Fprintln(f, randomToken(48))
	}

	// ALL: present once on every host
	fmt.Fprintf(f, "ALL %s\n", tokens.all)

	// SOME: present only on first N hosts (twice for visibility)
	if idx < someHosts {
		fmt.Fprintf(f, "SOME %s\n", tokens.some)
		fmt.Fprintf(f, "SOME %s\n", tokens.some)
	}

	// ONE: present only on host 0
	if idx == 0 {
		fmt.Fprintf(f, "ONE %s\n", tokens.one)
	}

	// FREQ: frequent on even hosts, less on odd hosts
	count := freqOdd
	if idx%2 == 0 {
		count = freqEven
	}
	for i := 0; i < count; i++ {
		fmt.Fprintf(f, "FREQ %s\n", tokens.freq)
	}

	return nil
}

func main() {
	lines := flag.Int("lines", 256, "number of weighted canonical lines per machine")
	randExtra := flag.Int("rand", 200, "number of extra random lines appended per machine")
	destDir := flag.String("dest", "~/g95", "destination directory on remote machines")
	flag.Parse()

	rand.Seed(time.Now().UnixNano())

	// Shared tokens across hosts
	tokens := struct{ all, some, one, freq string }{
		all:  randomToken(6),
		some: randomToken(6),
		one:  randomToken(6),
		freq: randomToken(6),
	}

	// Category parameters
	someHosts := 3
	freqEven := 50
	freqOdd := 5

	for i, host := range hosts {
		filename := fmt.Sprintf("machine.%d.log", i+1)
		log.Printf("Generating %s with %d base + %d random (ALL=%s SOME=%s ONE=%s FREQ=%s)\n", filename, *lines, *randExtra, tokens.all, tokens.some, tokens.one, tokens.freq)

		if err := generateLogFile(filename, *lines, *randExtra, i, tokens, someHosts, freqEven, freqOdd); err != nil {
			log.Fatalf("Error generating %s: %v", filename, err)
		}

		// scp the file to remote machine
		dest := fmt.Sprintf("%s:%s/%s", host, *destDir, filename)
		cmd := exec.Command("scp", filename, dest)
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Fatalf("Failed to scp %s to %s: %v\nOutput: %s",
				filename, host, err, string(out))
		}
		log.Printf("Copied %s to %s", filename, host)
	}

	log.Println("All log files generated and distributed")
	log.Printf("Use patterns: 'ALL %s', 'SOME %s', 'ONE %s', 'FREQ %s'", tokens.all, tokens.some, tokens.one, tokens.freq)
}
