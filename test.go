package main

import (
	"bufio"
	"bytes"
	"fmt"
	"log"
	"os/exec"
	"strings"
)

// Hosts and their log files
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

// Patterns for testing with descriptions
var testPatterns = map[string]string{
	"ERROR":  "frequent",
	"INFO":   "somewhat frequent",
	"DEBUG":  "infrequent",
	"UNIQUE": "appears only in one VM",
	"S.*ice": "regex pattern",
}

// Check if server is running on given host:port (12345)
func isServerRunning(host string) bool {
	cmd := exec.Command("ssh", host, "nc -z -w 2 localhost 12345")
	if err := cmd.Run(); err != nil {
		return false
	}
	return true
}

// Run local grep on one log file via ssh
func runLocalGrep(host, logfile, pattern string) ([]string, error) {
	cmd := exec.Command("ssh", host, fmt.Sprintf("grep '%s' %s", pattern, logfile))
	out, err := cmd.CombinedOutput()
	if err != nil {
		// grep exits 1 if no match, not a real error
		if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
			return []string{}, nil
		}
		return nil, fmt.Errorf("ssh/grep error: %v, output: %s", err, string(out))
	}

	lines := []string{}
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}
	return lines, nil
}

// Run distributed grep client2 with --show and extract only sample lines
func runDistributedGrep(activeHosts []string, pattern string) ([]string, error) {
	// Build --show argument like "9501,9502,..."
	hostIDs := []string{}
	for _, h := range activeHosts {
		parts := strings.Split(h, "-")
		id := parts[len(parts)-1] // e.g. "9501"
		hostIDs = append(hostIDs, id)
	}
	showArg := strings.Join(hostIDs, ",")

	cmd := exec.Command("./client2", "--show", showArg, pattern)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("distributed grep error: %v, output: %s", err, string(out))
	}

	lines := []string{}
	scanner := bufio.NewScanner(bytes.NewReader(out))

	// Only capture lines between "-- Sample Output Lines --" and "-- End Samples --"
	inSamples := false
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "-- Sample Output Lines --") {
			inSamples = true
			continue
		}
		if strings.HasPrefix(line, "-- End Samples --") {
			inSamples = false
			break
		}
		if inSamples {
			lines = append(lines, line)
		}
	}
	return lines, nil
}

// Filter distributed output by host
func filterByHost(distLines []string, host string) []string {
	filtered := []string{}
	for _, line := range distLines {
		if strings.HasPrefix(line, host+":") {
			filtered = append(filtered, line)
		}
	}
	return filtered
}

// Extract just the log content from distributed output
func extractLogContent(distLines []string) []string {
	contents := []string{}
	for _, line := range distLines {
		parts := strings.SplitN(line, ":", 3)
		if len(parts) == 3 {
			contents = append(contents, parts[2])
		}
	}
	return contents
}

// Compare two string slices
func compareSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func main() {
	activeHosts := []string{}
	for _, host := range hosts {
		if isServerRunning(host) {
			activeHosts = append(activeHosts, host)
		} else {
			log.Printf("Skipping %s (no server running on port 12345)", host)
		}
	}

	if len(activeHosts) == 0 {
		log.Fatal("No active hosts with server running. Exiting.")
	}

	for pattern, desc := range testPatterns {
		log.Printf("=== Testing pattern: %s (%s) ===", pattern, desc)
		distLines, err := runDistributedGrep(activeHosts, pattern)
		if err != nil {
			log.Fatalf("Failed to run distributed grep: %v", err)
		}

		for i, host := range activeHosts {
			logfile := fmt.Sprintf("/home/anandan3/g95/machine.%d.log", i+1)
			localLines, err := runLocalGrep(host, logfile, pattern)
			if err != nil {
				log.Fatalf("Local grep failed on %s: %v", host, err)
			}

			hostDistLines := filterByHost(distLines, host)
			hostDistContent := extractLogContent(hostDistLines)

			if compareSlices(localLines, hostDistContent) {
				log.Printf("✅ %s: OK", host)
			} else {
				log.Printf("❌ %s: MISMATCH!", host)
				log.Printf("Local: %v", localLines)
				log.Printf("Distributed: %v", hostDistContent)
			}
		}
	}

	log.Println("All tests finished.")
}
