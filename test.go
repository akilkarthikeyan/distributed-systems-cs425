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

// Patterns for testing
var testPatterns = []string{
    "ERROR", // frequent
    "INFO",  // somewhat frequent
    "DEBUG", // infrequent
    "UNIQUE", // appears only in one VM
    "S.*ice", // regex pattern
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

// Run distributed grep client
func runDistributedGrep(pattern string) ([]string, error) {
    cmd := exec.Command("./client", "-e", pattern)
    out, err := cmd.CombinedOutput()
    if err != nil {
        return nil, fmt.Errorf("distributed grep error: %v, output: %s", err, string(out))
    }

    lines := []string{}
    scanner := bufio.NewScanner(bytes.NewReader(out))
    for scanner.Scan() {
        lines = append(lines, scanner.Text())
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
    for _, pattern := range testPatterns {
        log.Printf("=== Testing pattern: %s ===", pattern)
        distLines, err := runDistributedGrep(pattern)
        if err != nil {
            log.Fatalf("Failed to run distributed grep: %v", err)
        }

        for i, host := range hosts {
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
