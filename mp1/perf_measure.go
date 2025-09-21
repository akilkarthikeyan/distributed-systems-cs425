package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"math"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"sync"
	"time"
)

type GrepArgs struct {
	Options []string
}
type GrepReply struct {
	Host   string
	File   string
	Output []string
	Err    string
}

func callRPCGrep(hostPort string, options []string, timeout time.Duration) ([]string, time.Duration, error) {
	start := time.Now()

	d := net.Dialer{Timeout: timeout}
	conn, err := d.Dial("tcp", hostPort)
	if err != nil {
		return nil, time.Since(start), err
	}
	defer conn.Close()

	client := rpc.NewClient(conn)
	defer client.Close()

	args := &GrepArgs{Options: options}
	var reply GrepReply

	if err := client.Call("GrepService.RunQuery", args, &reply); err != nil {
		return nil, time.Since(start), err
	}

	if reply.Err != "" {
		return nil, time.Since(start), fmt.Errorf(reply.Err)
	}

	return reply.Output, time.Since(start), nil
}

func generateLargeLog(host string, sizeMB int, machineNum int, sshUser string) error {
	// Approximate number of lines based on target size and avg line length (~80 bytes)
	if sizeMB < 1 {
		sizeMB = 1
	}
	linesNeeded := (sizeMB * 1024 * 1024) / 80

	// Generate logs where the server expects them: ~/machine.N.log
	script := fmt.Sprintf(`
        set -e
        cd "$HOME"
        LOGFILE="machine.%d.log"
        rm -f "$LOGFILE"
        # Use simple pattern to reduce CPU load
        for i in $(seq 1 %d); do
            echo "INFO task-$i completed" >> "$LOGFILE"
        done
        # Add just a few ERROR lines
        for i in $(seq 1 50); do
            echo "ERROR failed-$i" >> "$LOGFILE"
        done
        du -h "$LOGFILE"
    `, machineNum, linesNeeded)

	target := host
	if sshUser != "" && !containsAt(host) {
		target = sshUser + "@" + host
	}
	cmd := exec.Command("ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no", target, "bash", "-lc", script)
	_, err := cmd.CombinedOutput()
	return err
}

func containsAt(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '@' {
			return true
		}
	}
	return false
}

// vmNumberFromHost extracts the VM number (last two digits) from host like fa25-cs425-9505.cs.illinois.edu -> 5
func vmNumberFromHost(host string) int {
	// Find the last 4 consecutive digits in the hostname, then mod 100
	n := len(host)
	// scan for a 4-digit sequence
	for i := 0; i <= n-4; i++ {
		var d int
		_, err := fmt.Sscanf(host[i:i+4], "%d", &d)
		if err == nil && d >= 9000 && d <= 9999 { // coarse filter for our hosts
			return d % 100
		}
	}
	return 0
}

func measureLatency(hosts []string, pattern string, trials int, timeout time.Duration) (float64, float64) {
	var latencies []float64

	for trial := 1; trial <= trials; trial++ {
		fmt.Printf("  Trial %d/%d...", trial, trials)

		var wg sync.WaitGroup
		results := make(chan time.Duration, len(hosts))

		for _, host := range hosts {
			wg.Add(1)
			go func(h string) {
				defer wg.Done()
				_, latency, err := callRPCGrep(h+":12345", []string{"-c", pattern}, timeout)
				if err == nil {
					results <- latency
				}
			}(host)
		}

		wg.Wait()
		close(results)

		// Calculate average latency for this trial
		var sum time.Duration
		count := 0
		for lat := range results {
			sum += lat
			count++
		}

		if count > 0 {
			avgMs := float64(sum) / float64(count) / float64(time.Millisecond)
			latencies = append(latencies, avgMs)
			fmt.Printf(" %.2f ms\n", avgMs)
		} else {
			fmt.Println(" FAILED")
		}

		// No delay between trials
	}

	if len(latencies) == 0 {
		return 0, 0
	}

	// Calculate mean and standard deviation
	var sum float64
	for _, lat := range latencies {
		sum += lat
	}
	mean := sum / float64(len(latencies))

	var variance float64
	for _, lat := range latencies {
		variance += math.Pow(lat-mean, 2)
	}
	stddev := math.Sqrt(variance / float64(len(latencies)))

	return mean, stddev
}

func main() {
	// Flags
	var (
		setup     = flag.Bool("setup", false, "generate logs on remote hosts and exit")
		sizeMB    = flag.Int("sizeMB", 60, "log size per host in MB for setup")
		trials    = flag.Int("trials", 5, "number of trials per pattern")
		timeoutMs = flag.Int("timeoutMs", 2500, "RPC dial timeout in milliseconds")
		csvPath   = flag.String("csv", "perf_results.csv", "output CSV path for results")
		sshUser   = flag.String("user", "", "SSH username for remote hosts (e.g., akshatg4)")
	)
	flag.Parse()

	hosts := []string{
		"fa25-cs425-9505.cs.illinois.edu",
		"fa25-cs425-9506.cs.illinois.edu",
		"fa25-cs425-9507.cs.illinois.edu",
		"fa25-cs425-9508.cs.illinois.edu",
	}

	if *setup {
		fmt.Printf("Generating %dMB log files on 4 machines...\n", *sizeMB)
		var wg sync.WaitGroup
		for _, host := range hosts {
			wg.Add(1)
			go func(h string) {
				defer wg.Done()
				num := vmNumberFromHost(h)
				if num == 0 {
					fmt.Printf("Could not parse VM number from %s; skipping\n", h)
					return
				}
				fmt.Printf("Generating log on %s as machine.%d.log...\n", h, num)
				if err := generateLargeLog(h, *sizeMB, num, *sshUser); err != nil {
					fmt.Printf("Error on %s: %v\n", h, err)
				}
			}(host)
		}
		wg.Wait()
		fmt.Println("Setup complete!")
		return
	}

	fmt.Println("Query Latency Performance Test")
	fmt.Println("==============================")
	fmt.Printf("Testing 4 machines with %dMB log files each\n", *sizeMB)
	fmt.Printf("Running %d trials per test case\n\n", *trials)

	testCases := []struct {
		name    string
		pattern string
	}{
		{"Frequent Pattern", "INFO"},
		{"Rare Pattern", "ERROR"},
		{"No Matches", "NOTFOUND"},
	}

	// Prepare CSV writer
	f, err := os.Create(*csvPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot create CSV: %v\n", err)
		os.Exit(1)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	_ = w.Write([]string{"pattern", "avg_ms", "stddev_ms", "trials", "hosts"})

	timeout := time.Duration(*timeoutMs) * time.Millisecond

	fmt.Println("RESULTS:")
	fmt.Println("Pattern\t\t\tAvg (ms)\tStdDev (ms)")
	fmt.Println("-----------------------------------------------")

	for _, tc := range testCases {
		fmt.Printf("\nTesting %s (%s):\n", tc.name, tc.pattern)
		mean, stddev := measureLatency(hosts, tc.pattern, *trials, timeout)
		fmt.Printf("%-20s\t%.2f\t\t%.2f\n", tc.name, mean, stddev)
		_ = w.Write([]string{tc.name + " (" + tc.pattern + ")", fmt.Sprintf("%.2f", mean), fmt.Sprintf("%.2f", stddev), fmt.Sprintf("%d", *trials), fmt.Sprintf("%d", len(hosts))})
		w.Flush()
	}

	fmt.Printf("\nWrote CSV to %s. Use it to plot error bars.\n", *csvPath)
}
