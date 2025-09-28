package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type QueryOutput struct {
	Hostname string
	Address  string
	Output   string
	Error    error
}

type GrepResult struct {
	Hostname string
	Output   string
}

func main() {
	// hardcoded the path to the file containing IPs of all machines
	const filePath = "/home/shared/sources.json"

	IPs, err := generateSliceFromJson(filePath)
	if err != nil {
		log.Fatalf("Failed to load IPs: %v", err)
	}

	if len(IPs) == 0 {
		log.Fatal("No IP addresses found in the file")
	}

	args := os.Args[1:]
	if len(args) == 0 {
		log.Fatal("Usage: program <grep arguments> [--outdir <path>]")
	}

	outDir := "/home/shared/outputs"

	for i, arg := range args {
		if arg == "--outdir" && i+1 < len(args) {
			outDir = args[i+1]
			// strip outdir args so they aren’t sent to grep
			args = append(args[:i], args[i+2:]...)
			break
		}
	}

	if err := os.MkdirAll(outDir, 0755); err != nil {
		log.Fatalf("Failed to create output dir: %v", err)
	}
	entries, err := os.ReadDir(outDir)
	if err != nil {
		log.Fatalf("Failed to read output dir: %v", err)
	}
	for _, entry := range entries {
		os.RemoveAll(filepath.Join(outDir, entry.Name()))
	}

	getLogAll(IPs, args, outDir)
}

func getLogAll(addresses []string, req []string, outDir string) {
	// a list with length equals to the total number of machines
	results := make([]QueryOutput, len(addresses))
	var wg sync.WaitGroup

	start := time.Now()
	// create a new thread for each machine
	for i, address := range addresses {
		wg.Add(1)

		// each thread writes to its own index
		go func(index int, addr string) {
			defer wg.Done()

			result := QueryOutput{
				Address: addr,
			}

			// Connect to the RPC server
			conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
			if err != nil {
				result.Error = fmt.Errorf("connection timeout/failed: %v", err)
				result.Hostname = "Unknown" // Default when can't connect
				results[index] = result
				return
			}
			client := rpc.NewClient(conn)
			defer client.Close()

			var reply GrepResult

			// rpc call
			call := client.Go("RemoteGrep.Grep", &req, &reply, nil)
			select {
			case <-time.After(5 * time.Second):
				result.Error = fmt.Errorf("rpc call timed out")
				result.Hostname = "Unknown"
			case res := <-call.Done:
				if res.Error != nil {
					result.Error = fmt.Errorf("grep failed: %v", err)
					result.Hostname = "Unknown"
				} else {
					result.Hostname = reply.Hostname
					result.Output = reply.Output
				}
			}
			results[index] = result
		}(i, address)
	}

	wg.Wait()
	elapsed := time.Since(start)

	printFormattedResult(results, elapsed, outDir)
}

func printFormattedResult(results []QueryOutput, elapsed time.Duration, outDir string) {
	successCount := 0
	failureCount := 0
	totalLines := 0
	headLines := 5 // Number of lines to show from each machine's result

	for _, result := range results {
		fmt.Printf("%s, %s\n", result.Hostname, result.Address)
		fmt.Println("---------------")

		filename := fmt.Sprintf("%s_%s.txt",
			strings.ReplaceAll(result.Hostname, " ", "_"),
			strings.ReplaceAll(result.Address, ":", "_"),
		)
		filepath := filepath.Join(outDir, filename)

		if result.Error != nil {
			fmt.Printf("ERROR: %v\n", result.Error)
			os.WriteFile(filepath, []byte("ERROR: "+result.Error.Error()), 0644)
			failureCount++
		} else if result.Output == "" {
			fmt.Println("No matches")
			os.WriteFile(filepath, []byte("No matches"), 0644)
			successCount++
		} else {
			lines := strings.Split(strings.TrimSpace(result.Output), "\n")
			lineCount := 0
			for _, line := range lines {
				if line != "" {
					lineCount++
				}
			}
			totalLines += lineCount

			fmt.Printf("%d lines\n", lineCount)

			// Save full output to file
			err := os.WriteFile(filepath, []byte(result.Output), 0644)
			if err != nil {
				fmt.Printf("Failed to write file %s: %v\n", filepath, err)
			}

			// Show preview on console (first N lines)
			displayCount := headLines
			if lineCount < headLines {
				displayCount = lineCount
			}
			for i := 0; i < displayCount; i++ {
				fmt.Println(lines[i])
			}
			if lineCount > headLines {
				fmt.Printf("... (%d more lines)\n", lineCount-headLines)
			}
			successCount++
		}

		fmt.Println()
		fmt.Println()
	}

	// Minimal summary
	fmt.Println("===============")
	fmt.Printf("Summary: %d/%d successful\n", successCount, len(results))
	fmt.Printf("Total lines: %d\n", totalLines)
	fmt.Printf("Latency: %v\n", elapsed)
}

func generateSliceFromJson(filePath string) ([]string, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	var list []string
	err = json.Unmarshal(file, &list)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling JSON: %w", err)
	}

	return list, nil
}
