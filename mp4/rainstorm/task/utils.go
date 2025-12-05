package main

import (
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

func GetProcessAddress(node *Process) string {
	return fmt.Sprintf("%s:%d", node.IP, node.Port)
}

func AssignTask(stage int, taskIndex int, numTasks int) int {
	hashValue := stage*31 + taskIndex
	return hashValue % numTasks
}

// to flush to HyDFS periodically
type HyDFSFlusher struct {
	mu            sync.Mutex
	lines         []string
	interval      time.Duration
	hyDFSFilename string
}

func NewHyDFSFlusher(interval time.Duration, hyDFSFilename string) *HyDFSFlusher {
	f := &HyDFSFlusher{
		lines:         make([]string, 0),
		interval:      interval,
		hyDFSFilename: hyDFSFilename,
	}

	// Start background goroutine
	go f.run()

	return f
}

func (f *HyDFSFlusher) Append(line string) {
	f.mu.Lock()
	f.lines = append(f.lines, line)
	f.mu.Unlock()
}

func (f *HyDFSFlusher) run() {
	ticker := time.NewTicker(f.interval)
	defer ticker.Stop()

	for range ticker.C {
		f.flush()
	}
}

func (f *HyDFSFlusher) flush() {
	f.mu.Lock()

	if len(f.lines) == 0 {
		f.mu.Unlock()
		return
	}

	linesToWrite := make([]string, len(f.lines))
	copy(linesToWrite, f.lines)

	// Clear buffer
	f.lines = f.lines[:0]

	f.mu.Unlock()

	f.writeToFile(linesToWrite)

	// if f.callback != nil {
	// 	f.callback()
	// }
}

func (f *HyDFSFlusher) writeToFile(lines []string) {
	filesDir := "/home/anandan3/g95/mp4/rainstorm/files"

	temp := fmt.Sprintf("%s/%s", filesDir, f.hyDFSFilename)
	file, err := os.Create(temp) // overwrite file each flush
	if err != nil {
		log.Printf("Error creating file: %v", err)
		return
	}
	defer file.Close()

	for _, line := range lines {
		file.WriteString(line + "\n")
	}

	// Post to HyDFS
	req := HyDFSFileRequest{LocalFilename: temp, HyDFSFilename: f.hyDFSFilename}
	_, err = SendPostRequest("/append", req)
	if err != nil {
		log.Printf("error posting to HyDFS: %v", err)
	}
}
