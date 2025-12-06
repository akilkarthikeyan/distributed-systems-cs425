package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
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
	flusherType   string // "processed" or "acked" or "sink"
	mu            sync.Mutex
	lines         []string
	interval      time.Duration
	hyDFSFilename string
	created       bool
}

func NewHyDFSFlusher(interval time.Duration, hyDFSFilename string, flusherType string) *HyDFSFlusher {
	f := &HyDFSFlusher{
		lines:         make([]string, 0),
		interval:      interval,
		hyDFSFilename: hyDFSFilename,
		flusherType:   flusherType,
	}

	temp := fmt.Sprintf("%s/%s", Dir, hyDFSFilename)

	os.WriteFile(temp, []byte{}, 0644)

	// POST /create
	req := HyDFSFileRequest{
		LocalFilename: temp,
		HyDFSFilename: hyDFSFilename,
	}

	_, err := SendPostRequest("/create", req)
	if err != nil {
		log.Printf("HyDFSFlusher: failed to create file %s: %v", hyDFSFilename, err)
	} else {
		f.created = true
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
	temp := fmt.Sprintf("%s/%s", Dir, f.hyDFSFilename)
	file, err := os.Create(temp) // overwrite file each flush
	if err != nil {
		log.Printf("Error creating file: %v", err)
		return
	}
	defer file.Close()

	switch f.flusherType {
	case "sink":
		for _, line := range lines {
			parts := strings.SplitN(line, Delimiter, 2)
			// key := parts[0]
			value := parts[1]
			file.WriteString(value + "\n")
		}
	case "processed", "acked":
		for _, line := range lines {
			file.WriteString(line + "\n")
		}
	}

	// Post to HyDFS
	req := HyDFSFileRequest{LocalFilename: temp, HyDFSFilename: f.hyDFSFilename}
	_, err = SendPostRequest("/append", req)
	if err != nil {
		log.Printf("error posting to HyDFS: %v", err)
		return
	}

	if f.flusherType == "processed" {
		// record as stored and send ACK back
		for _, line := range lines {
			parts := strings.SplitN(line, Delimiter, 2)
			key := parts[0]
			Stored.Store(key, "")

			targetAddrStr, ok := Received.Load(key)
			if ok {
				targetAddr, err := net.ResolveUDPAddr("udp", targetAddrStr.(string))
				if err != nil {
					log.Printf("resolve target addr error for ACK after storing log: %v\n", err)
					continue
				}
				payload := &AckPayload{Key: key}
				payloadBytes, _ := json.Marshal(payload)
				ackMsg := &Message{
					MessageType: Ack,
					From:        &SelfTask,
					Payload:     payloadBytes,
				}
				sendUDP(targetAddr, ackMsg)
				log.Printf("[INFO] Sent ACK for tuple key=%s to task %s\n", key, targetAddrStr.(string))
			}
		}
	}
}
