package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var inputWriter *bufio.Writer
var udpConn *net.UDPConn

var (
	// Configuration flags
	opPath       string
	opArgsString string // Holds the single string from --opArgs
	opTypeStr    string
	opType       OpType

	stage     int
	taskIndex int

	inputRate int

	hydfsSourceFile string
	hydfsDestFile   string

	port int

	autoScaleEnabled bool
	lw               int
	hw               int

	exactlyOnce bool

	Received  sync.Map // For exactly-once processing
	processed sync.Map // For exactly-once processing
	acked     sync.Map // For exactly-once processing

	Stored sync.Map // to keep track of what has been stored in persistent storage

	processedButNotAcked map[string]string // processed - acked
	mu                   sync.Mutex        // protects processedButNotAcked

	successors   map[int]Process // key: taskIndex, only present if opType is not SinkOp
	sinkFlusher  *HyDFSFlusher
	ackedFlusher *HyDFSFlusher

	IsSourceDone atomic.Bool // to indicate source task is done sending tuples

	Dir          string
	rainstormRun string // identifier for rainstorm run

	tuplesThisSecond atomic.Int64
)

const Delimiter = "&"

func sendUDP(addr *net.UDPAddr, msg *Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("send udp marshal error: %v\n", err)
		return
	}
	if _, err := udpConn.WriteToUDP(data, addr); err != nil {
		log.Printf("send udp write error: %v\n", err)
	}
}

func listenUDP() {
	buf := make([]byte, 4096)
	for {
		n, raddr, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("listen udp read error: %v\n", err)
			continue
		}
		var msg Message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			log.Printf("listen udp unmarshal from %v error: %v\n", raddr, err)
			continue
		}
		handleMessage(&msg, nil)
	}
}

func sendTCP(addr string, msg *Message) (*Message, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	// Send request
	if err := encoder.Encode(msg); err != nil {
		return nil, err
	}

	// Wait for response
	var resp Message
	if err := decoder.Decode(&resp); err != nil {
		return nil, err
	}

	return &resp, nil
}

func listenTCP(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("listen tcp accept error: %v", err)
			continue
		}
		// Handle each client in a separate goroutine
		go handleTCPClient(conn)
	}
}

func handleTCPClient(conn net.Conn) {
	defer conn.Close()
	decoder := json.NewDecoder(conn)
	encoder := json.NewEncoder(conn)

	for {
		var msg Message
		if err := decoder.Decode(&msg); err != nil {
			if errors.Is(err, io.EOF) {
				// Connection closed by client, so return
			} else {
				log.Printf("handle tcp decode from %v error: %v\n", conn.RemoteAddr(), err)
			}
			return
		}
		handleMessage(&msg, encoder)
	}
}

func handleMessage(msg *Message, encoder *json.Encoder) {
	if msg.MessageType == Ack {
	} else if msg.MessageType == Tuple {
	} else {
		log.Printf("[INFO] recv %s from %s %s\n", msg.MessageType, msg.From.WhoAmI, GetProcessAddress(msg.From))
	}

	switch msg.MessageType {
	// TCP message
	case StartTransfer:
		var payload TransferPayload
		json.Unmarshal(msg.Payload, &payload)
		if opType != SinkOp {
			successors = payload.Successors
			log.Printf("[INFO] updated successors")
		}
		if opType == SourceOp {
			req := GetHttpRequest{HyDFSFilename: hydfsSourceFile, LocalFilename: fmt.Sprintf("%s/%s", Dir, hydfsSourceFile)}
			_, err := SendPostRequest("/get", req)
			if err != nil {
				log.Printf("error getting source file from HyDFS: %v", err)
			}
			go streamTuples(StreamTimeUnit)
		}
		encoder.Encode(&Message{
			MessageType: Ack,
			From:        &SelfTask,
		})

	// TCP message
	case ChangeTransfer:
		var payload TransferPayload
		json.Unmarshal(msg.Payload, &payload)
		if opType != SinkOp {
			successors = payload.Successors
			log.Printf("[INFO] updated successors")
		}
		encoder.Encode(&Message{
			MessageType: Ack,
			From:        &SelfTask,
		})

	// TCP message
	case StopTransfer:
		if opPath == "../bin/aggregate" {
			// Aggregate emits after receiving ---END---; mark done once we see output (only for non-sink)
			processTuple("---END---", inputWriter)
		} else {
			IsSourceDone.Store(true)
		}
		encoder.Encode(&Message{
			MessageType: Ack,
			From:        &SelfTask,
		})
		if opType == SinkOp {
			// sleep a bit to allow sinkFlusher to flush remaining data
			time.Sleep(2500 * time.Millisecond) // more than long enough I think

			// Send terminate to leader as TCP
			payload := TerminatePayload{
				Stage:     stage,
				TaskIndex: taskIndex,
			}
			payloadBytes, _ := json.Marshal(payload)
			msg := &Message{
				MessageType: Terminate,
				From:        &SelfTask,
				Payload:     payloadBytes,
			}
			leaderAddrStr := fmt.Sprintf("%s:%d", LeaderHost, LeaderPort)
			sendTCP(leaderAddrStr, msg)
			log.Printf("[TERMINATING] sent Terminate for stage %d task %d to leader\n", stage, taskIndex)

			// Commit suicide
			cleanup()
		}

	// UDP message
	case Tuple:
		tuplesThisSecond.Add(1)
		var payload TuplePayload
		json.Unmarshal(msg.Payload, &payload)

		log.Printf("[INFO] recv tuple key=%s from %s %s\n", payload.Key, msg.From.WhoAmI, GetProcessAddress(msg.From))

		targetAddr, err := net.ResolveUDPAddr("udp", GetProcessAddress(msg.From))
		if err != nil {
			log.Printf("resolve target addr error for ACK: %v\n", err)
			return
		}

		if opPath == "../bin/aggregate" {
			if exactlyOnce {
				_, isReceived := Received.Load(payload.Key)
				if isReceived {
					payload := AckPayload{Key: payload.Key}
					payloadBytes, _ := json.Marshal(payload)
					ackMsg := &Message{
						MessageType: Ack,
						From:        &SelfTask,
						Payload:     payloadBytes,
					}
					sendUDP(targetAddr, ackMsg)
					log.Printf("[INFO] sent ACK for tuple key=%s to task %s\n", payload.Key, GetProcessAddress(msg.From))
				} else {
					Received.Store(payload.Key, GetProcessAddress(msg.From))
					tuple := fmt.Sprintf("%s%s%s", payload.Key, Delimiter, payload.Value)
					processTuple(tuple, inputWriter)
					payload := AckPayload{Key: payload.Key}
					payloadBytes, _ := json.Marshal(payload)
					ackMsg := &Message{
						MessageType: Ack,
						From:        &SelfTask,
						Payload:     payloadBytes,
					}
					sendUDP(targetAddr, ackMsg)
					log.Printf("[INFO] sent ACK for tuple key=%s to task %s\n", payload.Key, GetProcessAddress(msg.From))
				}
			} else { // atleast once
				tuple := fmt.Sprintf("%s%s%s", payload.Key, Delimiter, payload.Value)
				processTuple(tuple, inputWriter)
				payload := AckPayload{Key: payload.Key}
				payloadBytes, _ := json.Marshal(payload)
				ackMsg := &Message{
					MessageType: Ack,
					From:        &SelfTask,
					Payload:     payloadBytes,
				}
				sendUDP(targetAddr, ackMsg)
				log.Printf("[INFO] sent ACK for tuple key=%s to task %s\n", payload.Key, GetProcessAddress(msg.From))
			}
		} else if opPath == "../bin/transform" || opPath == "../bin/identity" || opPath == "../bin/filter" {
			Received.Store(payload.Key, GetProcessAddress(msg.From)) // for processedFlusher and ackedFlusher and startOutputReader
			if exactlyOnce {
				_, isStored := Stored.Load(payload.Key)
				if isStored {
					payload := AckPayload{Key: payload.Key}
					payloadBytes, _ := json.Marshal(payload)
					ackMsg := &Message{
						MessageType: Ack,
						From:        &SelfTask,
						Payload:     payloadBytes,
					}
					sendUDP(targetAddr, ackMsg)
					log.Printf("[INFO] sent ACK for tuple key=%s to task %s\n", payload.Key, GetProcessAddress(msg.From))
				} else {
					_, isProcessed := processed.Load(payload.Key)
					if isProcessed {
						// do nothing
					} else {
						// process
						tuple := fmt.Sprintf("%s%s%s", payload.Key, Delimiter, payload.Value)
						processTuple(tuple, inputWriter)
					}
				}
			} else {
				// process
				tuple := fmt.Sprintf("%s%s%s", payload.Key, Delimiter, payload.Value)
				processTuple(tuple, inputWriter)
			}
		}

	// UDP message
	case Ack:
		var payload AckPayload
		json.Unmarshal(msg.Payload, &payload)

		log.Printf("[INFO] recv ack for tuple key=%s from %s %s\n", payload.Key, msg.From.WhoAmI, GetProcessAddress(msg.From))

		if exactlyOnce {
			mu.Lock()
			_, found := processedButNotAcked[payload.Key]
			if found {
				acked.Store(payload.Key, "")
				// store in persistent storage
				ackedFlusher.Append(payload.Key)
				delete(processedButNotAcked, payload.Key)
			}
			mu.Unlock()
		} else {
			mu.Lock()
			delete(processedButNotAcked, payload.Key)
			mu.Unlock()

			// send ACK back to original sender of tuple
			targetAddrStr, ok := Received.Load(payload.Key)
			if ok {
				targetAddr, err := net.ResolveUDPAddr("udp", targetAddrStr.(string))
				if err != nil {
					log.Printf("resolve target addr error for ACK back to original sender: %v\n", err)
					return
				}
				payload := &AckPayload{Key: payload.Key}
				payloadBytes, _ := json.Marshal(payload)
				ackMsg := &Message{
					MessageType: Ack,
					From:        &SelfTask,
					Payload:     payloadBytes,
				}
				sendUDP(targetAddr, ackMsg)
				log.Printf("[INFO] sent ACK for tuple key=%s to task %s\n", payload.Key, targetAddrStr.(string))
			}
		}
	}
}

func main() {
	pid := os.Getpid()

	flag.StringVar(&opPath, "opPath", "", "Path to the external op_exe executable.")
	flag.StringVar(&opArgsString, "opArgs", "", "Space-separated arguments for the op_exe.")
	flag.StringVar(&opTypeStr, "opType", "", "Type of operator (Source, Filter, Sink, etc.).")

	flag.IntVar(&stage, "stage", 0, "Stage number of this task.")
	flag.IntVar(&taskIndex, "taskIndex", 0, "Task index within the stage.")
	flag.StringVar(&rainstormRun, "runId", "", "Unique identifier for the RainStorm run.")

	flag.IntVar(&inputRate, "inputRate", -1, "Input rate (default is -1).")

	flag.StringVar(&hydfsSourceFile, "hydfsSourceFile", "", "HyDFS source file.")
	flag.StringVar(&hydfsDestFile, "hydfsDestFile", "", "HyDFS destination file.")

	flag.IntVar(&port, "port", 9001, "The unique network port for this task.")

	flag.BoolVar(&autoScaleEnabled, "autoscaleEnabled", false, "Flag to enable auto-scaling.")
	flag.IntVar(&lw, "lw", 0, "Low watermark for autoscaling.")
	flag.IntVar(&hw, "hw", 0, "High watermark for autoscaling.")

	flag.BoolVar(&exactlyOnce, "exactlyOnce", false, "Flag to enable exactly-once processing.")

	flag.Parse()

	logFile := fmt.Sprintf("../logs/%s_task_%d.log", rainstormRun, pid)
	os.MkdirAll("../logs", 0755)

	Dir = fmt.Sprintf("/home/anandan3/g95/mp4/rainstorm/temp/%s_task_%d", rainstormRun, pid)
	os.MkdirAll(Dir, 0755)

	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		// fmt.Printf("log file open error: %v", err)
		return
	}
	defer f.Close()
	log.SetOutput(f)

	// Initialize processedButNotAcked !!!
	processedButNotAcked = make(map[string]string)

	if exactlyOnce {
		ackedFlusher = NewHyDFSFlusher(FlushInterval, fmt.Sprintf("%s_task_%d_stage_%d_acked.log", rainstormRun, taskIndex, stage), "acked")
	}

	// Parse opType
	switch strings.ToLower(opTypeStr) {
	case "source":
		opType = SourceOp
	case "sink":
		opType = SinkOp
	default:
		opType = OtherOp
	}

	if opType != SourceOp {
		opArgsSlice := strings.Fields(opArgsString)

		cmd := exec.Command(opPath, opArgsSlice...)

		stdinPipe, _ := cmd.StdinPipe()
		stdoutPipe, _ := cmd.StdoutPipe()

		defer stdinPipe.Close()
		defer cmd.Wait()

		cmd.Start()
		inputWriter = bufio.NewWriter(stdinPipe)

		go startOutputReader(stdoutPipe)
	}

	// Get self hostname
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("get hostname error: %v", err)
		return
	}
	SelfHost = hostname

	SelfTask = Process{
		WhoAmI: Task,
		IP:     SelfHost,
		Port:   port,
	}

	// Init processed, Stored, acked maps, Stored = processed from HyDFS
	if exactlyOnce {
		processedHyDFSLog := fmt.Sprintf("%s_task_%d_stage_%d_processed.log", rainstormRun, taskIndex, stage)
		ackedHyDFSLog := fmt.Sprintf("%s_task_%d_stage_%d_acked.log", rainstormRun, taskIndex, stage)

		req1 := GetHttpRequest{HyDFSFilename: processedHyDFSLog, LocalFilename: fmt.Sprintf("%s/%s", Dir, processedHyDFSLog)}
		_, err = SendPostRequest("/get", req1)
		if err != nil {
			log.Printf("[WARN] file not found in HyDFS: %s", processedHyDFSLog)
		} else {
			file, err := os.Open(fmt.Sprintf("%s/%s", Dir, processedHyDFSLog))
			if err != nil {
				log.Printf("open processed log file error: %v", err)
			} else {
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					line := scanner.Text()
					parts := strings.SplitN(line, Delimiter, 2)
					key := parts[0]
					value := parts[1]

					processed.Store(key, value)
					Stored.Store(key, "")
				}
				file.Close()
				// delete the file (cause we use same name in flusher)
				os.Remove(fmt.Sprintf("%s/%s", Dir, processedHyDFSLog))
			}
		}
		req2 := GetHttpRequest{HyDFSFilename: ackedHyDFSLog, LocalFilename: fmt.Sprintf("%s/%s", Dir, ackedHyDFSLog)}
		_, err = SendPostRequest("/get", req2)
		if err != nil {
			log.Printf("[WARN] file not found in HyDFS: %s", ackedHyDFSLog)
		} else {
			file, err := os.Open(fmt.Sprintf("%s/%s", Dir, ackedHyDFSLog))
			if err != nil {
				log.Printf("open acked log file error: %v", err)
			} else {
				scanner := bufio.NewScanner(file)
				for scanner.Scan() {
					key := scanner.Text()
					acked.Store(key, "")
				}
				file.Close()
				// delete the file (cause we use same name in flusher)
				os.Remove(fmt.Sprintf("%s/%s", Dir, ackedHyDFSLog))
			}
		}
		// Initialize processedButNotAcked
		mu.Lock()
		processed.Range(func(key, value any) bool {
			k := key.(string)
			v := value.(string)
			if opPath == "../bin/filter" {
				parts := strings.SplitN(v, Delimiter, 2)
				result := parts[0]
				value := parts[1]
				_, isAcked := acked.Load(k)
				if !isAcked && result == "PASS" {
					processedButNotAcked[k] = value
				}
			} else {
				_, isAcked := acked.Load(k)
				if !isAcked {
					processedButNotAcked[k] = v
				}
			}
			return true
		})
		mu.Unlock()
	}

	// Listen for UDP messages
	listenAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
	if err != nil {
		log.Printf("resolve listenAddr error: %v", err)
		return
	}
	udpConn, err = net.ListenUDP("udp", listenAddr)
	if err != nil {
		log.Printf("udp error: %v", err)
	}
	defer udpConn.Close()

	go listenUDP()

	// Listen for TCP messages
	tcpLn, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		fmt.Printf("tcp error: %v", err)
		return
	}

	go listenTCP(tcpLn)

	go heartbeat(HeartBeatTimeUnit)

	if opType != SinkOp {
		go forwardTuples(ForwardTimeUnit)
	}

	select {}
}

func startOutputReader(stdoutPipe io.Reader) {
	if opType == SinkOp { // can't fail per spec
		sinkFlusher = NewHyDFSFlusher(FlushInterval, hydfsDestFile, "sink")
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			outputTuple := scanner.Text()
			if opPath != "../bin/filter" {
				parts := strings.SplitN(outputTuple, Delimiter, 2)
				key := parts[0]
				value := parts[1]
				processed.Store(key, value)
				sinkFlusher.Append(outputTuple)
			} else {
				// for filter: outputTuple = key&PASS&value or key&FAIL&value
				parts := strings.SplitN(outputTuple, Delimiter, 3)
				key := parts[0]
				result := parts[1]
				value := parts[2]
				processed.Store(key, fmt.Sprintf("%s%s%s", result, Delimiter, value))
				if result == "PASS" {
					sinkFlusher.Append(fmt.Sprintf("%s%s%s", key, Delimiter, value))
				}
			}

			if opPath != "../bin/aggregate" {
				// send ACK back to original sender of tuple
				key := strings.SplitN(outputTuple, Delimiter, 2)[0]
				targetAddrStr, ok := Received.Load(key)
				if ok {
					targetAddr, err := net.ResolveUDPAddr("udp", targetAddrStr.(string))
					if err != nil {
						log.Printf("resolve target addr error for ACK back to original sender: %v\n", err)
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
					log.Printf("[INFO] sent ACK for tuple key=%s to task %s\n", key, targetAddrStr.(string))
				}
			}
		}
	} else {
		if exactlyOnce {
			processedFlusher := NewHyDFSFlusher(FlushInterval, fmt.Sprintf("%s_task_%d_stage_%d_processed.log", rainstormRun, taskIndex, stage), "processed")
			scanner := bufio.NewScanner(stdoutPipe)
			for scanner.Scan() {
				outputTuple := scanner.Text()
				parts := strings.SplitN(outputTuple, Delimiter, 2)
				key := parts[0]
				value := parts[1]
				switch opPath {
				case "../bin/transform", "../bin/identity", "../bin/aggregate":
					processed.Store(key, value)
					// sends tuple forward
					mu.Lock()
					processedButNotAcked[key] = value
					mu.Unlock()
					// stores in persistent storage and send ACK back
					processedFlusher.Append(outputTuple)
				case "../bin/filter":
					// for filter: outputTuple = key&PASS&value or key&FAIL&value
					parts := strings.SplitN(outputTuple, Delimiter, 3)
					key := parts[0]
					result := parts[1]
					value := parts[2]
					if result == "PASS" {
						processed.Store(key, fmt.Sprintf("%s%s%s", result, Delimiter, value))
						// sends tuple forward
						mu.Lock()
						processedButNotAcked[key] = value
						mu.Unlock()
						// stores in persistent storage and send ACK back
						processedFlusher.Append(outputTuple)
					} else { // FAIL
						processed.Store(key, fmt.Sprintf("%s%s%s", result, Delimiter, value))
						// don't send forward
						// store in persistent storage and send ACK back
						processedFlusher.Append(outputTuple)
					}
				}
			}
		} else {
			scanner := bufio.NewScanner(stdoutPipe)
			for scanner.Scan() {
				outputTuple := scanner.Text()
				if opPath == "../bin/filter" {
					// for filter: outputTuple = key&PASS&value or key&FAIL&value
					parts := strings.SplitN(outputTuple, Delimiter, 3)
					key := parts[0]
					result := parts[1]
					value := parts[2]
					if result == "PASS" {
						mu.Lock()
						processedButNotAcked[key] = value
						mu.Unlock()
					} else {
						// don't send forward
						// but send ACK
						targetAddrStr, ok := Received.Load(key)
						if ok {
							targetAddr, err := net.ResolveUDPAddr("udp", targetAddrStr.(string))
							if err != nil {
								log.Printf("resolve target addr error for ACK back to original sender: %v\n", err)
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
							log.Printf("[INFO] sent ACK for tuple key=%s to task %s\n", key, targetAddrStr.(string))
						}
					}
				} else {
					parts := strings.SplitN(outputTuple, Delimiter, 2)
					key := parts[0]
					value := parts[1]
					// sends tuple forward
					mu.Lock()
					processedButNotAcked[key] = value
					mu.Unlock()
				}
			}
		}
	}
}

func processTuple(tuple string, inputWriter *bufio.Writer) {
	fmt.Fprintf(inputWriter, "%s\n", tuple)
	inputWriter.Flush()
}

func heartbeat(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	leaderAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", LeaderHost, LeaderPort))
	if err != nil {
		log.Printf("heartbeat resolve leader addr error: %v\n", err)
		return
	}

	for range ticker.C {
		count := tuplesThisSecond.Swap(0)
		heartBeatPayload := HeartBeatPayload{
			Stage:           stage,
			TaskIndex:       taskIndex,
			TuplesPerSecond: int(count),
		}
		payloadBytes, _ := json.Marshal(heartBeatPayload)

		msg := &Message{
			MessageType: HeartBeat,
			From:        &SelfTask,
			Payload:     payloadBytes,
		}
		sendUDP(leaderAddr, msg)
	}
}

func forwardTuples(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	alreadySet := false // for aggregate op to set IsSourceDone only once (avoid unnecessary atomic ops)

	for {
		<-ticker.C

		if len(successors) == 0 { // no successors
			continue
		}

		mu.Lock()
		// Check if isSourceDone and processedButNotAcked is empty
		if IsSourceDone.Load() && len(processedButNotAcked) == 0 {
			mu.Unlock()
			payload := TerminatePayload{
				Stage:     stage,
				TaskIndex: taskIndex,
			}
			payloadBytes, _ := json.Marshal(payload)

			msg := &Message{
				MessageType: Terminate,
				From:        &SelfTask,
				Payload:     payloadBytes,
			}
			// Send to leader as TCP
			leaderAddrStr := fmt.Sprintf("%s:%d", LeaderHost, LeaderPort)
			sendTCP(leaderAddrStr, msg)

			log.Printf("[TERMINATING] sent Terminate for stage %d task %d to leader\n", stage, taskIndex)

			// Commit suicide
			cleanup()
		}

		// Make a copy to minimize lock time
		localCopy := make(map[string]string, len(processedButNotAcked))
		for k, v := range processedButNotAcked {
			localCopy[k] = v
		}
		mu.Unlock()

		if opPath == "../bin/aggregate" && len(localCopy) != 0 && !alreadySet {
			IsSourceDone.Store(true)
			alreadySet = true
		}

		for key, value := range localCopy {
			tuplePayload := TuplePayload{
				Key:   key,
				Value: value,
			}
			payloadBytes, _ := json.Marshal(tuplePayload)

			msg := &Message{
				MessageType: Tuple,
				From:        &SelfTask,
				Payload:     payloadBytes,
			}

			idx := AssignTask(key, len(successors))
			successor, found := successors[idx]
			if found {
				targetAddr, err := net.ResolveUDPAddr("udp", GetProcessAddress(&successor))
				if err != nil {
					log.Printf("forward resolve target addr error: %v\n", err)
					continue
				}
				sendUDP(targetAddr, msg)
				log.Printf("[INFO] send tuple key=%s to %s\n", key, GetProcessAddress(&successor))
			}
		}
	}
}

func streamTuples(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	localFilename := fmt.Sprintf("%s/%s", Dir, hydfsSourceFile)

	file, err := os.Open(localFilename)
	if err != nil {
		log.Printf("streamTuples open source file error: %v\n", err)
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNumber := 0

	for {
		<-ticker.C

		// Read up to inputRate lines
		for i := 0; i < inputRate; i++ {
			if !scanner.Scan() {
				if err := scanner.Err(); err != nil {
					log.Printf("streamTuples scan error: %v\n", err)
				} else { // Normal EOF
				}
				IsSourceDone.Store(true)
				return
			}

			lineNumber++
			line := scanner.Text()
			// processed.Store(line, line)
			key := fmt.Sprintf("%s_%d", hydfsSourceFile, lineNumber)

			mu.Lock()
			processedButNotAcked[key] = line
			mu.Unlock()
		}
	}
}

func cleanup() {
	// Delete Dir
	os.RemoveAll(Dir)
	os.Exit(0)
}
