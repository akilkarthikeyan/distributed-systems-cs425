package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"
)

var udpConn *net.UDPConn
var portList []int
var portMutex sync.Mutex

const taskExePath = "../bin/task"

func popPort() (int, error) {
	portMutex.Lock()
	defer portMutex.Unlock()
	if len(portList) == 0 {
		return 0, fmt.Errorf("no available ports")
	}
	p := portList[0]
	portList = portList[1:]
	return p, nil
}

func sendUDP(addr *net.UDPAddr, msg *Message) {
	data, err := json.Marshal(msg)
	if err != nil {
		fmt.Printf("send udp marshal error: %v", err)
		return
	}
	if _, err := udpConn.WriteToUDP(data, addr); err != nil {
		fmt.Printf("send udp write error: %v", err)
	}
}

func listenUDP() {
	buf := make([]byte, 4096)
	for {
		n, raddr, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			fmt.Printf("listen udp read error: %v", err)
			continue
		}
		var msg Message
		if err := json.Unmarshal(buf[:n], &msg); err != nil {
			fmt.Printf("listen udp unmarshal from %v error: %v", raddr, err)
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
			fmt.Printf("listen tcp accept error: %v", err)
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
				fmt.Printf("handle tcp decode from %v error: %v\n", conn.RemoteAddr(), err)
			}
			return
		}
		handleMessage(&msg, encoder)
	}
}

func handleMessage(msg *Message, encoder *json.Encoder) { // encoder can only be present for TCP messages
	log.Printf("[INFO] recv %s from %s %s\n", msg.MessageType, msg.From.WhoAmI, GetProcessAddress(msg.From))

	switch msg.MessageType {
	// UDP message
	case HeartBeat:
		var payload HeartBeatPayload
		json.Unmarshal(msg.Payload, &payload)
		tasksByStage, exists := Tasks[payload.Stage]
		if !exists {
			return
		}
		taskInfo, exists := tasksByStage[payload.TaskIndex]
		if !exists {
			return
		}
		taskInfo.LastUpdated = Tick
		Tasks[payload.Stage][payload.TaskIndex] = taskInfo

	// TCP message
	case Join: // will recv join if you are the leader
		// add node to list of nodes
		NewNode := *msg.From
		Nodes = append(Nodes, NewNode)
		// send ack back to joining node
		ackMsg := &Message{
			MessageType: Ack,
			From:        &SelfNode,
		}
		encoder.Encode(ackMsg)

	// TCP message
	case SpawnTaskRequest:
		var payload SpawnTaskRequestPayload
		json.Unmarshal(msg.Payload, &payload)
		port, _ := popPort()
		args := []string{
			"--port", fmt.Sprintf("%d", port),
			"--opPath", payload.OpPath,
			"--opArgs", payload.OpArgs,
			"--opType", payload.OpType,
			"--stage", fmt.Sprintf("%d", payload.Stage),
			"--taskIndex", fmt.Sprintf("%d", payload.TaskIndex),
			// boolean flags
			"--autoscaleEnabled=" + fmt.Sprintf("%t", payload.AutoScaleEnabled),
			"--exactlyOnce=" + fmt.Sprintf("%t", payload.ExactlyOnce),
		}

		if payload.OpType == string(SourceOp) {
			args = append(args, "--hydfsSourceFile", payload.HyDFSSourceFile)
			args = append(args, "--inputRate", fmt.Sprintf("%d", payload.InputRate))
		} else if payload.OpType == string(SinkOp) {
			args = append(args, "--hydfsDestFile", payload.HyDFSDestFile)
		}

		if payload.AutoScaleEnabled {
			args = append(args, "--lw", fmt.Sprintf("%d", payload.LW))
			args = append(args, "--hw", fmt.Sprintf("%d", payload.HW))
		}

		cmd := exec.Command(taskExePath, args...)
		if err := cmd.Start(); err != nil {
			fmt.Printf("spawn task exec error: %v", err)
			return
		}

		respPayload := SpawnTaskResponsePayload{
			PID:  cmd.Process.Pid,
			IP:   SelfHost,
			Port: port,
		}
		payloadBytes, _ := json.Marshal(respPayload)
		respMsg := &Message{
			MessageType: SpawnTaskResponse,
			From:        &SelfNode,
			Payload:     payloadBytes,
		}
		encoder.Encode(respMsg)

	default:
		fmt.Printf("unknown message type: %s\n", msg.MessageType)
	}
}

func startRainStorm() {
	// Spawn tasks
	for stage := 0; stage < Nstages; stage++ {
		// Stage 0 tasks are actually Stage 1 tasks, because Stage 0 is the separate source task
		for taskIndex := 0; taskIndex < NtasksPerStage; taskIndex++ {
			opPath := OpPaths[stage]
			opArgs := OpArgsList[stage]
			opType := OtherOp
			if stage == Nstages-1 {
				opType = SinkOp
			}
			reqPayload := SpawnTaskRequestPayload{
				OpPath:           opPath,
				OpArgs:           opArgs,
				OpType:           string(opType),
				Stage:            stage + 1,
				TaskIndex:        taskIndex,
				AutoScaleEnabled: AutoScaleEnabled,
				ExactlyOnce:      ExactlyOnce,
			}
			if stage == Nstages-1 {
				reqPayload.HyDFSDestFile = HyDFSDestFile
			}
			if AutoScaleEnabled {
				reqPayload.LW = LW
				reqPayload.HW = HW
			}

			payloadBytes, _ := json.Marshal(reqPayload)
			reqMsg := &Message{
				MessageType: SpawnTaskRequest,
				From:        &SelfNode,
				Payload:     payloadBytes,
			}
			targetNode := Nodes[AssignNode(stage+1, taskIndex, len(Nodes))]
			response, _ := sendTCP(GetProcessAddress(&targetNode), reqMsg)
			var respPayload SpawnTaskResponsePayload
			json.Unmarshal(response.Payload, &respPayload)

			// Check if the inner map for 'stage' exists, if not, initialize it
			if Tasks[stage+1] == nil {
				Tasks[stage+1] = make(map[int]TaskInfo)
			}

			Tasks[stage+1][taskIndex] = TaskInfo{
				Stage:       stage + 1,
				TaskIndex:   taskIndex,
				PID:         respPayload.PID,
				TaskType:    opType,
				IP:          respPayload.IP,
				Port:        respPayload.Port,
				NodeIP:      targetNode.IP,
				NodePort:    targetNode.Port,
				LastUpdated: Tick,
			}

			log.Printf("[INFO] spawned %s task stage %d index %d at %s:%d with pid %d\n", reqPayload.OpType, stage+1, taskIndex, respPayload.IP, respPayload.Port, respPayload.PID)
		}
	}
	// Spawn source task
	reqPayload := SpawnTaskRequestPayload{
		OpType:          string(SourceOp),
		Stage:           0,
		TaskIndex:       0,
		HyDFSSourceFile: HyDFSSourceFile,
		InputRate:       InputRate,
	}
	payloadBytes, _ := json.Marshal(reqPayload)
	reqMsg := &Message{
		MessageType: SpawnTaskRequest,
		From:        &SelfNode,
		Payload:     payloadBytes,
	}
	targetNode := Nodes[AssignNode(0, 0, len(Nodes))]
	response, _ := sendTCP(GetProcessAddress(&targetNode), reqMsg)
	var respPayload SpawnTaskResponsePayload
	json.Unmarshal(response.Payload, &respPayload)

	if Tasks[0] == nil {
		Tasks[0] = make(map[int]TaskInfo)
	}

	Tasks[0][0] = TaskInfo{
		Stage:       0,
		TaskIndex:   0,
		PID:         respPayload.PID,
		TaskType:    SourceOp,
		IP:          respPayload.IP,
		Port:        respPayload.Port,
		NodeIP:      targetNode.IP,
		NodePort:    targetNode.Port,
		LastUpdated: Tick,
	}

	log.Printf("[INFO] spawned %s task stage %d index %d at %s:%d with pid %d\n", reqPayload.OpType, 0, 0, respPayload.IP, respPayload.Port, respPayload.PID)

	time.Sleep(250 * time.Millisecond) // wait a bit for tasks to be ready

	// Spawning over, now send successor info and start tasks, do this in reverse
	for stage := Nstages; stage >= 1; stage-- {
		for taskIndex := 0; taskIndex < NtasksPerStage; taskIndex++ {
			// prepare successor map
			successors := make(map[int]Process)
			if stage < Nstages {
				for succIndex := 0; succIndex < NtasksPerStage; succIndex++ {
					succTaskInfo := Tasks[stage+1][succIndex]
					successors[succIndex] = Process{
						WhoAmI: Task,
						IP:     succTaskInfo.IP,
						Port:   succTaskInfo.Port,
					}
				}
			}
			transferPayload := TransferPayload{
				Successors: successors,
			}
			payloadBytes, _ := json.Marshal(transferPayload)
			transferMsg := &Message{
				MessageType: StartTransfer,
				From:        &SelfNode,
				Payload:     payloadBytes,
			}
			taskInfo := Tasks[stage][taskIndex]
			taskAddr := fmt.Sprintf("%s:%d", taskInfo.IP, taskInfo.Port)
			_, err := sendTCP(taskAddr, transferMsg)
			if err != nil {
				fmt.Printf("TCP starttransfer to %s error: %v\n", taskAddr, err)
			}
		}
	}

	// Now send startTransfer to source task
	successors := make(map[int]Process)
	for succIndex := 0; succIndex < NtasksPerStage; succIndex++ {
		succTaskInfo := Tasks[1][succIndex] // stage 1 is first stage
		successors[succIndex] = Process{
			WhoAmI: Task,
			IP:     succTaskInfo.IP,
			Port:   succTaskInfo.Port,
		}
	}
	transferPayload := TransferPayload{
		Successors: successors,
	}
	payloadBytes, _ = json.Marshal(transferPayload)
	transferMsg := &Message{
		MessageType: StartTransfer,
		From:        &SelfNode,
		Payload:     payloadBytes,
	}
	sourceTaskInfo := Tasks[0][0]
	sourceTaskAddr := fmt.Sprintf("%s:%d", sourceTaskInfo.IP, sourceTaskInfo.Port)
	_, err := sendTCP(sourceTaskAddr, transferMsg)
	if err != nil {
		fmt.Printf("TCP starttransfer to sourceTask error: %v\n", err)
	}
}

func handleCommand(fields []string) {
	if len(fields) == 0 {
		return // Ignore empty lines
	}
	command := fields[0]

	switch command {
	case "rainstorm":
		// check if leader
		if SelfHost != LeaderHost || SelfPort != LeaderPort {
			fmt.Println("ERROR: Only the leader can handle rainstorm commands")
			return
		}
		// rainstorm <Nstages> <Ntasks> <op1_exe> <op1_numargs> <op1_arg1> <op1_arg2> ... <op2_exe> <op2_numargs> <op2_arg1> ...
		// <hydfs_source_file> <hydfs_dest_file> <exactly_once> <autoscale_enabled> <input_rate> [<lw> <hw>]

		idx := 1

		// Parse Nstages
		var err error
		Nstages, err = strconv.Atoi(fields[idx])
		if err != nil {
			fmt.Println("ERROR:", err)
			return
		}
		idx++

		NtasksPerStage, err = strconv.Atoi(fields[idx])
		if err != nil {
			fmt.Println("ERROR:", err)
			return
		}
		idx++

		for s := 0; s < Nstages; s++ {
			// 1. Operator path
			opPath := fields[idx]
			idx++

			// 2. Number of args for this operator
			numArgs, err := strconv.Atoi(fields[idx])
			if err != nil {
				fmt.Printf("ERROR: invalid numArgs for stage %d: %v\n", s, err)
				return
			}
			idx++

			// 3. Read exactly numArgs args
			args := fields[idx : idx+numArgs]
			idx += numArgs

			// 4. Join them into one space-separated string
			opArgs := strings.Join(args, " ")

			// 5. Save
			OpPaths = append(OpPaths, opPath)
			OpArgsList = append(OpArgsList, opArgs)
		}

		HyDFSSourceFile = fields[idx]
		idx++

		HyDFSDestFile = fields[idx]
		idx++

		ExactlyOnce, err = strconv.ParseBool(fields[idx])
		if err != nil {
			fmt.Println("ERROR:", err)
			return
		}
		idx++

		AutoScaleEnabled, err = strconv.ParseBool(fields[idx])
		if err != nil {
			fmt.Println("ERROR:", err)
			return
		}
		idx++

		InputRate, err = strconv.Atoi(fields[idx])
		if err != nil {
			fmt.Println("ERROR:", err)
			return
		}
		idx++

		if AutoScaleEnabled {
			LW, err = strconv.Atoi(fields[idx])
			if err != nil {
				fmt.Println("ERROR:", err)
				return
			}
			idx++

			HW, err = strconv.Atoi(fields[idx])
			if err != nil {
				fmt.Println("ERROR:", err)
				return
			}
			idx++
		}
		go startRainStorm()

	default:
		fmt.Println("Unknown command:", command)
		return
	}
}

func printUsage() {
	fmt.Println("Enter commands")
	fmt.Println("\nAvailable commands:")
	fmt.Println("  rainstorm <Nstages> <Ntasks> <op1_exe> <op1_numargs> <op1_arg1> <op1_arg2> ... <op2_exe> <op2_numargs> <op2_arg1> ... <hydfs_source_file> <hydfs_dest_file> <exactly_once> <autoscale_enabled> <input_rate> [<lw> <hw>]")
	fmt.Print(">> ")
}

func main() {
	os.MkdirAll("../logs", 0755)
	f, err := os.OpenFile("../logs/server.log", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("log file open error: %v", err)
		return
	}
	defer f.Close()
	log.SetOutput(f)

	// Get self hostname
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("get hostname error: %v", err)
		return
	}
	SelfHost = hostname

	// Initialize top-level map
	Tasks = make(map[int]map[int]TaskInfo)

	// Add yourself to list of nodes
	SelfNode = Process{
		WhoAmI: Node,
		IP:     SelfHost,
		Port:   SelfPort,
	}
	Nodes = append(Nodes, SelfNode)

	// Initialize port list
	portList = make([]int, 0, 9500-9001+1)
	for p := 9001; p <= 9500; p++ {
		portList = append(portList, p)
	}

	// Listen for UDP messages
	listenAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", SelfPort))
	if err != nil {
		fmt.Printf("resolve listenAddr error: %v", err)
		return
	}
	udpConn, err = net.ListenUDP("udp", listenAddr)
	if err != nil {
		fmt.Printf("udp error: %v", err)
	}
	defer udpConn.Close()

	go listenUDP()

	// Listen for TCP messages
	tcpLn, err := net.Listen("tcp", fmt.Sprintf(":%d", SelfPort))
	if err != nil {
		fmt.Printf("tcp error: %v", err)
		return
	}

	go listenTCP(tcpLn)

	AmILeader = (SelfHost == LeaderHost && SelfPort == LeaderPort)

	// Send join msg to leader process if you are not the leader
	if !AmILeader {
		joinMsg := &Message{
			MessageType: Join,
			From:        &SelfNode,
		}
		leaderAddr := fmt.Sprintf("%s:%d", LeaderHost, LeaderPort)
		sendTCP(leaderAddr, joinMsg)
		log.Printf("[INFO] sent %s to %s\n", joinMsg.MessageType, leaderAddr)
	}

	if AmILeader {
		go tick(TimeUnit)
	}

	reader := bufio.NewScanner(os.Stdin)
	printUsage()

	// main loop reads commands from stdin
	for reader.Scan() {
		line := reader.Text()
		fields := strings.Fields(line)
		if len(fields) > 0 {
			handleCommand(fields)
		}
		fmt.Print(">> ")
	}

	if err := reader.Err(); err != nil && err != io.EOF {
		fmt.Printf("Error reading from stdin: %v\n", err)
	}
}

func tick(interval time.Duration) { // maintains time, respawns tasks if needed
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		Tick++

		for stageKey, tasksByStage := range Tasks {
			for taskIndex, taskInfo := range tasksByStage {
				if Tick-taskInfo.LastUpdated <= Tfail {
					continue
				} // task is alive
				log.Printf("[FAIL] stage %d task %d at node %s pid %d failed\n", taskInfo.Stage, taskInfo.TaskIndex, taskInfo.NodeIP, taskInfo.PID)
				opPath := OpPaths[taskInfo.Stage-1]
				opArgs := OpArgsList[taskInfo.Stage-1]
				reqPayload := SpawnTaskRequestPayload{
					OpPath:           opPath,
					OpArgs:           opArgs,
					OpType:           string(taskInfo.TaskType),
					Stage:            taskInfo.Stage,
					TaskIndex:        taskInfo.TaskIndex,
					AutoScaleEnabled: AutoScaleEnabled,
					ExactlyOnce:      ExactlyOnce,
				}

				if taskInfo.TaskType == SinkOp {
					reqPayload.HyDFSDestFile = HyDFSDestFile
				}
				if AutoScaleEnabled {
					reqPayload.LW = LW
					reqPayload.HW = HW
				}

				payloadBytes, _ := json.Marshal(reqPayload)
				reqMsg := &Message{
					MessageType: SpawnTaskRequest,
					From:        &SelfNode,
					Payload:     payloadBytes,
				}
				targetNode := Nodes[AssignNode(taskInfo.Stage, taskInfo.TaskIndex, len(Nodes))]
				response, _ := sendTCP(GetProcessAddress(&targetNode), reqMsg)
				var respPayload SpawnTaskResponsePayload
				json.Unmarshal(response.Payload, &respPayload)
				Tasks[stageKey][taskIndex] = TaskInfo{
					Stage:       taskInfo.Stage,
					TaskIndex:   taskInfo.TaskIndex,
					PID:         respPayload.PID,
					TaskType:    taskInfo.TaskType,
					IP:          respPayload.IP,
					Port:        respPayload.Port,
					NodeIP:      targetNode.IP,
					NodePort:    targetNode.Port,
					LastUpdated: Tick,
				}

				log.Printf("[INFO] respawned %s task stage %d index %d at %s:%d with pid %d\n", reqPayload.OpType, reqPayload.Stage, reqPayload.TaskIndex, respPayload.IP, respPayload.Port, respPayload.PID)
			}
		}
	}
}
