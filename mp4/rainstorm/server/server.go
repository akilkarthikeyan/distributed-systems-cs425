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
	"syscall"
	"time"
)

var udpConn *net.UDPConn
var portList []int
var portMutex sync.Mutex
var tasksMu sync.RWMutex
var stageRatesMu sync.RWMutex
var rainStormRun string // identifier for rainstorm run

var tickDone chan struct{}
var autoscaleDone chan struct{}

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

		if !AutoScaleEnabled {
			tasksMu.Lock()
			tasksByStage, exists := Tasks[payload.Stage]
			if !exists {
				tasksMu.Unlock()
				return
			}
			taskInfo, exists := tasksByStage[payload.TaskIndex]
			if !exists {
				tasksMu.Unlock()
				return
			}
			taskInfo.LastUpdated = Tick
			Tasks[payload.Stage][payload.TaskIndex] = taskInfo
			tasksMu.Unlock()
		} else {
			stageRatesMu.Lock()
			if StageInputRates[payload.Stage] == nil {
				StageInputRates[payload.Stage] = make(map[int]*InputRateData)
			}
			if StageInputRates[payload.Stage][payload.TaskIndex] == nil {
				StageInputRates[payload.Stage][payload.TaskIndex] = &InputRateData{
					rates: make([]int, 3),
				}
			}

			data := StageInputRates[payload.Stage][payload.TaskIndex]
			data.rates[data.index] = payload.TuplesPerSecond
			data.index = (data.index + 1) % 3
			if data.count < 3 {
				data.count++
			}
			stageRatesMu.Unlock()
		}

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
			"--runId", payload.RunID,
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

	// TCP message
	case Terminate:
		var payload TerminatePayload
		json.Unmarshal(msg.Payload, &payload)

		// Delete the terminated task from the stage
		tasksMu.Lock()
		tasksByStage, exists := Tasks[payload.Stage]
		if exists {
			delete(tasksByStage, payload.TaskIndex)
			Tasks[payload.Stage] = tasksByStage
		}
		tasksMu.Unlock()

		// Check if all tasks in this stage have terminated
		tasksMu.RLock()
		tasksByStage, exists = Tasks[payload.Stage]
		numTasksRemaining := 0
		if exists {
			numTasksRemaining = len(tasksByStage)
		}
		tasksMu.RUnlock()

		// Send ACK back dumbo
		encoder.Encode(&Message{
			MessageType: Ack,
			From:        &SelfNode,
		})

		// for Autoscale, once source terminates, turn autoscale off
		if payload.Stage == 0 && AutoScaleEnabled {
			if autoscaleDone != nil {
				close(autoscaleDone)
			}

			// kill all tasks
			go func() {
				time.Sleep(500 * time.Millisecond)

				tasksMu.RLock()
				allTasks := make([]TaskInfo, 0)
				for stage := 1; stage <= Nstages; stage++ {
					for _, taskInfo := range Tasks[stage] {
						allTasks = append(allTasks, taskInfo)
					}
				}
				tasksMu.RUnlock()

				for _, taskInfo := range allTasks {
					killPayload := KillPayload{
						PID: taskInfo.PID,
					}
					payloadBytes, _ := json.Marshal(killPayload)
					killMsg := &Message{
						MessageType: Kill,
						From:        &SelfNode,
						Payload:     payloadBytes,
					}
					nodeAddr := fmt.Sprintf("%s:%d", taskInfo.NodeIP, taskInfo.NodePort)
					sendTCP(nodeAddr, killMsg)
				}

				log.Printf("[END] RainStorm run %s ended successfully!\n", rainStormRun)
			}()

			return
		}

		if numTasksRemaining == 0 { // all tasks in this stage have terminated
			// If this is not the last stage, send StopTransfer to next stage tasks
			if payload.Stage < Nstages {
				tasksMu.RLock()
				nextStageTasks := Tasks[payload.Stage+1]
				tasksMu.RUnlock()

				stopTransferMsg := &Message{
					MessageType: StopTransfer,
					From:        &SelfNode,
				}

				for _, taskInfo := range nextStageTasks {
					taskAddr := fmt.Sprintf("%s:%d", taskInfo.IP, taskInfo.Port)
					sendTCP(taskAddr, stopTransferMsg)
					log.Printf("[INFO] sent StopTransfer to stage %d task %d at %s\n", payload.Stage+1, taskInfo.TaskIndex, taskAddr)
				}
			} else {
				log.Printf("[END] RainStorm run %s ended successfully!\n", rainStormRun)

				if !AutoScaleEnabled && tickDone != nil {
					close(tickDone)
				}
			}
		}

	// TCP message
	case Kill:
		var payload KillPayload
		json.Unmarshal(msg.Payload, &payload)

		var ackPayload AckPayload

		// Kill the process with SIGKILL
		err := syscall.Kill(payload.PID, syscall.SIGKILL)
		if err != nil {
			fmt.Printf("kill process %d error: %v\n", payload.PID, err)
			// send ACK with success=false
			ackPayload.Success = false
		} else {
			// send ACK with success=true
			ackPayload.Success = true
			log.Printf("[KILL] killed process with PID %d\n", payload.PID)
		}
		payloadBytes, _ := json.Marshal(ackPayload)
		ackMsg := &Message{
			MessageType: Ack,
			From:        &SelfNode,
			Payload:     payloadBytes,
		}
		encoder.Encode(ackMsg)

	default:
		fmt.Printf("unknown message type: %s\n", msg.MessageType)
	}
}

func startRainStorm() {
	rainStormRun = GenerateRunID()

	log.Printf("[START] starting RainStorm run %s with %d stages and %d tasks per stage\n", rainStormRun, Nstages, NtasksPerStage)

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
				RunID:            rainStormRun,
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

			tasksMu.Lock()
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
			tasksMu.Unlock()

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
		RunID:           rainStormRun,
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

	tasksMu.Lock()
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
	tasksMu.Unlock()

	log.Printf("[INFO] spawned %s task stage %d index %d at %s:%d with pid %d\n", reqPayload.OpType, 0, 0, respPayload.IP, respPayload.Port, respPayload.PID)

	time.Sleep(2 * time.Second) // wait a bit for tasks to be ready

	// Spawning over, now send successor info and start tasks, do this in reverse
	for stage := Nstages; stage >= 1; stage-- {
		for taskIndex := 0; taskIndex < NtasksPerStage; taskIndex++ {
			// prepare successor map
			tasksMu.RLock()
			succTasks := Tasks[stage+1]
			taskInfo := Tasks[stage][taskIndex]
			tasksMu.RUnlock()

			successors := make(map[int]Process)
			if stage < Nstages {
				for succIndex := 0; succIndex < NtasksPerStage; succIndex++ {
					succTaskInfo := succTasks[succIndex]
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
			taskAddr := fmt.Sprintf("%s:%d", taskInfo.IP, taskInfo.Port)
			_, err := sendTCP(taskAddr, transferMsg)
			if err != nil {
				fmt.Printf("TCP starttransfer to %s error: %v\n", taskAddr, err)
			}
			log.Printf("[INFO] sent startTransfer to stage %d task %d at %s\n", stage, taskIndex, taskAddr)
		}
	}

	// Now send startTransfer to source task
	tasksMu.RLock()
	succTasks := Tasks[1]
	sourceTaskInfo := Tasks[0][0]
	tasksMu.RUnlock()

	successors := make(map[int]Process)
	for succIndex := 0; succIndex < NtasksPerStage; succIndex++ {
		succTaskInfo := succTasks[succIndex] // stage 1 is first stage
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
	sourceTaskAddr := fmt.Sprintf("%s:%d", sourceTaskInfo.IP, sourceTaskInfo.Port)
	_, err := sendTCP(sourceTaskAddr, transferMsg)
	if err != nil {
		fmt.Printf("TCP starttransfer to sourceTask error: %v\n", err)
	}
	log.Printf("[INFO] sent startTransfer to source task at %s\n", sourceTaskAddr)
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

		if ExactlyOnce && AutoScaleEnabled {
			fmt.Println("ERROR: ExactlyOnce and AutoScaleEnabled cannot both be true")
			return
		}

		if AmILeader {
			if !AutoScaleEnabled {
				tickDone = make(chan struct{})
				go tick(TimeUnit)
			} else {
				autoscaleDone = make(chan struct{})
				go autoscale(AutoscaleInterval)
			}
		}

		go startRainStorm()

	case "kill_task":
		if len(fields) != 3 {
			fmt.Println("Usage: kill_task <VM> <PID>")
			return
		}
		vmAddr := fields[1] // in format IP:Port
		pid, err := strconv.Atoi(fields[2])
		if err != nil {
			fmt.Println("ERROR:", err)
			return
		}

		killPayload := KillPayload{
			PID: pid,
		}
		payloadBytes, _ := json.Marshal(killPayload)
		killMsg := &Message{
			MessageType: Kill,
			From:        &SelfNode,
			Payload:     payloadBytes,
		}
		_, err = sendTCP(vmAddr, killMsg)
		if err != nil {
			fmt.Printf("send kill to %s error: %v\n", vmAddr, err)
			return
		}
		fmt.Printf("Sent kill for PID %d to %s\n", pid, vmAddr)

	case "list_tasks":
		tasksMu.RLock()
		for stage := 0; stage <= Nstages; stage++ {
			if Tasks[stage] == nil {
				continue
			}
			fmt.Printf("Stage %d:\n", stage)
			for idx, t := range Tasks[stage] {
				opExe := "source"
				if stage >= 1 && stage-1 < len(OpPaths) {
					opExe = OpPaths[stage-1]
				}
				logPath := fmt.Sprintf("../logs/%s_task_%d.log", rainStormRun, t.PID)
				fmt.Printf("  Task %d: VM=%s:%d PID=%d op=%s log=%s\n",
					idx, t.NodeIP, t.NodePort, t.PID, opExe, logPath)
			}
			fmt.Println()
		}
		tasksMu.RUnlock()

	default:
		fmt.Println("Unknown command:", command)
		return
	}
}

func printUsage() {
	fmt.Println("Available commands:")
	fmt.Println("  1. rainstorm <Nstages> <Ntasks> <op1_exe> <op1_numargs> <op1_arg1> <op1_arg2> ... <op2_exe> <op2_numargs> <op2_arg1> ... <hydfs_source_file> <hydfs_dest_file> <exactly_once> <autoscale_enabled> <input_rate> [<lw> <hw>]")
	fmt.Println("  2. kill_task <VM> <PID>")
	fmt.Println("  3. list_tasks")
	fmt.Println()
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
	StageInputRates = make(map[int]map[int]*InputRateData) // Add this line

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

	reader := bufio.NewScanner(os.Stdin)
	printUsage()

	// main loop reads commands from stdin
	for reader.Scan() {
		line := reader.Text()
		fields := strings.Fields(line)
		if len(fields) > 0 {
			handleCommand(fields)
		}
		// fmt.Print(">> ")
	}

	if err := reader.Err(); err != nil && err != io.EOF {
		fmt.Printf("Error reading from stdin: %v\n", err)
	}
}

func tick(interval time.Duration) { // maintains time, respawns tasks if needed
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-tickDone:
			return
		case <-ticker.C:
			Tick++
			toRespawn := make([]TaskInfo, 0)
			tasksMu.RLock()
			for _, tasksByStage := range Tasks {
				for _, taskInfo := range tasksByStage {
					if Tick-taskInfo.LastUpdated > Tfail {
						toRespawn = append(toRespawn, taskInfo)
					}
				}
			}
			tasksMu.RUnlock()

			failed := make([]TaskInfo, 0, len(toRespawn))
			for _, taskInfo := range toRespawn {
				log.Printf("[FAIL] stage %d task %d address %s:%d pid %d failed\n", taskInfo.Stage, taskInfo.TaskIndex, taskInfo.IP, taskInfo.Port, taskInfo.PID)

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
					RunID:            rainStormRun,
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

				newInfo := TaskInfo{
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

				tasksMu.Lock()
				if Tasks[taskInfo.Stage] == nil {
					Tasks[taskInfo.Stage] = make(map[int]TaskInfo)
				}
				Tasks[taskInfo.Stage][taskInfo.TaskIndex] = newInfo
				tasksMu.Unlock()

				log.Printf("[INFO] respawned %s task stage %d index %d at %s:%d with pid %d\n", reqPayload.OpType, reqPayload.Stage, reqPayload.TaskIndex, respPayload.IP, respPayload.Port, respPayload.PID)

				failed = append(failed, newInfo)
			}

			if len(failed) == 0 {
				continue
			}

			// send startTransfer to respawned tasks
			go func(failed []TaskInfo) {
				// sleep a bit to allow tasks to be ready
				time.Sleep(1 * time.Second)
				for _, taskInfo := range failed {
					if taskInfo.Stage < Nstages {
						tasksMu.RLock()
						successors := taskInfoMapToProcessMap(Tasks[taskInfo.Stage+1])
						tasksMu.RUnlock()
						transferPayload := TransferPayload{Successors: successors}
						payloadBytes, _ := json.Marshal(transferPayload)
						transferMsg := &Message{
							MessageType: StartTransfer,
							From:        &SelfNode,
							Payload:     payloadBytes,
						}
						taskAddr := fmt.Sprintf("%s:%d", taskInfo.IP, taskInfo.Port)
						_, err := sendTCP(taskAddr, transferMsg)
						if err != nil {
							fmt.Printf("TCP starttransfer to %s error: %v\n", taskAddr, err)
						}
						log.Printf("[INFO] sent startTransfer to stage %d task %d at %s\n", taskInfo.Stage, taskInfo.TaskIndex, taskAddr)
					}
				}
				// also have to update successors of previous stage tasks,
				// get a list of all stages that had failed tasks
				updatedStages := make(map[int]bool)
				for _, taskInfo := range failed {
					if taskInfo.Stage > 0 {
						updatedStages[taskInfo.Stage-1] = true
					}
				}
				for stage := range updatedStages {
					tasksMu.RLock()
					successors := taskInfoMapToProcessMap(Tasks[stage+1])
					stageTasks := Tasks[stage]
					tasksMu.RUnlock()
					transferPayload := TransferPayload{Successors: successors}
					payloadBytes, _ := json.Marshal(transferPayload)
					transferMsg := &Message{
						MessageType: ChangeTransfer,
						From:        &SelfNode,
						Payload:     payloadBytes,
					}
					for _, taskInfo := range stageTasks {
						taskAddr := fmt.Sprintf("%s:%d", taskInfo.IP, taskInfo.Port)
						_, err := sendTCP(taskAddr, transferMsg)
						if err != nil {
							fmt.Printf("TCP changetransfer to %s error: %v\n", taskAddr, err)
						}
						log.Printf("[INFO] sent changeTransfer to stage %d task %d at %s\n", taskInfo.Stage, taskInfo.TaskIndex, taskAddr)
					}
				}
			}(failed)
		}
	}
}

func autoscale(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-autoscaleDone:
			return
		case <-ticker.C:
			Tick++

			// Print summary header
			tasksMu.RLock()
			stage0Count := len(Tasks[0])
			tasksMu.RUnlock()
			fmt.Println("---------AutoScale Summary---------")
			fmt.Printf("Stage 0 tasks: %d\n", stage0Count)

			// Calculate average input rate per stage and make scaling decisions
			for stage := 1; stage <= Nstages; stage++ {
				stageRatesMu.RLock()
				taskRates := StageInputRates[stage]
				if taskRates == nil {
					stageRatesMu.RUnlock()
					continue
				}

				totalRate := 0
				taskCount := 0

				for _, data := range taskRates {
					if data.count > 0 {
						sum := 0
						for i := 0; i < data.count; i++ {
							sum += data.rates[i]
						}
						avgRate := sum / data.count
						totalRate += avgRate
						taskCount++
					}
				}
				stageRatesMu.RUnlock()

				if taskCount == 0 {
					continue
				}

				avgRatePerTask := totalRate / taskCount
				log.Printf("[AUTOSCALE] Stage %d: avg rate per task = %d tuples/sec (LW=%d, HW=%d, tasks=%d)\n",
					stage, avgRatePerTask, LW, HW, taskCount)

				tasksMu.RLock()
				currentTasks := Tasks[stage]
				currentTaskCount := len(currentTasks)
				tasksMu.RUnlock()

				// Print per-stage summary (stdout only)
				fmt.Printf("Stage %d tasks: %d, avg rate per task: %d tuple/sec (LW=%d, HW=%d)\n", stage, currentTaskCount, avgRatePerTask, LW, HW)

				// Scale down: avgRatePerTask < LW and more than 1 task
				if avgRatePerTask < LW && currentTaskCount > 1 {
					log.Printf("[AUTOSCALE] Stage %d scaling DOWN (removing 1 task)\n", stage)
					fmt.Printf("[AUTOSCALE] Stage %d scaling DOWN (removing 1 task)\n", stage)

					// Pick a task to kill (highest index)
					victimTaskIndex := -1
					var victimTask TaskInfo
					tasksMu.RLock()
					for idx, task := range currentTasks {
						if idx > victimTaskIndex {
							victimTaskIndex = idx
							victimTask = task
						}
					}
					tasksMu.RUnlock()

					if victimTaskIndex == -1 {
						continue
					}

					// Remove from Tasks and StageInputRates immediately
					tasksMu.Lock()
					delete(Tasks[stage], victimTaskIndex)
					tasksMu.Unlock()

					stageRatesMu.Lock()
					delete(StageInputRates[stage], victimTaskIndex)
					stageRatesMu.Unlock()

					go func(stage int, victimTask TaskInfo, victimTaskIndex int) {
						killPayload := KillPayload{
							PID: victimTask.PID,
						}
						payloadBytes, _ := json.Marshal(killPayload)
						killMsg := &Message{
							MessageType: Kill,
							From:        &SelfNode,
							Payload:     payloadBytes,
						}
						nodeAddr := fmt.Sprintf("%s:%d", victimTask.NodeIP, victimTask.NodePort)
						_, err := sendTCP(nodeAddr, killMsg)
						if err != nil {
							log.Printf("[ERROR] failed to send kill to %s: %v\n", nodeAddr, err)
						} else {
							log.Printf("[AUTOSCALE] Killed stage %d task %d (PID %d)\n", stage, victimTaskIndex, victimTask.PID)
						}

						// Update predecessors' successors
						time.Sleep(500 * time.Millisecond)

						tasksMu.RLock()
						successors := taskInfoMapToProcessMap(Tasks[stage])
						prevStageTasks := Tasks[stage-1]
						tasksMu.RUnlock()

						transferPayload := TransferPayload{Successors: successors}
						payloadBytes, _ = json.Marshal(transferPayload)
						changeMsg := &Message{
							MessageType: ChangeTransfer,
							From:        &SelfNode,
							Payload:     payloadBytes,
						}

						for _, taskInfo := range prevStageTasks {
							taskAddr := fmt.Sprintf("%s:%d", taskInfo.IP, taskInfo.Port)
							sendTCP(taskAddr, changeMsg)
							log.Printf("[INFO] sent ChangeTransfer to stage %d task %d at %s\n",
								stage-1, taskInfo.TaskIndex, taskAddr)
						}
					}(stage, victimTask, victimTaskIndex)

					// Scale up: avgRatePerTask > HW
				} else if avgRatePerTask > HW {
					log.Printf("[AUTOSCALE] Stage %d scaling UP (adding 1 task)\n", stage)
					fmt.Printf("[AUTOSCALE] Stage %d scaling UP (adding 1 task)\n", stage)

					// Find next available task index
					tasksMu.RLock()
					newTaskIndex := 0
					for idx := range currentTasks {
						if idx >= newTaskIndex {
							newTaskIndex = idx + 1
						}
					}
					tasksMu.RUnlock()

					// Spawn new task
					opPath := OpPaths[stage-1]
					opArgs := OpArgsList[stage-1]
					opType := OtherOp
					if stage == Nstages {
						opType = SinkOp
					}

					reqPayload := SpawnTaskRequestPayload{
						OpPath:           opPath,
						OpArgs:           opArgs,
						OpType:           string(opType),
						Stage:            stage,
						TaskIndex:        newTaskIndex,
						AutoScaleEnabled: AutoScaleEnabled,
						ExactlyOnce:      ExactlyOnce,
						LW:               LW,
						HW:               HW,
						RunID:            rainStormRun,
					}

					if opType == SinkOp {
						reqPayload.HyDFSDestFile = HyDFSDestFile
					}

					payloadBytes, _ := json.Marshal(reqPayload)
					reqMsg := &Message{
						MessageType: SpawnTaskRequest,
						From:        &SelfNode,
						Payload:     payloadBytes,
					}

					targetNode := Nodes[AssignNode(stage, newTaskIndex, len(Nodes))]
					response, err := sendTCP(GetProcessAddress(&targetNode), reqMsg)
					if err != nil {
						log.Printf("[ERROR] failed to spawn task: %v\n", err)
						continue
					}

					var respPayload SpawnTaskResponsePayload
					json.Unmarshal(response.Payload, &respPayload)

					newTaskInfo := TaskInfo{
						Stage:       stage,
						TaskIndex:   newTaskIndex,
						PID:         respPayload.PID,
						TaskType:    opType,
						IP:          respPayload.IP,
						Port:        respPayload.Port,
						NodeIP:      targetNode.IP,
						NodePort:    targetNode.Port,
						LastUpdated: Tick, // don't really use this in autoscale
					}

					tasksMu.Lock()
					Tasks[stage][newTaskIndex] = newTaskInfo
					tasksMu.Unlock()

					log.Printf("[AUTOSCALE] Spawned new stage %d task %d at %s:%d with PID %d\n",
						stage, newTaskIndex, respPayload.IP, respPayload.Port, respPayload.PID)
					fmt.Printf("[AUTOSCALE] Spawned new stage %d task %d at %s:%d with PID %d\n",
						stage, newTaskIndex, respPayload.IP, respPayload.Port, respPayload.PID)

					// Send StartTransfer and update predecessors in goroutine
					go func(stage int, newTaskInfo TaskInfo) {
						time.Sleep(1 * time.Second)

						// Send successors to new task
						if stage < Nstages {
							tasksMu.RLock()
							successors := taskInfoMapToProcessMap(Tasks[stage+1])
							tasksMu.RUnlock()

							transferPayload := TransferPayload{Successors: successors}
							payloadBytes, _ := json.Marshal(transferPayload)
							transferMsg := &Message{
								MessageType: StartTransfer,
								From:        &SelfNode,
								Payload:     payloadBytes,
							}
							taskAddr := fmt.Sprintf("%s:%d", newTaskInfo.IP, newTaskInfo.Port)
							sendTCP(taskAddr, transferMsg)
							log.Printf("[INFO] sent StartTransfer to stage %d task %d at %s\n",
								stage, newTaskInfo.TaskIndex, taskAddr)
						}

						// Update predecessors' successors
						tasksMu.RLock()
						successors := taskInfoMapToProcessMap(Tasks[stage])
						prevStageTasks := Tasks[stage-1]
						tasksMu.RUnlock()

						transferPayload := TransferPayload{Successors: successors}
						payloadBytes, _ := json.Marshal(transferPayload)
						changeMsg := &Message{
							MessageType: ChangeTransfer,
							From:        &SelfNode,
							Payload:     payloadBytes,
						}

						for _, taskInfo := range prevStageTasks {
							taskAddr := fmt.Sprintf("%s:%d", taskInfo.IP, taskInfo.Port)
							sendTCP(taskAddr, changeMsg)
							log.Printf("[INFO] sent ChangeTransfer to stage %d task %d at %s\n",
								stage-1, taskInfo.TaskIndex, taskAddr)
						}
					}(stage, newTaskInfo)
				}
			}
			fmt.Println("---------End of Summary---------")
		}
	}
}

func taskInfoMapToProcessMap(m map[int]TaskInfo) map[int]Process {
	out := make(map[int]Process, len(m))
	for k, v := range m {
		out[k] = Process{WhoAmI: Task, IP: v.IP, Port: v.Port}
	}
	return out
}

// TODO: implement list_tasks, kill_task
// TODO: beautify autoscale outputs to console (also say how many tasks in each stage blah blah)
