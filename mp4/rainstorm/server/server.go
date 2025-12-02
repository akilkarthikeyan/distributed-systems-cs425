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
	"strings"
	"sync"
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
	log.Printf("recv %s from %s %s", msg.MessageType, msg.From.WhoAmI, GetProcessAddress(msg.From))

	switch msg.MessageType {
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
		// Remove later
		reqPayload := SpawnTaskRequestPayload{
			OpPath:           "../bin/identity",
			OpArgs:           []string{"arg1", "arg2"},
			OpType:           string(OtherOp),
			AutoScaleEnabled: false,
			ExactlyOnce:      false,
		}
		payloadBytes, _ := json.Marshal(reqPayload)
		reqMsg := &Message{
			MessageType: SpawnTaskRequest,
			From:        &SelfNode,
			Payload:     payloadBytes,
		}
		response, _ := sendTCP(GetProcessAddress(msg.From), reqMsg)
		log.Printf("received spawn task response: %+v", response)

	// TCP message
	case SpawnTaskRequest:
		var payload SpawnTaskRequestPayload
		json.Unmarshal(msg.Payload, &payload)
		port, _ := popPort()
		args := []string{
			"--port", fmt.Sprintf("%d", port),
			"--opPath", payload.OpPath,
			"--opArgs", strings.Join(payload.OpArgs, " "),
			"--opType", payload.OpType,
			"--autoscaleEnabled", fmt.Sprintf("%t", payload.AutoScaleEnabled),
			"--exactlyOnce", fmt.Sprintf("%t", payload.ExactlyOnce),
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
			Success: true,
			PID:     cmd.Process.Pid,
			IP:      SelfHost,
			Port:    port,
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

func handleCommand(fields []string) {
	if len(fields) == 0 {
		return // Ignore empty lines
	}
	command := fields[0]

	var body string
	var err error

	switch command {
	case "hydfs_list_mem_ids":
		body, err = SendGetRequest("/list_mem_ids")
		if err == nil {
			var members []HyDFSMemberInfo
			if json.Unmarshal([]byte(body), &members) == nil {
				fmt.Println("--- Membership List ---")
				for _, m := range members {
					fmt.Printf("RingID: %20d | ID: %s | Status: %s | HB: %d | LastUpdated: %d\n", m.RingID, m.ID, m.Status, m.Heartbeat, m.LastUpdated)
				}
			} else {
				fmt.Println("Raw Response:", body)
			}
		}
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
		}

	case "hydfs_list_self":
		body, err = SendGetRequest("/list_self")
		if err == nil {
			fmt.Println("Self ID:", body)
		}
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
		}

	case "hydfs_create":
		if len(fields) != 3 {
			fmt.Println("Usage: hydfs_create <localfilename> <HyDFSfilename>")
			return
		}
		req := HyDFSFileRequest{LocalFilename: fields[1], HyDFSFilename: fields[2]}
		body, err = SendPostRequest("/create", req)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
		} else if command != "list_mem_ids" && command != "ls" && command != "list_self" {
			fmt.Println("SUCCESS:", body)
		}

	case "hydfs_append":
		if len(fields) != 3 {
			fmt.Println("Usage: hydfs_append <localfilename> <HyDFSfilename>")
			return
		}
		req := HyDFSFileRequest{LocalFilename: fields[1], HyDFSFilename: fields[2]}
		body, err = SendPostRequest("/append", req)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
		} else if command != "list_mem_ids" && command != "ls" && command != "list_self" {
			fmt.Println("SUCCESS:", body)
		}

	case "hydfs_get":
		if len(fields) != 3 {
			fmt.Println("Usage: hydfs_get <HyDFSfilename> <localfilename>")
			return
		}
		req := GetHttpRequest{HyDFSFilename: fields[1], LocalFilename: fields[2]}
		body, err = SendPostRequest("/get", req)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
		} else if command != "list_mem_ids" && command != "ls" && command != "list_self" {
			fmt.Println("SUCCESS:", body)
		}

	case "hydfs_merge":
		if len(fields) != 2 {
			fmt.Println("Usage: hydfs_merge <HyDFSfilename>")
			return
		}
		req := MergeHttpRequest{HyDFSFilename: fields[1]}
		body, err = SendPostRequest("/merge", req)
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
		} else if command != "list_mem_ids" && command != "ls" && command != "list_self" {
			fmt.Println("SUCCESS:", body)
		}

	case "hydfs_ls":
		if len(fields) != 2 {
			fmt.Println("Usage: hydfs_ls <HyDFSfilename>")
			return
		}
		filename := fields[1]
		body, err = SendGetRequest(fmt.Sprintf("/ls?filename=%s", filename))
		if err == nil {
			var lsResp LsResponse
			if json.Unmarshal([]byte(body), &lsResp) == nil {
				fmt.Printf("File RingID: %20d\n", lsResp.FileRingID)
				fmt.Println("Replicas:")
				for _, r := range lsResp.Replicas {
					fmt.Printf("  RingID: %20d | ID: %s\n", r.RingID, r.ID)
				}
			} else {
				fmt.Println("Raw Response:", body)
			}
		} else {
			fmt.Printf("ERROR: %v\n", err)
		}

	default:
		fmt.Println("Unknown command:", command)
		return
	}
}

func printUsage() {
	fmt.Println("Enter commands")
	fmt.Println("\nAvailable commands:")
	fmt.Println("  hydfs_list_mem_ids")
	fmt.Println("  hydfs_list_self")
	fmt.Println("  hydfs_create <localfile> <hydfsfile>")
	fmt.Println("  hydfs_append <localfile> <hydfsfile>")
	fmt.Println("  hydfs_get <hydfsfile> <localfile>")
	fmt.Println("  hydfs_merge <hydfsfile>")
	fmt.Println("  hydfs_ls <hydfsfile>")
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

	// Send join msg to leader process if you are not the leader
	if !(SelfHost == LeaderHost && SelfPort == LeaderPort) {
		joinMsg := &Message{
			MessageType: Join,
			From:        &SelfNode,
		}
		leaderAddr := fmt.Sprintf("%s:%d", LeaderHost, LeaderPort)
		sendTCP(leaderAddr, joinMsg)
		log.Printf("sent %s to %s", joinMsg.MessageType, leaderAddr)
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
