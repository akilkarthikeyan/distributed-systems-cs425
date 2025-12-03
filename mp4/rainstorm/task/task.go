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
)

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

// I don't think we'll need this but just in case
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

// I don't think we'll need this but just in case
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

func handleMessage(msg *Message, encoder *json.Encoder) {

}

func main() {
	pid := os.Getpid()
	logFile := fmt.Sprintf("../logs/task.%d.log", pid)

	os.MkdirAll("../logs", 0755)
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		// fmt.Printf("log file open error: %v", err)
		return
	}
	defer f.Close()
	log.SetOutput(f)

	flag.StringVar(&opPath, "opPath", "", "Path to the external op_exe executable.")
	flag.StringVar(&opArgsString, "opArgs", "", "Space-separated arguments for the op_exe.")
	flag.StringVar(&opTypeStr, "opType", "", "Type of operator (Source, Filter, Sink, etc.).")

	flag.IntVar(&stage, "stage", 0, "Stage number of this task.")
	flag.IntVar(&taskIndex, "taskIndex", 0, "Task index within the stage.")

	flag.IntVar(&inputRate, "inputRate", -1, "Input rate (default is -1).")

	flag.StringVar(&hydfsSourceFile, "hydfsSourceFile", "", "HyDFS source file.")
	flag.StringVar(&hydfsDestFile, "hydfsDestFile", "", "HyDFS destination file.")

	flag.IntVar(&port, "port", 9001, "The unique network port for this task.")

	flag.BoolVar(&autoScaleEnabled, "autoscaleEnabled", false, "Flag to enable auto-scaling.")
	flag.IntVar(&lw, "lw", 0, "Low watermark for autoscaling.")
	flag.IntVar(&hw, "hw", 0, "High watermark for autoscaling.")

	flag.BoolVar(&exactlyOnce, "exactlyOnce", false, "Flag to enable exactly-once processing.")

	flag.Parse()

	// Parse opType
	switch strings.ToLower(opTypeStr) {
	case "source":
		opType = SourceOp
	case "sink":
		opType = SinkOp
	default:
		opType = OtherOp
	}

	opArgsSlice := strings.Fields(opArgsString)

	cmd := exec.Command(opPath, opArgsSlice...)

	stdinPipe, _ := cmd.StdinPipe()
	stdoutPipe, _ := cmd.StdoutPipe()

	defer stdinPipe.Close()
	defer cmd.Wait()

	cmd.Start()
	inputWriter = bufio.NewWriter(stdinPipe)

	go startOutputReader(stdoutPipe)

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

	go heartbeat(TimeUnit)

	// // to test
	// sendTuple("OMMALE", inputWriter)
	// sendTuple("YOYO HONEY SINGH", inputWriter)

	select {}
}

func startOutputReader(stdoutPipe io.Reader) {
	scanner := bufio.NewScanner(stdoutPipe)
	for scanner.Scan() {
		outputTuple := scanner.Text()
		log.Println("Processed Output:", outputTuple)
	}
}

func sendTuple(tuple string, inputWriter *bufio.Writer) {
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

	heartBeatPayload := HeartBeatPayload{
		Stage:     stage,
		TaskIndex: taskIndex,
	}
	payloadBytes, _ := json.Marshal(heartBeatPayload)

	for {
		<-ticker.C
		msg := &Message{
			MessageType: HeartBeat,
			From:        &SelfTask,
			Payload:     payloadBytes,
		}
		sendUDP(leaderAddr, msg)
	}
}

// TODO: send heartbeat to leader periodically
// TODO: what to do if you are the source task
// TODO: handle exactly-once semantics
