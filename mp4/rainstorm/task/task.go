package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/exec"
	"strings"
)

var inputWriter *bufio.Writer
var udpConn *net.UDPConn

var (
	// Configuration flags
	opPath       string
	opArgsString string // Holds the single string from --opArgs
	opTypeStr    string
	opType       OpType

	inputRate int

	hydfsSourceFile string
	hydfsDestFile   string

	port int

	autoScaleEnabled bool
	lw               int
	hw               int

	exactlyOnce bool
)

func handleMessage(msg *Message, inputWriter *bufio.Writer) {

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

func main() {
	pid := os.Getpid()
	logFile := fmt.Sprintf("../logs/task.%d.log", pid)

	os.MkdirAll("../logs", 0755)
	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Printf("log file open error: %v", err)
		return
	}
	defer f.Close()
	log.SetOutput(f)

	flag.StringVar(&opPath, "opPath", "", "Path to the external op_exe executable.")
	flag.StringVar(&opArgsString, "opArgs", "", "Space-separated arguments for the op_exe.")
	flag.StringVar(&opTypeStr, "opType", "", "Type of operator (Source, Filter, Sink, etc.).")

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

	inputWriter = bufio.NewWriter(stdinPipe)

	go startOutputReader(stdoutPipe)

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

	// to test
	sendTuple("OMMALE", inputWriter)
	sendTuple("YOYO HONEY SINGH", inputWriter)

	select {}
}

func startOutputReader(stdoutPipe io.Reader) {
	scanner := bufio.NewScanner(stdoutPipe)
	for scanner.Scan() {
		outputTuple := scanner.Text()
		fmt.Println("Processed Output:", outputTuple)
	}
}

func sendTuple(tuple string, inputWriter *bufio.Writer) {
	fmt.Fprintf(inputWriter, "%s\n", tuple)
	inputWriter.Flush()
}

// TODO: send heartbeat to leader periodically
// TODO: what to do if you are the source task
// TODO: handle exactly-once semantics
