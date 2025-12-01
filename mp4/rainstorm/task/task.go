package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"strconv"
)

var inputWriter *bufio.Writer
var udpConn *net.UDPConn

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
	if len(os.Args) < 3 {
		fmt.Printf("insufficient arguments; expected: <op_exe_path> <port> [op_args...]\n")
		return
	}

	opExePath := os.Args[1]
	portStr := os.Args[2]

	var opArgs []string
	if len(os.Args) > 3 {
		opArgs = os.Args[3:]
	}

	// Convert port string to int
	var port int
	if p, err := strconv.Atoi(portStr); err == nil {
		port = p
	} else {
		fmt.Printf("invalid port: %v\n", err)
		return
	}

	cmd := exec.Command(opExePath, opArgs...)

	stdinPipe, _ := cmd.StdinPipe()
	stdoutPipe, _ := cmd.StdoutPipe()

	defer stdinPipe.Close()
	defer cmd.Wait()

	// Start the operator process
	cmd.Start()
	inputWriter = bufio.NewWriter(stdinPipe)

	go startOutputReader(stdoutPipe)

	// Listen for UDP messages
	listenAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", port))
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
