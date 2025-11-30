package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os/exec"
)

var inputWriter *bufio.Writer

func connectTCP(addr string) (net.Conn, *json.Encoder, *json.Decoder, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, nil, nil, err
	}

	encoder := json.NewEncoder(conn)
	decoder := json.NewDecoder(conn)

	return conn, encoder, decoder, nil
}

func sendAndReceivePersistent(conn net.Conn, encoder *json.Encoder, decoder *json.Decoder, msg *Message) (*Message, error) {
	// Send request
	if err := encoder.Encode(msg); err != nil {
		conn.Close()
		return nil, err
	}

	// Wait for response
	var resp Message
	if err := decoder.Decode(&resp); err != nil {
		conn.Close()
		return nil, err
	}

	return &resp, nil
}

func main() {
	// Remove this later
	inputTuple := "example_user_record_123"

	opExePath := "./op1"
	cmd := exec.Command(opExePath)

	// Get persistent pipe handles
	stdinPipe, _ := cmd.StdinPipe()
	stdoutPipe, _ := cmd.StdoutPipe()

	defer stdinPipe.Close()
	defer cmd.Wait()

	cmd.Start()

	inputWriter = bufio.NewWriter(stdinPipe)

	go startOutputReader(stdoutPipe)

	// Remove this later
	sendTuple(inputTuple, inputWriter)

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
