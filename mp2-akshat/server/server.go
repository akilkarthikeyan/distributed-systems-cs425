package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
)

type MsgType string

const (
	MsgPing    MsgType = "PING"
	MsgAck     MsgType = "ACK"
	MsgJoin    MsgType = "JOIN"
	MsgJoinOK  MsgType = "JOIN_OK"
	MsgPingReq MsgType = "PING_REQ"
)

type RemoteGrep int

type GrepResult struct {
	Hostname string
	Output   string
}

func (t *RemoteGrep) Grep(req *[]string, reply *GrepResult) error {
	// Get hostname of this machine
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	reply.Hostname = hostname

	machineNum, err := getMachineNumber(hostname)
	if err != nil {
		return err
	}
	logFile := fmt.Sprintf("/home/shared/logs/vm%d.log", machineNum)
	args := append(*req, logFile)

	// Execute grep command
	cmd := exec.Command("grep", args...)
	output, err := cmd.CombinedOutput()

	reply.Output = string(output)

	if err != nil {
		// exit status 1: no match found
		// this is not considered an error
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() == 1 {
				return nil
			}
		}

		return fmt.Errorf("command execution failed: %v", err)
	}

	log.Printf("Executed command: '%s' on host: %s", *req, hostname)
	return nil
}

func main() {
	port := flag.String("port", "1234", "port number for the program to listen on")
	flag.Parse()

	hostname, err := os.Hostname()
	if err == nil && hostname != "vm9501" {
		request := MembershipRequest{Hostname: hostname, Port: *port}
		var response MembershipList
		err = sendMembershipRequest(request, &response)
		if err != nil {
			log.Fatalf("Failed to send membership request: %v", err)
		}
	}
	// Register the RPC service
	grepServer := new(RemoteGrep)
	rpc.Register(grepServer)

	// Listen for incoming RPC connections
	tcpListener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("Failed to listen on port %s: %v", *port, err)
	}
	defer tcpListener.Close()

	udpAddr, err := net.ResolveUDPAddr("udp", ":"+*port)
	if err != nil {
		log.Fatalf("Failed to resolve UDP address: %v", err)
	}

	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatalf("Failed to listen on UDP port %s: %v", *port, err)
	}
	defer udpConn.Close()

	log.Printf("listening on TCP/UDP port %s", *port)

	go handleUDPMessages(udpConn)

	// Accept and handle incoming connections
	for {
		conn, err := tcpListener.Accept()
		if err != nil {
			log.Printf("Failed to accept connection: %v", err)
			continue
		}

		go func(c net.Conn) {
			defer c.Close()
			log.Printf("Client connected: %s", c.RemoteAddr())
			rpc.ServeConn(c)
			log.Printf("Client disconnected: %s", c.RemoteAddr())
		}(conn)
	}
}
