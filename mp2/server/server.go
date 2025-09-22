package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"regexp"
	"strconv"
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

// log files are named vm#.log based on the machine number
// parse the machine number from hostname
// for example input fa25-cs425-6401.cs.illinois.edu returns 1
func getMachineNumber(hostname string) (int, error) {
	re := regexp.MustCompile(`-(\d{4})\.`)
	match := re.FindStringSubmatch(hostname)
	if len(match) < 2 {
		return 0, fmt.Errorf("no machine number found in hostname: %s", hostname)
	}

	num, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, err
	}

	return num - 6400, nil
}

func handleUDPMessages(conn *net.UDPConn) {
	buf := make([]byte, 1024)

	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("Error reading UDP message: %v", err)
			continue
		}

		// Process message in goroutine to avoid blocking
		go func(data []byte, sender *net.UDPAddr) {
			var message Message
			err := json.Unmarshal(data, &message)
			if err != nil {
				log.Printf("Failed to unmarshal UDP message from %s: %v", sender, err)
				return
			}

			log.Printf("Received UDP message from %s: %+v", sender, message)

			// Handle different message types here
			switch message.Type {
			case "heartbeat":
				// Handle heartbeat
			case "gossip":
				// Handle gossip
			case "ping":
				// Handle ping
			case "ack":
				// Handle ack
			default:
				log.Printf("Unknown message type: %s", message.Type)
			}
		}(buf[:n], addr)
	}
}
