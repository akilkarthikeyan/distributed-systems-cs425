package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
)

var udpConn *net.UDPConn

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
		handleMessage(&msg)
	}
}

func handleMessage(msg *Message) {

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
	// fmt.Println("--- HyDFS Client Interface ---")
	// fmt.Println("Connects to:", HyDFSServerURL)
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

	reader := bufio.NewScanner(os.Stdin)
	printUsage()

	// Get self hostname
	hostname, err := os.Hostname()
	if err != nil {
		fmt.Printf("get hostname error: %v", err)
		return
	}
	selfHost = hostname

	// Main loop reads commands from stdin
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
