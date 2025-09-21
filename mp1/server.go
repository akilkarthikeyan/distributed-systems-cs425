package main

import (
    "log"
    "net"
    // "os"
    "fmt"
    "net/rpc"
    "os/exec"
    "strings"
    "strconv"
)

// GrepArgs defines what the client sends
type GrepArgs struct {
    Options []string // e.g., ["-e", "ERROR", "-c"]
}

// GrepReply defines what the server sends back
type GrepReply struct {
    Host   string   // hostname of this VM
    File   string   // local log file
    Output []string // matching lines or counts
    Err    string   // error string if any
}

// GrepService exposes RPC methods
type GrepService struct{}

// RunQuery executes grep locally on the VM's log file
func (s *GrepService) RunQuery(args *GrepArgs, reply *GrepReply) error {
    logFile := getLogFile()

    // Construct grep command
    cmd := exec.Command("grep", append(args.Options, logFile)...)

    out, err := cmd.Output()
    if err != nil {
        // grep exits nonzero if no matches — not always a real error
        if _, ok := err.(*exec.ExitError); !ok {
            reply.Err = err.Error()
            return nil
        }
    }

    lines := strings.Split(strings.TrimSpace(string(out)), "\n")
    if len(lines) == 1 && lines[0] == "" {
        lines = []string{} // no matches
    }

    reply.Host = getHostname()
    reply.File = logFile
    reply.Output = lines
    return nil
}

// getHostname returns the machine's hostname
func getHostname() string {
    out, err := exec.Command("hostname").Output()
    if err != nil {
        return "unknown"
    }
    return strings.TrimSpace(string(out))
}

func getLogFile() string {
    hostname := getHostname() // e.g., "fa25-cs425-9504.cs.illinois.edu"

    // Split by "-" and take the last hyphen-separated part
    parts := strings.Split(hostname, "-")
    if len(parts) < 3 {
        return "machine.unknown.log"
    }
    lastPart := parts[len(parts)-1] // e.g., "9504.cs.illinois.edu"

    // Remove the domain part
    lastPart = strings.SplitN(lastPart, ".", 2)[0] // "9504"

    // Convert to integer
    num, err := strconv.Atoi(lastPart)
    if err != nil {
        return "machine.unknown.log"
    }

    // Map 9501 → 1, 9502 → 2, ..., 9510 → 10
    vmNumber := num % 100
    if vmNumber < 1 || vmNumber > 10 {
        return "machine.unknown.log"
    }

    return fmt.Sprintf("machine.%d.log", vmNumber)
}


func main() {
    grepService := new(GrepService)
    rpc.Register(grepService)

    listener, err := net.Listen("tcp", ":12345")
    if err != nil {
        log.Fatal("Listen error:", err)
    }
    log.Println("RPC server listening on port 12345...")

    for {
        conn, err := listener.Accept()
        if err != nil {
            log.Println("Connection accept error:", err)
            continue
        }
        go rpc.ServeConn(conn) // handle RPC requests
    }
}
