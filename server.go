package main

import (
    "log"
    "net"
    "net/rpc"
    "os/exec"
    "strings"
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
    home, err := os.UserHomeDir()
    if err != nil {
        return "machine.1.log" // fallback
    }
    return home + "/machine.1.log"
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
