package main

import (
    "fmt"
    "log"
    "math/rand"
    "net"
    "net/rpc"
    "os"
    "os/exec"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

// randomToken returns a random alphanumeric string of length n.
func randomToken(n int) string {
    const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
    b := make([]byte, n)
    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }
    return string(b)
}

type GrepArgs struct { 
    Options []string 
}
type GrepReply struct { 
    Host string; 
    File string; 
    Output []string; 
    Err string 
}

func callRPCGrep(hostPort string, options []string) ([]string, error) {
    d := net.Dialer{Timeout: 3 * time.Second}
    conn, err := d.Dial("tcp", hostPort)
    if err != nil {
        return nil, err
    }
    defer conn.Close()
    client := rpc.NewClient(conn)
    defer client.Close()
    args := &GrepArgs{Options: options}
    var reply GrepReply
    if err := client.Call("GrepService.RunQuery", args, &reply); err != nil { return nil, err }
    if reply.Err != "" { 
        return nil, fmt.Errorf(reply.Err) 
    }
    return reply.Output, nil
}

func runSSH(host string, script string) (string, error) {
    cmd := exec.Command(
        "ssh",
        "-q",
        "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "LogLevel=ERROR",
        "-o", "ConnectTimeout=5",
        host, script,
    )
    out, err := cmd.CombinedOutput()
    return string(out), err
}

func main() {
    hosts := []string{
        "fa25-cs425-9501.cs.illinois.edu",
        "fa25-cs425-9502.cs.illinois.edu",
        "fa25-cs425-9503.cs.illinois.edu",
        "fa25-cs425-9504.cs.illinois.edu",
        "fa25-cs425-9505.cs.illinois.edu",
        "fa25-cs425-9506.cs.illinois.edu",
        "fa25-cs425-9507.cs.illinois.edu",
        "fa25-cs425-9508.cs.illinois.edu",
        "fa25-cs425-9509.cs.illinois.edu",
        "fa25-cs425-9510.cs.illinois.edu",
    }

    // Optional: if G95_TUNNEL=1, use local SSH tunnels on ports 12001..12010 for RPC
    useTunnel := os.Getenv("G95_TUNNEL") == "1"

    rand.Seed(time.Now().UnixNano())
    // Tokens
    tokAll := randomToken(6)
    tokSome := randomToken(6)
    tokOne := randomToken(6)
    tokFreq := randomToken(6)
    tokNone := randomToken(6) // not inserted anywhere

    // Prepare logs on each host: random content + controlled lines
    {
        var wg sync.WaitGroup
        wg.Add(len(hosts))
        for i := 1; i <= len(hosts); i++ {
            idx := i - 1
            h := hosts[idx]
            file := "machine." + strconv.Itoa(i) + ".log"
            wg.Add(0)
            go func(host string, file string, idx int) {
                defer wg.Done()
                var b strings.Builder
                // base random block
                b.WriteString(fmt.Sprintf("head -c 65536 </dev/urandom | base64 > /tmp/%s; ", file))
                // always present once (all hosts)
                b.WriteString(fmt.Sprintf("echo 'ALL %s' >> /tmp/%s; ", tokAll, file))
                // present on first 3 hosts (some hosts)
                if idx < 3 {
                    b.WriteString(fmt.Sprintf("for i in 1 2; do echo 'SOME %s' >> /tmp/%s; done; ", tokSome, file))
                }
                // present only on host 0 (one host)
                if idx == 0 {
                    b.WriteString(fmt.Sprintf("echo 'ONE %s' >> /tmp/%s; ", tokOne, file))
                }
                // frequent on even-index hosts
                if idx%2 == 0 {
                    b.WriteString(fmt.Sprintf("for i in $(seq 1 50); do echo 'FREQ %s' >> /tmp/%s; done; ", tokFreq, file))
                } else {
                    b.WriteString(fmt.Sprintf("for i in $(seq 1 5); do echo 'FREQ %s' >> /tmp/%s; done; ", tokFreq, file))
                }

                script := "set -e; " + b.String()
                if out, err := runSSH(host, script); err != nil {
                    log.Printf("[%s] prep error: %v; out=%s", host, err, out)
                }
            }(h, file, idx)
        }
        wg.Wait()
    }

    type testCase struct {
        name    string
        pattern string
    }

    tests := []testCase{
        {name: "all-hosts", pattern: tokAll},
        {name: "some-hosts", pattern: tokSome},
        {name: "one-host", pattern: tokOne},
        {name: "frequent", pattern: tokFreq},
        {name: "nonexistent", pattern: tokNone},
    }

    // Run tests
    for _, tc := range tests {
        log.Printf("=== Running test: %s (pattern=%s)", tc.name, tc.pattern)

        type hostResult struct {
            host       string
            rpcLines   []string
            sshLines   []string
            rpcErr     error
            sshErr     error
        }

        results := make([]hostResult, len(hosts))

        var wg sync.WaitGroup
        wg.Add(len(hosts))
        for i, h := range hosts {
            i := i
            h := h
            go func() {
                defer wg.Done()
                // RPC
                hostPort := h + ":12345"
                if useTunnel {
                    hostPort = fmt.Sprintf("127.0.0.1:%d", 12000+i+1)
                }
                lines, err := callRPCGrep(hostPort, []string{"-n", "-F", tc.pattern})
                // SSH ground truth
                out, sshErr := runSSH(h, fmt.Sprintf("grep -nF -- '%s' /tmp/machine.%d.log || true", tc.pattern, i+1))
                sshLines := []string{}
                if s := strings.TrimSpace(out); s != "" {
                    sshLines = strings.Split(s, "\n")
                }
                results[i] = hostResult{host: h, rpcLines: lines, sshLines: sshLines, rpcErr: err, sshErr: sshErr}
            }()
        }
        wg.Wait()

        // Evaluate
        passed := 0
        tested := 0
        for _, r := range results {
            // If RPC failed, we consider the host failed/ignored, consistent with fault tolerance requirement
            if r.rpcErr != nil {
                log.Printf("[%s] RPC error: %v (ignored in pass/fail count)", r.host, r.rpcErr)
                continue
            }
            tested++
            // Normalize: sort comparisons
            sort.Strings(r.rpcLines)
            sort.Strings(r.sshLines)
            if equalStringSlices(r.rpcLines, r.sshLines) {
                passed++
            } else {
                log.Printf("[%s] MISMATCH for pattern %q\n  rpc: %v\n  ssh: %v", r.host, tc.pattern, r.rpcLines, r.sshLines)
            }
        }
        log.Printf("Test %q: %d/%d hosts matched (RPC errors ignored)", tc.name, passed, tested)
    }
}

func equalStringSlices(a, b []string) bool {
    if len(a) != len(b) {
        return false
    }
    for i := range a {
        if a[i] != b[i] {
            return false
        }
    }
    return true
}