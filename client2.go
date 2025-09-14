package main

import (
    "fmt"
    "net"
    "net/rpc"
    "os"
    "sort"
    "strconv"
    "strings"
    "sync"
    "time"
)

// Match the server structs
type GrepArgs struct {
    Options []string // grep flags and pattern, e.g., ["-e", "ERROR"]
}

type GrepReply struct {
    Host   string
    File   string
    Output []string
    Err    string
}

type result struct {
    host   string
    file   string
    lines  []string
    errStr string
}

// parseArgs extracts optional client-only flags and returns (optionsForGrep, showFilters, maxLines, port)
// Supported client flags:
//   --show hostSubstr[,hostSubstr...]   show matching lines only for these hosts; if omitted, do not print lines
//   --max-lines N                        limit lines printed per shown host (default: unlimited)
//   --port P                             RPC port (default: 12345)
func parseArgs(argv []string) (options []string, show []string, maxLines int, port string) {
    options = []string{}
    show = nil
    maxLines = -1
    port = "12345"

    i := 0
    for i < len(argv) {
        a := argv[i]
        switch a {
        case "--show":
            if i+1 < len(argv) {
                show = strings.Split(argv[i+1], ",")
                i += 2
                continue
            }
        case "--max-lines":
            if i+1 < len(argv) {
                if n, err := strconv.Atoi(argv[i+1]); err == nil {
                    maxLines = n
                }
                i += 2
                continue
            }
        case "--port":
            if i+1 < len(argv) {
                port = argv[i+1]
                i += 2
                continue
            }
        default:
            // treat as grep option
            options = append(options, a)
            i++
            continue
        }
        // fallback if malformed flag usage
        options = append(options, a)
        i++
    }
    return
}

func main() {
    if len(os.Args) < 2 {
        fmt.Println("Usage: ./client2 [--show host1,host2] [--max-lines N] [--port 12345] <grep options and pattern>")
        fmt.Println("Examples:")
        fmt.Println("  ./client2 -F ERROR")
        fmt.Println("  ./client2 --show 9501,9503 --max-lines 20 -E 'WARN|ERROR'")
        return
    }

    // Parse client flags and pass the rest to grep on servers
    options, showHosts, maxLines, port := parseArgs(os.Args[1:])

    // List of RPC servers (all 10 VMs)
    baseHosts := []string{
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
    hosts := make([]string, 0, len(baseHosts))
    for _, h := range baseHosts {
        hosts = append(hosts, h+":"+port)
    }

    var wg sync.WaitGroup
    results := make(chan result, len(hosts))

    wg.Add(len(hosts))
    for _, host := range hosts {
        go func(h string) {
            defer wg.Done()
            // Short timeout so a crashed/closed server doesn't block the demo
            conn, err := net.DialTimeout("tcp", h, 2*time.Second)
            if err != nil {
                results <- result{host: h, errStr: "dial: " + err.Error()}
                return
            }
            defer conn.Close()

            client := rpc.NewClient(conn)
            defer client.Close()

            args := &GrepArgs{Options: options}
            var reply GrepReply
            if err := client.Call("GrepService.RunQuery", args, &reply); err != nil {
                results <- result{host: h, errStr: "rpc: " + err.Error()}
                return
            }
            if reply.Err != "" {
                results <- result{host: h, errStr: reply.Err}
                return
            }
            results <- result{host: reply.Host, file: reply.File, lines: reply.Output}
        }(host)
    }

    go func() {
        wg.Wait()
        close(results)
    }()

    // Collect results
    perHostCount := map[string]int{}
    perHostFile := map[string]string{}
    hadErrors := []string{}
    selectedLines := map[string][]string{}

    matchesHost := func(host string) bool {
        if len(showHosts) == 0 {
            return false
        }
        for _, sub := range showHosts {
            if sub == "" {
                continue
            }
            if strings.Contains(host, sub) {
                return true
            }
        }
        return false
    }

    for r := range results {
        if r.errStr != "" {
            hadErrors = append(hadErrors, fmt.Sprintf("%s (%s)", r.host, r.errStr))
            continue
        }
        perHostCount[r.host] = len(r.lines)
        perHostFile[r.host] = r.file
        if matchesHost(r.host) {
            if maxLines >= 0 && len(r.lines) > maxLines {
                selectedLines[r.host] = append([]string{}, r.lines[:maxLines]...)
            } else {
                selectedLines[r.host] = append([]string{}, r.lines...)
            }
        }
    }

    // Output: optional lines for selected hosts
    if len(selectedLines) > 0 {
        fmt.Println("-- Sample Output Lines --")
        keys := make([]string, 0, len(selectedLines))
        for h := range selectedLines {
            keys = append(keys, h)
        }
        sort.Strings(keys)
        for _, h := range keys {
            file := perHostFile[h]
            for _, line := range selectedLines[h] {
                fmt.Printf("%s:%s:%s\n", h, file, line)
            }
        }
        fmt.Println("-- End Samples --")
    }

    // Output: counts per host and total
    fmt.Println("-- Line Counts By Host --")
    keys := make([]string, 0, len(perHostCount))
    for h := range perHostCount {
        keys = append(keys, h)
    }
    sort.Strings(keys)
    total := 0
    for _, h := range keys {
        c := perHostCount[h]
        total += c
        fmt.Printf("%s %s count=%d\n", h, perHostFile[h], c)
    }
    fmt.Printf("TOTAL %d\n", total)

    if len(hadErrors) > 0 {
        fmt.Println("-- Skipped/Errors (excluded from counts) --")
        sort.Strings(hadErrors)
        for _, e := range hadErrors {
            fmt.Println(e)
        }
    }
}

