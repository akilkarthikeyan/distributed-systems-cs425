package main

import (
    "fmt"
    "log"
    "net/rpc"
    "os"
    "sync"
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

func main() {
    if len(os.Args) < 2 {
        fmt.Println("Usage: ./client -e <pattern> [grep options]")
        return
    }

    // The grep options are passed from command line
    options := os.Args[1:]

    // List of RPC servers (all 10 VMs)
    hosts := []string{
        "fa25-cs425-9501.cs.illinois.edu:12345",
        "fa25-cs425-9502.cs.illinois.edu:12345",
        // "fa25-cs425-9503.cs.illinois.edu:12345",
        // "fa25-cs425-9504.cs.illinois.edu:12345",
        // "fa25-cs425-9505.cs.illinois.edu:12345",
        // "fa25-cs425-9506.cs.illinois.edu:12345",
        // "fa25-cs425-9507.cs.illinois.edu:12345",
        // "fa25-cs425-9508.cs.illinois.edu:12345",
        // "fa25-cs425-9509.cs.illinois.edu:12345",
        // "fa25-cs425-9510.cs.illinois.edu:12345",
    }

    var wg sync.WaitGroup
    wg.Add(len(hosts))

    for _, host := range hosts {
        go func(h string) {
            defer wg.Done()
            client, err := rpc.Dial("tcp", h)
            if err != nil {
                log.Printf("Failed to connect to %s: %v\n", h, err)
                return
            }
            defer client.Close()

            args := &GrepArgs{Options: options}
            var reply GrepReply
            err = client.Call("GrepService.RunQuery", args, &reply)
            if err != nil {
                log.Printf("RPC error from %s: %v\n", h, err)
                return
            }

            // Print each matching line prefixed with host and file
            for _, line := range reply.Output {
                fmt.Printf("%s:%s:%s\n", reply.Host, reply.File, line)
            }
        }(host)
    }

    wg.Wait()
}
