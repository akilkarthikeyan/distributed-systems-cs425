package main

import (
    "log"
    "os/exec"
    // "strings"
)

func main() {
	// assumes server is running and client program is available
	// assumes machine.i.log is present for i = 1-10

	// List of RPC servers (all 10 VMs)
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

	for i := 1; i <= 10; i++ {
		log.Printf("Running test %d\n", i)
		cmd := exec.Command("scp", "machine."+string(i)+".log", hosts[i-1]+":~/machine." + string(i)+".log")
		out, err := cmd.CombinedOutput()
		if err != nil {
			log.Fatalf("Failed to copy log file to %s: %v, output: %s\n", hosts[i-1], err, string(out))
		}
	}

}
