package main

import (
    "log"
    "os/exec"
    "strconv"
)

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

    for i := 1; i <= 10; i++ {
        log.Printf("Running test %d\n", i)

        srcFile := "machine." + strconv.Itoa(i) + ".log"
        destFile := "machine." + strconv.Itoa(i) + ".log"
        host := hosts[i-1]

        cmd := exec.Command("scp", srcFile, host+":~/"+destFile)
        out, err := cmd.CombinedOutput()
        if err != nil {
            log.Fatalf("Failed to copy %s to %s: %v\nOutput: %s", srcFile, host, err, string(out))
        }
    }
}
