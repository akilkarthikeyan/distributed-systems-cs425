package main

import (
    "flag"
    "fmt"
    "log"
    "math/rand"
    "os"
    "os/exec"
    "time"
)

// canonical logs
var logTemplates = []string{
    "2025-09-10 12:00:00 ERROR Database connection lost",
    "2025-09-10 12:00:00 INFO Service started",
    "2025-09-10 12:00:00 WARN Disk almost full",
    "2025-09-10 12:00:00 DEBUG Cache miss",
    "2025-09-10 12:00:00 INFO User login",
    "2025-09-10 12:00:00 ERROR Timeout while calling API",
    "2025-09-10 12:00:00 INFO File uploaded",
    "2025-09-10 12:00:00 DEBUG Session expired",
    "2025-09-10 12:00:00 WARN High memory usage",
    "2025-09-10 12:00:00 INFO Healthcheck passed",
}

// weights for frequency distribution
var weights = []int{
    5, // frequent
    3, // somewhat frequent
    1, 1, 1, 1, 1, 1, 1, 1, // rare
}

var hosts = []string{
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

// generate one log file with weighted distribution
func generateLogFile(filename string, n int) error {
    f, err := os.Create(filename)
    if err != nil {
        return err
    }
    defer f.Close()

    total := 0
    for _, w := range weights {
        total += w
    }

    for i := 0; i < n; i++ {
        r := rand.Intn(total)
        sum := 0
        var line string
        for j, w := range weights {
            sum += w
            if r < sum {
                line = logTemplates[j]
                break
            }
        }
        fmt.Fprintln(f, line)
    }

    return nil
}

func main() {
    lines := flag.Int("lines", 16, "number of log lines per machine")
    destDir := flag.String("dest", "~/g95", "destination directory on remote machines")
    flag.Parse()

    rand.Seed(time.Now().UnixNano())

    for i, host := range hosts {
        filename := fmt.Sprintf("machine.%d.log", i+1)
        log.Printf("Generating %s with %d lines\n", filename, *lines)

        if err := generateLogFile(filename, *lines); err != nil {
            log.Fatalf("Error generating %s: %v", filename, err)
        }

        // scp the file to remote machine
        dest := fmt.Sprintf("%s:%s/%s", host, *destDir, filename)
        cmd := exec.Command("scp", filename, dest)
        out, err := cmd.CombinedOutput()
        if err != nil {
            log.Fatalf("Failed to scp %s to %s: %v\nOutput: %s",
                filename, host, err, string(out))
        }
        log.Printf("Copied %s to %s", filename, host)
    }

    log.Println("✅ All log files generated and distributed")
}
