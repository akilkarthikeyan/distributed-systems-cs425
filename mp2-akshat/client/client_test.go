package main

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type TestServer struct {
	Data string
}

func (t *TestServer) Grep(req *[]string, reply *GrepResult) error {
	tmpFile := filepath.Join(os.TempDir(),
		"test_server_"+time.Now().Format("150405.000000")+"_"+strconv.Itoa(os.Getpid())+".log")

	if err := os.WriteFile(tmpFile, []byte(t.Data), 0644); err != nil {
		return err
	}
	defer os.Remove(tmpFile)

	args := append(*req, tmpFile)
	cmd := exec.Command("grep", args...)
	out, err := cmd.CombinedOutput()

	if exitErr, ok := err.(*exec.ExitError); ok && exitErr.ExitCode() == 1 {
		out = []byte("")
		err = nil
	}

	hostname, _ := os.Hostname()
	reply.Hostname = hostname
	reply.Output = string(out)
	return err
}

func startTestServer(t *testing.T, addr string, data string) {
	server := rpc.NewServer()
	if err := server.RegisterName("RemoteGrep", &TestServer{Data: data}); err != nil {
		t.Fatalf("failed to register server: %v", err)
	}

	l, err := net.Listen("tcp", addr)
	if err != nil {
		t.Fatalf("failed to listen on %s: %v", addr, err)
	}

	go server.Accept(l)
	time.Sleep(100 * time.Millisecond)
}

// Helpers

func runAndCheck(t *testing.T, addresses []string, args []string, expectedCount int, expectedLines []string) {
	outDir := "./test_outputs"
	os.RemoveAll(outDir)
	if err := os.MkdirAll(outDir, 0755); err != nil {
		t.Fatalf("failed to create outDir: %v", err)
	}

	getLogAll(addresses, args, outDir)

	files, _ := os.ReadDir(outDir)
	total := 0
	var allLines []string

	for _, f := range files {
		content, err := os.ReadFile(filepath.Join(outDir, f.Name()))
		if err != nil {
			t.Errorf("failed to read %s: %v", f.Name(), err)
			continue
		}
		txt := strings.TrimSpace(string(content))
		if txt == "" || strings.HasPrefix(txt, "ERROR:") || strings.HasPrefix(txt, "No matches") {
			continue
		}
		lines := strings.Split(txt, "\n")
		allLines = append(allLines, lines...)
		total += len(lines)
	}

	if total != expectedCount {
		t.Errorf("pattern=%v expected %d matches, got %d", args, expectedCount, total)
	}

	if len(expectedLines) > 0 {
		gotSet := make(map[string]bool)
		for _, l := range allLines {
			gotSet[l] = true
		}
		for _, exp := range expectedLines {
			if !gotSet[exp] {
				t.Errorf("expected line %q not found, got=%v", exp, allLines)
			}
		}
	}
}

// random noise

func makeRandomLines(n int) []string {
	words := []string{"alpha", "bravo", "charlie", "delta", "echo"}
	var lines []string
	for i := 0; i < n; i++ {
		lines = append(lines,
			fmt.Sprintf("%s %s", words[rand.Intn(len(words))], words[rand.Intn(len(words))]))
	}
	return lines
}

// Tests

func TestDistributedGrep_AllCases(t *testing.T) {
	rand.Seed(time.Now().UnixNano())

	// Server1 data: known + random noise
	s1 := strings.Join(append(makeRandomLines(5),
		"apple banana",
		"cat dog",
		"apple tree"), "\n")

	// Server2 data: known + random noise
	s2 := strings.Join(append(makeRandomLines(5),
		"banana",
		"apple pie",
		"cat apple"), "\n")

	startTestServer(t, ":9101", s1)
	startTestServer(t, ":9102", s2)

	addresses := []string{"localhost:9101", "localhost:9102"}

	t.Run("FrequentPattern", func(t *testing.T) {
		runAndCheck(t,
			addresses,
			[]string{"apple"},
			4,
			[]string{"apple banana", "apple tree", "apple pie", "cat apple"},
		)
	})

	t.Run("InfrequentPattern", func(t *testing.T) {
		runAndCheck(t,
			addresses,
			[]string{"banana"},
			2,
			[]string{"apple banana", "banana"},
		)
	})

	t.Run("RegexPattern", func(t *testing.T) {
		runAndCheck(t,
			addresses,
			[]string{"-e", "^cat"},
			2,
			[]string{"cat dog", "cat apple"},
		)
	})

	t.Run("NoMatch", func(t *testing.T) {
		runAndCheck(t,
			addresses,
			[]string{"chess"},
			0,
			nil,
		)
	})

	t.Run("CrashServer", func(t *testing.T) {
		runAndCheck(t,
			[]string{"localhost:9101", "localhost:9199"},
			[]string{"apple"},
			2,
			[]string{"apple banana", "apple tree"},
		)
	})

	t.Run("OneServerOnly", func(t *testing.T) {
		runAndCheck(t,
			addresses,
			[]string{"pie"},
			1,
			[]string{"apple pie"},
		)
	})

	t.Run("TimeoutServer", func(t *testing.T) {
		runAndCheck(t,
			[]string{"localhost:9101", "localhost:9999"},
			[]string{"apple"},
			2,
			[]string{"apple banana", "apple tree"},
		)
	})
}
