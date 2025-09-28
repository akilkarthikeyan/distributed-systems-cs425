package main

import (
	"errors"
	"net"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Test the basic Grep functionality
func TestRemoteGrep_Grep(t *testing.T) {
	tests := []struct {
		name        string
		args        []string
		setupFile   bool
		fileContent string
		wantOutput  string
		wantErr     bool
	}{
		{
			name:        "successful grep with match",
			args:        []string{"test", "testdata/test.txt"},
			setupFile:   true,
			fileContent: "this is a test line\nanother line\ntest again",
			wantOutput:  "this is a test line\ntest again",
			wantErr:     false,
		},
		{
			name:        "grep with no match",
			args:        []string{"nomatch", "testdata/test.txt"},
			setupFile:   true,
			fileContent: "this is a test line\nanother line",
			wantOutput:  "",
			wantErr:     false, // exit code 1 is not an error
		},
		{
			name:       "grep with invalid file",
			args:       []string{"test", "nonexistent.txt"},
			setupFile:  false,
			wantOutput: "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup test file if needed
			if tt.setupFile {
				setupTestFile(t, "testdata/test.txt", tt.fileContent)
				defer os.RemoveAll("testdata")
			}

			// Create RemoteGrep instance and call Grep
			var rg RemoteGrep
			var result GrepResult
			err := rg.Grep(&tt.args, &result)

			// Check error
			if (err != nil) != tt.wantErr {
				t.Errorf("Grep() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Check hostname is set
			if result.Hostname == "" {
				t.Error("Hostname should not be empty")
			}

			// Check output (if no error expected)
			if !tt.wantErr && tt.wantOutput != "" {
				// Normalize line endings for comparison
				gotOutput := strings.TrimSpace(result.Output)
				wantOutput := strings.TrimSpace(tt.wantOutput)
				if !strings.Contains(gotOutput, wantOutput) {
					t.Errorf("Grep() output = %v, want %v", gotOutput, wantOutput)
				}
			}
		})
	}
}

// Test the RPC server integration
func TestRPCServer(t *testing.T) {
	// Start server on a test port
	port := "9999"
	done := make(chan bool)
	go func() {
		startTestServer(t, port)
		done <- true
	}()

	// Give server time to start
	time.Sleep(100 * time.Millisecond)

	// Connect to the server
	client, err := rpc.Dial("tcp", "localhost:"+port)
	if err != nil {
		t.Fatalf("Failed to connect to RPC server: %v", err)
	}
	defer client.Close()

	// Setup test file
	setupTestFile(t, "testdata/rpc_test.txt", "hello world\nfoo bar\nhello again")
	defer os.RemoveAll("testdata")

	// Test cases
	tests := []struct {
		name         string
		args         []string
		wantInOutput string
		wantErr      bool
	}{
		{
			name:         "RPC call with match",
			args:         []string{"hello", "testdata/rpc_test.txt"},
			wantInOutput: "hello world",
			wantErr:      false,
		},
		{
			name:         "RPC call with no match",
			args:         []string{"nomatch", "testdata/rpc_test.txt"},
			wantInOutput: "",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var result GrepResult
			err := client.Call("RemoteGrep.Grep", &tt.args, &result)

			if (err != nil) != tt.wantErr {
				t.Errorf("RPC call error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantInOutput != "" && !strings.Contains(result.Output, tt.wantInOutput) {
				t.Errorf("RPC call output = %v, want to contain %v", result.Output, tt.wantInOutput)
			}

			if result.Hostname == "" {
				t.Error("Hostname should not be empty in RPC response")
			}
		})
	}
}

// Test edge cases and error handling
func TestRemoteGrep_ErrorHandling(t *testing.T) {
	var rg RemoteGrep

	t.Run("invalid grep arguments", func(t *testing.T) {
		args := []string{"--invalid-option"}
		var result GrepResult
		err := rg.Grep(&args, &result)

		if err == nil {
			t.Error("Expected error for invalid grep arguments")
		}
	})

	t.Run("empty arguments", func(t *testing.T) {
		args := []string{}
		var result GrepResult
		err := rg.Grep(&args, &result)

		// grep with no arguments should error
		if err == nil {
			t.Error("Expected error for empty arguments")
		}
	})
}

// Test concurrent RPC calls
func TestConcurrentRPCCalls(t *testing.T) {
	// Start server
	port := "9998"
	done := make(chan bool)
	go func() {
		startTestServer(t, port)
		done <- true
	}()
	time.Sleep(100 * time.Millisecond)

	// Setup test file
	setupTestFile(t, "testdata/concurrent_test.txt", "line1\nline2\nline3")
	defer os.RemoveAll("testdata")

	// Number of concurrent clients
	numClients := 10
	clientDone := make(chan bool, numClients)

	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			client, err := rpc.Dial("tcp", "localhost:"+port)
			if err != nil {
				t.Errorf("Client %d: Failed to connect: %v", clientID, err)
				clientDone <- false
				return
			}
			defer client.Close()

			args := []string{"line", "testdata/concurrent_test.txt"}
			var result GrepResult
			err = client.Call("RemoteGrep.Grep", &args, &result)
			if err != nil {
				t.Errorf("Client %d: RPC call failed: %v", clientID, err)
				clientDone <- false
				return
			}

			if !strings.Contains(result.Output, "line") {
				t.Errorf("Client %d: Unexpected output: %v", clientID, result.Output)
				clientDone <- false
				return
			}

			clientDone <- true
		}(i)
	}

	// Wait for all clients to complete
	successCount := 0
	for i := 0; i < numClients; i++ {
		if <-clientDone {
			successCount++
		}
	}

	if successCount != numClients {
		t.Errorf("Not all concurrent calls succeeded: %d/%d", successCount, numClients)
	}
}

// Benchmark the Grep function
func BenchmarkRemoteGrep_Grep(b *testing.B) {
	// Setup test file
	setupTestFile(b, "testdata/bench_test.txt", strings.Repeat("test line\n", 1000))
	defer os.RemoveAll("testdata")

	var rg RemoteGrep
	args := []string{"test", "testdata/bench_test.txt"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result GrepResult
		_ = rg.Grep(&args, &result)
	}
}

// Test with specific exit codes
func TestRemoteGrep_ExitCodes(t *testing.T) {
	var rg RemoteGrep

	// Test exit code 1 (no match found - should not return error)
	t.Run("exit code 1 - no match", func(t *testing.T) {
		setupTestFile(t, "testdata/nomatch.txt", "foo bar baz")
		defer os.RemoveAll("testdata")

		args := []string{"xyz", "testdata/nomatch.txt"}
		var result GrepResult
		err := rg.Grep(&args, &result)

		if err != nil {
			t.Errorf("Expected no error for exit code 1, got: %v", err)
		}

		if result.Output != "" {
			t.Errorf("Expected empty output for no match, got: %v", result.Output)
		}
	})

	// Test exit code 2 (error - should return error)
	t.Run("exit code 2 - file not found", func(t *testing.T) {
		args := []string{"test", "nonexistent.txt"}
		var result GrepResult
		err := rg.Grep(&args, &result)

		if err == nil {
			t.Error("Expected error for file not found")
		}

		if !strings.Contains(err.Error(), "command execution failed") {
			t.Errorf("Expected 'command execution failed' error, got: %v", err)
		}
	})
}

// Test hostname retrieval
func TestRemoteGrep_Hostname(t *testing.T) {
	var rg RemoteGrep

	// Get expected hostname
	expectedHostname, _ := os.Hostname()
	if expectedHostname == "" {
		expectedHostname = "unknown-host"
	}

	setupTestFile(t, "testdata/hostname_test.txt", "test")
	defer os.RemoveAll("testdata")

	args := []string{"test", "testdata/hostname_test.txt"}
	var result GrepResult
	err := rg.Grep(&args, &result)

	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if result.Hostname != expectedHostname {
		t.Errorf("Expected hostname %s, got %s", expectedHostname, result.Hostname)
	}
}

// Helper functions

func setupTestFile(t testing.TB, filename, content string) {
	t.Helper()

	// Create testdata directory if it doesn't exist
	if err := os.MkdirAll("testdata", 0755); err != nil {
		t.Fatalf("Failed to create testdata directory: %v", err)
	}

	// Write test file
	if err := os.WriteFile(filename, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
}

func startTestServer(t *testing.T, port string) {
	t.Helper()

	// Register the RPC service
	grepServer := new(RemoteGrep)
	if err := rpc.Register(grepServer); err != nil {
		// Already registered is OK for multiple test runs
		if !strings.Contains(err.Error(), "already registered") {
			t.Fatalf("Failed to register RPC service: %v", err)
		}
	}

	// Listen for incoming RPC connections
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		t.Fatalf("Failed to listen on port %s: %v", port, err)
	}

	// Accept connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}
}

// Mock-based testing approach (Alternative testing strategy)
// This requires refactoring the original code to support dependency injection

// CommandExecutor interface for mocking exec.Command
type CommandExecutor interface {
	Execute(name string, args ...string) ([]byte, error)
}

// RealCommandExecutor uses actual exec.Command
type RealCommandExecutor struct{}

func (r *RealCommandExecutor) Execute(name string, args ...string) ([]byte, error) {
	cmd := exec.Command(name, args...)
	return cmd.CombinedOutput()
}

// MockCommandExecutor for testing without actual grep
type MockCommandExecutor struct {
	Output []byte
	Err    error
}

func (m *MockCommandExecutor) Execute(name string, args ...string) ([]byte, error) {
	return m.Output, m.Err
}

// MockExitError for simulating exit codes
type MockExitError struct {
	exitCode int
}

func (m *MockExitError) Error() string {
	return "mock exit error"
}

func (m *MockExitError) ExitCode() int {
	return m.exitCode
}

// Enhanced RemoteGrep with dependency injection (for demonstration)
// This shows how you could refactor your code for better testability
type RemoteGrepWithDI struct {
	executor CommandExecutor
}

func NewRemoteGrepWithDI(executor CommandExecutor) *RemoteGrepWithDI {
	if executor == nil {
		executor = &RealCommandExecutor{}
	}
	return &RemoteGrepWithDI{executor: executor}
}

func (t *RemoteGrepWithDI) Grep(req *[]string, reply *GrepResult) error {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown-host"
	}
	reply.Hostname = hostname

	output, err := t.executor.Execute("grep", (*req)...)
	reply.Output = string(output)

	if err != nil {
		// Check for exit code 1 (no match)
		if exitError, ok := err.(*exec.ExitError); ok {
			if exitError.ExitCode() == 1 {
				return nil
			}
		}
		// Also check for mock exit error
		if mockError, ok := err.(*MockExitError); ok {
			if mockError.ExitCode() == 1 {
				return nil
			}
		}
		return errors.New("command execution failed")
	}

	return nil
}

// Test with mock executor (demonstrates isolated unit testing)
func TestRemoteGrepWithDI_MockedExecution(t *testing.T) {
	tests := []struct {
		name       string
		mockOutput []byte
		mockErr    error
		wantOutput string
		wantErr    bool
	}{
		{
			name:       "successful mock grep",
			mockOutput: []byte("mocked grep output"),
			mockErr:    nil,
			wantOutput: "mocked grep output",
			wantErr:    false,
		},
		{
			name:       "mock grep with no match (exit code 1)",
			mockOutput: []byte(""),
			mockErr:    &MockExitError{exitCode: 1},
			wantOutput: "",
			wantErr:    false,
		},
		{
			name:       "mock grep with error (exit code 2)",
			mockOutput: []byte(""),
			mockErr:    &MockExitError{exitCode: 2},
			wantOutput: "",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mock := &MockCommandExecutor{
				Output: tt.mockOutput,
				Err:    tt.mockErr,
			}

			rg := NewRemoteGrepWithDI(mock)
			args := []string{"test", "file.txt"}
			var result GrepResult

			err := rg.Grep(&args, &result)

			if (err != nil) != tt.wantErr {
				t.Errorf("Grep() error = %v, wantErr %v", err, tt.wantErr)
			}

			if result.Output != tt.wantOutput {
				t.Errorf("Grep() output = %v, want %v", result.Output, tt.wantOutput)
			}

			if result.Hostname == "" {
				t.Error("Hostname should not be empty")
			}
		})
	}
}
