package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// for interacting with HyDFS server
func SendPostRequest(endpoint string, data interface{}) (string, error) {
	jsonValue, err := json.Marshal(data)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %w", err)
	}

	resp, err := http.Post(HyDFSServerURL+endpoint, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		return "", fmt.Errorf("failed to connect to server (%s): %w", HyDFSServerURL, err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	body := string(bodyBytes)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("server returned error (Status: %s): %s", resp.Status, body)
	}

	return body, nil
}

// for interacting with HyDFS server
func SendGetRequest(endpoint string) (string, error) {
	resp, err := http.Get(HyDFSServerURL + endpoint)
	if err != nil {
		return "", fmt.Errorf("failed to connect to server (%s): %w", HyDFSServerURL, err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	body := string(bodyBytes)
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("server returned error (Status: %s): %s", resp.Status, body)
	}

	return body, nil
}
