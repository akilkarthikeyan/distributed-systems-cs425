package main

const HyDFSServerURL = "http://127.0.0.1:8080"

// Types for POST requests
type HyDFSFileRequest struct {
	LocalFilename string `json:"local_filename"`
	HyDFSFilename string `json:"hydfs_filename"`
}

type GetHttpRequest struct {
	HyDFSFilename string `json:"hydfs_filename"`
	LocalFilename string `json:"local_filename"`
}

type MergeHttpRequest struct {
	HyDFSFilename string `json:"hydfs_filename"`
}

type HyDFSMemberInfo struct {
	RingID      uint64 `json:"ring_id"`
	ID          string `json:"id"`
	Status      string `json:"status"`
	Heartbeat   int    `json:"heartbeat"`
	LastUpdated int    `json:"last_updated_tick"`
}

// Type for GET /ls response (simplified)
type LsResponse struct {
	Filename   string `json:"filename"`
	FileRingID int64  `json:"file_ring_id"`
	Replicas   []struct {
		RingID uint64 `json:"ring_id"`
		ID     string `json:"id"`
	} `json:"replicas"`
}
