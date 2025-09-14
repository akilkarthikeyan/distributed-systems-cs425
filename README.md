# Distributed GREP

This project implements a distributed grep system using Go RPC.
Each VM runs a server that listens for queries, and a client can issue search requests across all VMs at once.

### Prerequisites

- Go 1.20+ installed on all VMs
- SSH access to all 10 course VMs

### Setup

Run these steps on each VM (eg. fa25-cs425-9501 … fa25-cs425-9510):

```
# 1. Clone this repo
git https://gitlab.engr.illinois.edu/akshatg4/g95.git g95
cd g95

# 2. Build the server
go build -o server server.go

# 3. Start the server
./server
```

- The server will listen on port 12345. Make sure the port is not blocked.  
- Repeat the above steps for all 10 VMs.
- Place machine.{i}.log file in VM i, inside the g95 folder (or) build the generate_logs.go file (go build -o generate_logs generate_logs.go) and run it (./generate_logs) to generate and place sample logs in every machine.

### Running the client

From any VM, build and run the client:

```
# Build client
go build -o client client.go

# Example queries
./client -E "ERROR"
./client -E "WARN|ERROR"
./client "INFO"
```
The client will connect to all 10 servers (whichever ones are up and running) and print results in the format:

```
<hostname>:<logfile>:<line>
```

### Testing

We provide a generate_logs.go file (to generate logs in each VM i) and a test.go file (to verify whether the distributed grep output is correct).

#### generate_logs.go

- Generates machine.{i}.log and places in VM i.
- Generates ERROR logs with high frequency
- Generates INFO logs with somewhat high frequency
- Generates other type of logs with low frequency
- Generates UNIQUE log in just machine.2.log

#### test.go (assumes all servers are up)

* Performs tests for
    * Frequent (ERROR) pattern
    * Somewhat frequent (INFO) pattern
    * Infrequent (WARN) pattern
    * Unique (UNIQUE) pattern that appears only in one VM
    * Regex (S.*ice) pattern
* For each pattern, it performs distributed grep followed by local grep in each machine. Then, verifies the distributed results against local grep on each machine.

Run as,
```
go run test.go
```