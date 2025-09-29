# Distributed Membership Protocol

This project implements a distributed membership service using Go.  
It supports **two protocols** (gossip-based and ping-based) and can operate in **suspect** or **nosuspect** modes.  
Each VM maintains a membership list, detects failures, and communicates with other VMs over UDP.

---

## Features

- **Gossip protocol**: Periodically exchanges membership lists with random peers.
- **Ping protocol**: Periodically pings peers and expects ACKs for failure detection.
- **Suspect / Nosuspect modes**:
  - *suspect*: Marks unresponsive nodes as suspected before declaring them failed.
  - *nosuspect*: Directly marks unresponsive nodes as failed.
- **Dynamic switching**: Protocol and suspect mode can be switched at runtime, and changes are broadcast to all nodes.
- **Failure simulation**: Adjustable message drop probability (`FailureRate`).

---

## Prerequisites

- Go 1.20+ installed on all VMs
- SSH access to all 10 course VMs  
  (`fa25-cs425-9501.cs.illinois.edu … fa25-cs425-9510.cs.illinois.edu`)

---

## Running

Run these steps on each VM (make sure introducer i.e VM1 is run first):

```bash
# 1. Clone this repo
git clone https://gitlab.engr.illinois.edu/akshatg4.git mp2
cd mp2

# 2. Run the code as
go run . {failure_rate} {ping|gossip} {suspect|nosuspect}

# 3. CMD options
list_mem
list_self
switch {ping|gossip} {suspect|nosuspect}
display_protocol
display_suspects
```




