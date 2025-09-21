#!/bin/bash

# List of machines (hostnames or IPs)
MACHINES=(
    bofanc2@fa25-cs425-6401.cs.illinois.edu
    bofanc2@fa25-cs425-6402.cs.illinois.edu
    bofanc2@fa25-cs425-6403.cs.illinois.edu
    bofanc2@fa25-cs425-6404.cs.illinois.edu
    bofanc2@fa25-cs425-6405.cs.illinois.edu
    bofanc2@fa25-cs425-6406.cs.illinois.edu
    bofanc2@fa25-cs425-6407.cs.illinois.edu
    bofanc2@fa25-cs425-6408.cs.illinois.edu
    bofanc2@fa25-cs425-6409.cs.illinois.edu
    bofanc2@fa25-cs425-6410.cs.illinois.edu
)

# Path to the executable
EXECUTABLE="/home/shared/bin/server"

# Loop over each machine and start the executable
for HOST in "${MACHINES[@]}"; do
    echo "Connecting to $HOST..."
    ssh "$HOST" "nohup $EXECUTABLE > /tmp/server.log 2>&1 &" && \
    echo "Started server on $HOST" || \
    echo "Failed to start server on $HOST"
done
