#!/bin/bash
# List of your remote hosts
hosts=(
    "bofanc2@fa25-cs425-6405.cs.illinois.edu"
    "bofanc2@fa25-cs425-6406.cs.illinois.edu"
    "bofanc2@fa25-cs425-6407.cs.illinois.edu"
    "bofanc2@fa25-cs425-6408.cs.illinois.edu"
    "bofanc2@fa25-cs425-6409.cs.illinois.edu"
    "bofanc2@fa25-cs425-6410.cs.illinois.edu"
)

# Directory you want to create
directory="/home/shared/bin"

echo "Creating directory '$directory' and setting permissions on all machines"

for host in "${hosts[@]}"; do
    echo "Processing $host..."
    if ssh -t "$host" "sudo mkdir -p '$directory' && sudo chmod 777 '$directory'"; then
        echo "Success: $host (directory created and permissions set to 777)"
    else
        echo "Failed: $host"
    fi
done

echo "completed"