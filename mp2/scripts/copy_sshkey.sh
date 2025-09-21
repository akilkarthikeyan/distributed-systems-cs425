#!/bin/bash
hosts=(
    "fa25-cs425-6402.cs.illinois.edu"
    "fa25-cs425-6403.cs.illinois.edu"
    "fa25-cs425-6404.cs.illinois.edu"
)
username="bofanc2"

echo "This script will copy your SSH key to all remote hosts."
read -p "Press Enter to continue"

for host in "${hosts[@]}"; do
    echo "Copying SSH key to $username@$host..."
    ssh-copy-id "$username@$host"
    echo "---"
done

echo "completed"