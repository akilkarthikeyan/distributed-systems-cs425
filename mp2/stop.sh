#!/bin/bash

# List the VMs you want to stop (comment/uncomment as needed)
vms=(
  fa25-cs425-9501.cs.illinois.edu
  fa25-cs425-9502.cs.illinois.edu
  fa25-cs425-9503.cs.illinois.edu
  fa25-cs425-9504.cs.illinois.edu
  # fa25-cs425-9505.cs.illinois.edu
  # fa25-cs425-9506.cs.illinois.edu
  # fa25-cs425-9507.cs.illinois.edu
  # fa25-cs425-9508.cs.illinois.edu
  # fa25-cs425-9509.cs.illinois.edu
  # fa25-cs425-9510.cs.illinois.edu
)

for vm in "${vms[@]}"; do
  echo ">>> Stopping on $vm"
  ssh "$vm" "
    pkill -f 'go run .'
  "
done
