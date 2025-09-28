#!/bin/bash

# List the VMs you want to run on (comment/uncomment as needed)
vms=(
  fa25-cs425-9501.cs.illinois.edu
  # fa25-cs425-9502.cs.illinois.edu
  # fa25-cs425-9503.cs.illinois.edu
  # fa25-cs425-9504.cs.illinois.edu
  # fa25-cs425-9505.cs.illinois.edu
  # fa25-cs425-9506.cs.illinois.edu
  # fa25-cs425-9507.cs.illinois.edu
  # fa25-cs425-9508.cs.illinois.edu
  # fa25-cs425-9509.cs.illinois.edu
  # fa25-cs425-9510.cs.illinois.edu
)

CODE_DIR="$HOME/g95/mp2"

for vm in "${vms[@]}"; do
  echo ">>> Starting on $vm"
  ssh "$vm" "
    cd $CODE_DIR
    nohup go run . >/dev/null 2>&1 &
  "
done
