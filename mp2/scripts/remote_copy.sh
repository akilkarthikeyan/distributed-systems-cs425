#!/bin/bash

file="/home/shared/bin/server"
remote_path="/home/shared/bin"
servers=(
    "bofanc2@fa25-cs425-6401.cs.illinois.edu"
    "bofanc2@fa25-cs425-6403.cs.illinois.edu"
    "bofanc2@fa25-cs425-6404.cs.illinois.edu"
    "bofanc2@fa25-cs425-6405.cs.illinois.edu"
    "bofanc2@fa25-cs425-6406.cs.illinois.edu"
    "bofanc2@fa25-cs425-6407.cs.illinois.edu"
    "bofanc2@fa25-cs425-6408.cs.illinois.edu"
    "bofanc2@fa25-cs425-6409.cs.illinois.edu"
    "bofanc2@fa25-cs425-6410.cs.illinois.edu"
)

for srv in "${servers[@]}"; do
  scp "$file" "$srv:$remote_path"
done
