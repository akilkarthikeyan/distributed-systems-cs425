#!/bin/bash
# generate_files.sh
# Creates N random files of a certain size

NUM_FILES=200

mkdir -p preload_files
for i in $(seq -w 1 "$NUM_FILES"); do
  dd if=/dev/urandom of="preload_files/file_${i}.bin" bs=128K count=1 status=none
done

echo "✅ Created $NUM_FILES files in ./preload_files"
