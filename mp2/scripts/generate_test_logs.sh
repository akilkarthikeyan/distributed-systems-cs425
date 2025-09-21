#!/bin/bash
# Create test logs and output directory under /home/shared/test
# Run this on your local machine (with VPN if required)

HOSTS=(
  yiweiw4@fa25-cs425-6401.cs.illinois.edu
  yiweiw4@fa25-cs425-6402.cs.illinois.edu
  yiweiw4@fa25-cs425-6403.cs.illinois.edu
  yiweiw4@fa25-cs425-6404.cs.illinois.edu
  yiweiw4@fa25-cs425-6405.cs.illinois.edu
  yiweiw4@fa25-cs425-6406.cs.illinois.edu
  yiweiw4@fa25-cs425-6407.cs.illinois.edu
  yiweiw4@fa25-cs425-6408.cs.illinois.edu
  yiweiw4@fa25-cs425-6409.cs.illinois.edu
  yiweiw4@fa25-cs425-6410.cs.illinois.edu
)

LOGDIR="/home/shared/test/logs"
OUTDIR="/home/shared/test/test_outputs"

FREQ="FREQPATTERN"
INFREQ="INFREQPATTERN"
REGEX="REGEXPAT"

for i in "${!HOSTS[@]}"; do
  host=${HOSTS[$i]}
  vmnum=$((i+1))
  echo ">>> Setting up VM $vmnum: $host"

  ssh "$host" bash -s <<EOF
    set -e
    mkdir -p "$LOGDIR"
    mkdir -p "$OUTDIR"

    logfile="$LOGDIR/vm${vmnum}.log"

    # Base content
    echo "random stuff" > "\$logfile"
    echo "another line" >> "\$logfile"

    # Frequent pattern (all VMs)
    echo "$FREQ line on vm${vmnum}" >> "\$logfile"

    # Infrequent pattern (only vm1)
    if [ $vmnum -eq 1 ]; then
      echo "$INFREQ unique" >> "\$logfile"
    fi

    # Regex patterns (only vm1..vm3)
    if [ $vmnum -le 3 ]; then
      echo "${REGEX}${vmnum} match" >> "\$logfile"
    fi

    echo "Created \$logfile with test patterns."
EOF

done

echo ">>> All test logs prepared under $LOGDIR on each VM."
echo ">>> Output directory created at $OUTDIR (empty, ready for client run)."
