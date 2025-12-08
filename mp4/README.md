# RainStorm: How to Run

## 1) Bring up your VMs
- Start as many VMs as you want in the cluster.
- Ensure passwordless SSH between them (recommended).

## 2) Start HyDFS
On the HyDFS master:
```bash
cd /path/to/hydfs
go run .
```
(Adjust per your HyDFS setup.)

## 3) Start the RainStorm server on each VM
On every VM:
```bash
cd /home/anandan3/g95/mp4/rainstorm/server
go run .
```
- The leader is hard-coded to `LeaderHost`/`LeaderPort`. Non-leader nodes will auto-join the leader.

## 4) Launch a RainStorm job (on the leader only)
From the leader server’s stdin:
```
rainstorm <Nstages> <NtasksPerStage> \
  <op1_exe> <op1_numargs> <op1_arg1> ... \
  <op2_exe> <op2_numargs> <op2_arg1> ... \
  <hydfs_source_file> <hydfs_dest_file> \
  <exactly_once> <autoscale_enabled> <input_rate> [<lw> <hw>]
```

Example (2 stages, 1 task each, filter -> aggregate):
```bash
rainstorm 2 1 \
  ../bin/filter 2 1 PASS \
  ../bin/aggregate 0 \
  edges.txt result.txt \
  true false 100000
```
- `exactly_once`: true/false
- `autoscale_enabled`: true/false (cannot be true when exactly_once is true)
- `input_rate`: tuples per second (source)
- If `autoscale_enabled=true`, also provide `lw hw` (low/high watermarks).

## 5) Inspect and control
- List tasks (leader stdin):
```bash
list_tasks
```
- Kill a task:
```bash
kill_task <VM:Port> <PID>
```
- Logs:
  - Server: `../logs/server.log`
  - Tasks: `../logs/<runId>_task_<pid>.log`

## 6) Cleanup
- Temp data per task: `/home/anandan3/g95/mp4/rainstorm/temp/<runId>_task_<pid>`
- Logs: `../logs/`
- Use your existing `build.sh` to clean logs/temp if needed.

Notes:
- For autoscaling runs, the autoscale loop starts automatically; for non-autoscale, the tick loop starts automatically.
- Source task uses `input_rate` and streams from the HyDFS source file; sink writes to the HyDFS dest file.