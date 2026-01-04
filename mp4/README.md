# RainStorm: How to Run

## 1) Bring up your VMs
- Start as many VMs as you want in the cluster.

## 2) Start HyDFS
Start HyDFS on every macine:
```bash
cd ~/g95/mp3
go run .
```

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
  ../bin/filter 2 D3-1 9 \
  ../bin/aggregate 1 9 \
  edges.txt result.txt \
  true false 100
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
  - Tasks: `../logs/<runId>_task_<taskno>_stage_<stageno>_<processed|acked>.log`

## 6) Cleanup
- Use `build.sh` to clean logs/temp if needed.

# Post-demo Debug

So our main problem during the demo was that rainstorm did not work correctly for consecutive runs but worked for the first run.

We made the following commits after demo to fix the problem:
1. https://github.com/akilkarthikeyan/cs425/commit/5b9f8d5f9ba0053ed9bbf7f5173c73dd0b3a4355 (Fix issue with rainstormRunId parsing)
2. https://github.com/akilkarthikeyan/cs425/commit/6cb49eaf5c4fdf5853ab7a6939f8f1a0afa73767 (Reset and clear global variables so that args are updated for new run)

These were both super small fixes and we are sorry we didn't test RainStorm rigorously and fix this prior to the demo.

Also, these 2 commits for my video demo to go smoothly:
1. https://github.com/akilkarthikeyan/cs425/commit/5500f3b5f63ed08ed7817ecdc8df24eb64b0beda (Remove heartbeat from server log to see other logs clearly)
2. https://github.com/akilkarthikeyan/cs425/commit/b7ccea2cc17e03711df5e6a66979f83756e3f477 (Remove autoscale print from terminal so that I can do list_tasks for autoscale)

Miscellaneous:
1. https://github.com/akilkarthikeyan/cs425/commit/6cd59a7689e2a9ea5176780398583351f42416c5 (README.md)
2. https://github.com/akilkarthikeyan/cs425/commit/8508153f1625049794a45f090ac3f786b040f9fa (README.md)

[This](https://drive.google.com/drive/folders/1IPDaVtdtgiayHgd3YVCTCN2uIoWCukxv?usp=sharing) google drive folder has the video and Test 1 (runId: ktyfp) and Test 2 (runId: axjbx) HyDFS logs and server log (log at leader). HyDFS logs for Test 1 and Test 2 are logs for the filter stage (source stage and aggregate stage don't write logs because we assumed they don't fail).

