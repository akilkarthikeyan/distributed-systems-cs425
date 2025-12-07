#!/bin/bash
set -e
mkdir -p /home/anandan3/g95/mp4/rainstorm/bin
go build -o /home/anandan3/g95/mp4/rainstorm/bin/identity /home/anandan3/g95/mp4/rainstorm/operators/identity/identity.go
go build -o /home/anandan3/g95/mp4/rainstorm/bin/transform /home/anandan3/g95/mp4/rainstorm/operators/transform/transform.go
go build -o /home/anandan3/g95/mp4/rainstorm/bin/filter /home/anandan3/g95/mp4/rainstorm/operators/filter/filter.go
go build -o /home/anandan3/g95/mp4/rainstorm/bin/aggregate /home/anandan3/g95/mp4/rainstorm/operators/aggregate/aggregate.go
go build -o /home/anandan3/g95/mp4/rainstorm/bin/task /home/anandan3/g95/mp4/rainstorm/task/task.go /home/anandan3/g95/mp4/rainstorm/task/types.go /home/anandan3/g95/mp4/rainstorm/task/utils.go /home/anandan3/g95/mp4/rainstorm/task/HyDFSTypes.go /home/anandan3/g95/mp4/rainstorm/task/HyDFSUtils.go
rm -f /home/anandan3/g95/mp4/rainstorm/logs/*.log
rm -rf /home/anandan3/g95/mp4/rainstorm/temp/*
