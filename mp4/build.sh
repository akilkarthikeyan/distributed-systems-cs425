#!/bin/bash
set -e
mkdir -p rainstorm/bin
go build -o rainstorm/bin/identity rainstorm/operators/identity/identity.go
go build -o rainstorm/bin/task rainstorm/task/task.go rainstorm/task/types.go rainstorm/task/utils.go rainstorm/task/HyDFSTypes.go rainstorm/task/HyDFSUtils.go
rm -f rainstorm/logs/*.log
