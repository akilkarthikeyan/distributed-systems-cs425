#!/bin/bash
set -e
mkdir -p rainstorm/bin
go build -o rainstorm/bin/identity rainstorm/operators/identity/identity.go
go build -o rainstorm/bin/task rainstorm/task/task.go rainstorm/task/types.go
