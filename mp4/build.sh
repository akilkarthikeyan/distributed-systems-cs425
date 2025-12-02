#!/bin/bash
set -e
go build -o operators/identity/identity operators/identity/identity.go
mv operators/identity/identity rainstorm/task/identity
go build -o rainstorm/task/task rainstorm/task/task.go