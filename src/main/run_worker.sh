#!/bin/bash

export GOPATH=/home/aneesh/playspace/6.824/src

go build -buildmode=plugin ../mrapps/$1.go
go run mrworker.go $1.so