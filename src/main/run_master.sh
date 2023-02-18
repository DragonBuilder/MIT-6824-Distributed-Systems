#!/bin/bash

export GOPATH=/home/aneesh/playspace/6.824/src

rm mr-out*
rm mr-0-*

go run mrmaster.go $1