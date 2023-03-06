package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// type ExampleArgs struct {
// 	X int
// }

// type ExampleReply struct {
// 	Y int
// }

type RegisterWorkerArgs struct {
}

type RegisterWorkerReply struct {
	Name string
}

type WorkerReadyArgs struct {
	WorkerName string
}

type WorkerReadyReply struct {
	JobType JobType
	// ReducerNum  int
	NumReducers    int
	Filename       string
	OutputFilename string
	Quit           bool
}

type BlankArgs struct {
}

type JobType string

const (
	_              = iota
	Map    JobType = "map"
	Reduce JobType = "reduce"
)

type Reply struct {
	JobType JobType
	// ReducerNum  int
	NumReducers    int
	Filename       string
	OutputFilename string
	Quit           bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

func workerSock(name string) string {
	s := "/var/tmp/824-mr-"
	s += name
	return s
}
