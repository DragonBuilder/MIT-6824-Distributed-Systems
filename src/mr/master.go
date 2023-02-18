package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Master struct {
	// Your definitions here.
	Filenames      []string
	NumReducers    int
	processedIndex int
	workers        []worker
	// startReduce    bool
}

type workerState uint

const (
	// idle workerState = "idle"
	idle workerState = iota
	inProgress
	completed
	failed
)

type worker struct {
	state workerState
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AssignTask(args *Args, reply *Reply) error {
	// reply.Y = args.X + 1

	// should be atomic ??

	if m.processedIndex < len(m.Filenames) {
		reply.JobType = Map
		reply.Filename = m.Filenames[m.processedIndex]
		reply.NumReducers = m.NumReducers
		m.processedIndex++
	} else {
		reply.Quit = true
	}

	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{Filenames: files, workers: make([]worker, 0), NumReducers: nReduce}

	// Your code here.

	m.server()
	return &m
}
