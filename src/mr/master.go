package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
)

type Master struct {
	// Your definitions here.
	workers map[string]worker

	Filenames     []string
	NumReducers   int
	mapTaskNum    int
	reduceTaskNum int
	// mapTaskProcessedIndex int
	// workers []worker
	done bool
	// startReduce    bool
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type workerState uint

const (
	booting workerState = iota
	idle
	inProgress
	completed
	failed
)

type worker struct {
	state workerState
	name  string
	sock  string
}

func (m *Master) RegisterWorker(args *BlankArgs, reply *RegisterWorkerReply) error {
	// should be locked to avoid race ??
	log.Println("new worker wants to register")
	totalWorkers := len(m.workers)
	w := worker{state: booting, name: fmt.Sprintf("worker-%d", totalWorkers+1)}
	// m.workers = append(m.workers, w)
	m.workers[w.name] = w
	log.Printf("new worker christened with name - %s\n", w.name)
	reply.Name = w.name
	return nil
}

func (m *Master) WorkerReady(args *WorkerReadyArgs, reply *WorkerReadyReply) error {
	w, ok := m.workers[args.WorkerName]
	if !ok {
		log.Fatalf("no worker with name - [%s] found!", args.WorkerName)
	}
	w.state = inProgress
	m.assignTask(reply)
	m.workers[w.name] = w
	return nil
}

func (m *Master) assignTask(reply *WorkerReadyReply) {
	if m.mapTaskNum < len(m.Filenames) {
		reply.JobType = Map
		reply.Filename = m.Filenames[m.mapTaskNum]
		reply.NumReducers = m.NumReducers
		m.mapTaskNum++
		log.Printf("Map task assigned - %s", reply.Filename)
	} else if m.reduceTaskNum < m.NumReducers {
		reply.JobType = Reduce
		intermediateFilename := fmt.Sprintf(IntermediateResultsFilenameFormat, m.reduceTaskNum)
		// log.Println(intermediateFilename)
		reply.Filename = intermediateFilename
		reply.OutputFilename = fmt.Sprintf("mr-out-%d", m.reduceTaskNum)
		m.reduceTaskNum++
		intrmdtData := readIntermediateFile(intermediateFilename)
		sort.Sort(ByKey(intrmdtData))
		writeIntermediateFile(intermediateFilename, intrmdtData)
		log.Printf("reduce task assigned - %s", reply.Filename)
	} else {
		reply.Quit = true
		m.done = true
	}
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) AssignTask(args *BlankArgs, reply *Reply) error {
	// reply.Y = args.X + 1

	// should be atomic ??

	if m.mapTaskNum < len(m.Filenames) {
		reply.JobType = Map
		reply.Filename = m.Filenames[m.mapTaskNum]
		reply.NumReducers = m.NumReducers
		m.mapTaskNum++
		log.Printf("Map task assigned - %s", reply.Filename)
	} else if m.reduceTaskNum < m.NumReducers {
		reply.JobType = Reduce
		intermediateFilename := fmt.Sprintf(IntermediateResultsFilenameFormat, m.reduceTaskNum)
		// log.Println(intermediateFilename)
		reply.Filename = intermediateFilename
		reply.OutputFilename = fmt.Sprintf("mr-out-%d", m.reduceTaskNum)
		m.reduceTaskNum++
		intrmdtData := readIntermediateFile(intermediateFilename)
		sort.Sort(ByKey(intrmdtData))
		writeIntermediateFile(intermediateFilename, intrmdtData)
		log.Printf("reduce task assigned - %s", reply.Filename)
	} else {
		reply.Quit = true
		m.done = true
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

	// Your code here.

	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{Filenames: files, workers: make(map[string]worker), NumReducers: nReduce}
	// Your code here.

	m.server()
	return &m
}
