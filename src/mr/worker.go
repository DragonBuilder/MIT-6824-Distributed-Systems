package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

//
// Map functions return a slice of KeyValue.
//

const IntermediateResultsFilenameFormat = "mr-0-%d.json"

type KeyValue struct {
	Key   string
	Value string
}

// type intermediate struct {
// 	Data []KeyValue
// }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
// func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

// 	// Your worker implementation here.

// 	reply := Reply{}
// 	for !reply.Quit {
// 		call("Master.AssignTask", BlankArgs{}, &reply)
// 		// log.Println(reply.Filename)

// 		if reply.JobType == Map {
// 			// doMap(reply, mapf)
// 			kva := mapf(reply.Filename, fileContents(reply.Filename))
// 			saveIntermediate(splitIntermediateResult(kva, reply.NumReducers))
// 		} else if reply.JobType == Reduce {
// 			// log.Println("reduce reading intermediate file: ", reply.Filename)
// 			m.Lock()
// 			data := readIntermediateFile(reply.Filename)
// 			m.Unlock()
// 			// log.Fatalln(data)
// 			reduced := doReduce(reducef, data)
// 			writeOutput(reply.OutputFilename, []byte(reduced))
// 		}
// 	}
// 	// uncomment to send the Example RPC to the master.
// 	// CallExample()
// }

type RunningWorker struct {
	name    string
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	done    chan bool
}

func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.

	log.Println("registering worker")

	reply := RegisterWorkerReply{}
	call("Master.RegisterWorker", BlankArgs{}, &reply)

	log.Printf("starting worker with name - %s\n", reply.Name)
	rw := RunWorker(reply.Name, mapf, reducef)

	log.Printf("worker waiting for tasks")
	log.Printf("worker notifying master")

	rw.askForTask()
	<-rw.Done()
}

func RunWorker(name string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) *RunningWorker {
	rw := &RunningWorker{
		name:    name,
		mapf:    mapf,
		reducef: reducef,
		done:    make(chan bool),
	}
	rw.server()
	return rw
}

func (rw *RunningWorker) Done() <-chan bool {
	return rw.done
}

func (rw *RunningWorker) askForTask() {
	reply := WorkerReadyReply{}
	for !reply.Quit {
		call("Master.WorkerReady", WorkerReadyArgs{rw.name}, &reply)
		if reply.JobType == Map {
			// doMap(reply, mapf)
			log.Printf("Map task received on file - %s", reply.Filename)
			kva := rw.mapf(reply.Filename, fileContents(reply.Filename))
			saveIntermediate(splitIntermediateResult(kva, reply.NumReducers))
		} else if reply.JobType == Reduce {
			// log.Println("reduce reading intermediate file: ", reply.Filename)
			// m.Lock()
			log.Printf("Reduce task received on file - %s", reply.Filename)
			data := readIntermediateFile(reply.Filename)
			// m.Unlock()
			// log.Fatalln(data)
			reduced := doReduce(rw.reducef, data)
			writeOutput(reply.OutputFilename, []byte(reduced))
		}
		// call("Master.WorkerReady", WorkerReadyArgs{rw.name}, &reply)
	}
}

func (rw RunningWorker) server() {
	rpc.Register(rw)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := workerSock(rw.name)
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)

}

func doReduce(reducef func(string, []string) string, intermediate []KeyValue) (output string) {
	// reduced := make([]string, 0)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		reduced := reducef(intermediate[i].Key, values)
		output += fmt.Sprintf("%v %v\n", intermediate[i].Key, reduced)

		// this is the correct format for each line of Reduce output.
		// fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
	return
}

func writeOutput(filename string, output []byte) {
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("error opening %s file : %v", filename, err)
	}
	defer file.Close()

	if _, err := file.Write(output); err != nil {
		log.Fatalf("error writing to %s file : %v", filename, err)
	}
}

func splitIntermediateResult(kva []KeyValue, numReducers int) map[string][]KeyValue {
	result := make(map[string][]KeyValue)
	for _, kv := range kva {
		//should be atomic?
		// log.Println(reply.NumReducers)
		i := ihash(kv.Key) % numReducers
		filename := fmt.Sprintf(IntermediateResultsFilenameFormat, i)
		result[filename] = append(result[filename], kv)
	}
	return result
}

var m sync.Mutex

func saveIntermediate(intermediate map[string][]KeyValue) {
	for filename, kvs := range intermediate {
		m.Lock()
		iData := readIntermediateFile(filename)
		iData = append(iData, kvs...)
		writeIntermediateFile(filename, iData)
		m.Unlock()
	}
}

func readIntermediateFile(filename string) []KeyValue {
	var data []KeyValue
	file, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("error opening %s file : %v", filename, err)
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)
	// log.Println(string(b))

	if err := json.Unmarshal(b, &data); err != nil {
		data = make([]KeyValue, 0)
		log.Printf("error unmarshalling json : %v\n", err)
	}
	return data
}

func writeIntermediateFile(filename string, iData []KeyValue) {
	buffer := new(bytes.Buffer)
	encoder := json.NewEncoder(buffer)
	encoder.Encode(iData)

	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("error opening %s file : %v", filename, err)
	}
	defer file.Close()

	if _, err := file.Write(buffer.Bytes()); err != nil {
		log.Fatalf("error writing to %s file : %v", filename, err)
	}
}

func fileContents(name string) string {
	file, err := os.Open(name)
	if err != nil {
		log.Fatalf("cannot open %v", name)
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", name)
	}
	return string(content)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
