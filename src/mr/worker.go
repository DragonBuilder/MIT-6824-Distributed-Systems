package mr

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
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
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	reply := Reply{}
	for !reply.Quit {
		call("Master.AssignTask", Args{}, &reply)
		// log.Println(reply.Filename)

		if reply.JobType == Map {
			// doMap(reply, mapf)
			kva := mapf(reply.Filename, fileContents(reply.Filename))
			saveIntermediate(splitIntermediateResult(kva, reply.NumReducers))
		}
	}
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

func splitIntermediateResult(kva []KeyValue, numReducers int) map[string][]KeyValue {
	result := make(map[string][]KeyValue)
	for _, kv := range kva {
		//should be atomic?
		// log.Println(reply.NumReducers)
		i := ihash(kv.Key) % numReducers
		filename := fmt.Sprintf("intermediate-%d.json", i)
		result[filename] = append(result[filename], kv)
	}
	return result
}

func saveIntermediate(intermediate map[string][]KeyValue) {
	for filename, kvs := range intermediate {
		var iData []KeyValue
		readIntermediateFile(filename, iData)
		iData = append(iData, kvs...)
		writeIntermediateFile(filename, iData)
	}
}

func readIntermediateFile(filename string, iData []KeyValue) {
	file, err := os.OpenFile(filename, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("error opening %s file : %v", filename, err)
	}
	defer file.Close()

	b, _ := ioutil.ReadAll(file)

	if err := json.Unmarshal(b, &iData); err != nil {
		iData = make([]KeyValue, 0)
		log.Printf("error unmarshalling json : %v\n", err)
	}
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
