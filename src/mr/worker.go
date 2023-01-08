package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	args := MRArgs{}
	for {
		reply := MRReply{}
		ok := RequestTask(&args, &reply)
		if !ok {
			// fmt.Printf("call failed!\n")
			return
		}

		if reply.TaskType == Exit {
			// fmt.Println(os.Getpid(), "worker exit")
			return
		}

		nReduce := reply.NReduce
		task := reply.Task
		taskIndex := task[0].TaskIndex

		switch reply.TaskType {
		case Map:
			// read in file to map
			filename := task[0].Filename
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			// do map
			intermediate := mapf(filename, string(content))

			ofile := []os.File{}
			for i := 0; i < nReduce; i++ {
				// assign results to target files mr-X-Y
				// X is the map task number, Y is the reduce task number
				oname := fmt.Sprintf("mr-%d-%d", taskIndex, i)
				temp, err := ioutil.TempFile(".", oname)
				// temp, err := os.OpenFile(oname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Fatalf("Cannot create file %v", oname)
				}
				ofile = append(ofile, *temp)
			}
			for _, kv := range intermediate {
				index := ihash(kv.Key) % nReduce
				fmt.Fprintf(&ofile[index], "%v %s\n", kv.Key, kv.Value)
			}

			args.TaskType = Map
			args.TaskIndex = taskIndex
			taskArgs := []Task{}
			for key, value := range ofile {
				taskArgs = append(taskArgs, Task{Filename: value.Name(), TaskIndex: key})
				value.Close()
			}
			args.Task = taskArgs

		case Reduce:
			mapKey := map[string][]string{}
			for _, t := range task {
				content, err := ioutil.ReadFile(t.Filename)
				if err != nil {
					log.Fatalf("Cannot read file %v", t.Filename)
				}
				lines := strings.Split(string(content), "\n")
				for _, line := range lines[:len(lines)-1] {
					field := strings.Split(line, " ")
					mapKey[field[0]] = append(mapKey[field[0]], field[1])
				}
			}
			oname := fmt.Sprintf("mr-out-%d", taskIndex)
			ofile, err := ioutil.TempFile(".", oname)
			if err != nil {
				log.Fatalf("Cannot create file %v", ofile)
			}
			defer ofile.Close()
			for k, v := range mapKey {
				fmt.Fprintf(ofile, "%v %s\n", k, reducef(k, v))
			}
			args.TaskType = Reduce
			args.TaskIndex = taskIndex
			args.Task = []Task{}
			args.Task = append(args.Task, Task{Filename: ofile.Name(), TaskIndex: taskIndex})
		}

		args.Pid = os.Getpid()
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func RequestTask(args *MRArgs, reply *MRReply) bool {
	return call("Coordinator.GetTask", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("yz", err)

	return false
}
