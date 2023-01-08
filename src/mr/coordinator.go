package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Coordinator struct {
	mutexM  sync.Mutex
	mutexR  sync.Mutex
	nReduce int

	numMapTaskDone int
	mapQueue       []Task
	mapTaskState   []TaskState

	numReduceTaskDone int
	reduceQueue       [][]Task
	reduceTaskState   []TaskState
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) GetTask(args *MRArgs, reply *MRReply) error {
	switch args.TaskType {
	case Map: // worker return with map result

		c.mutexM.Lock()
		inputState := c.mapTaskState[args.TaskIndex]
		// job done in 10 seconds
		if inputState == InProgress {

			c.mapTaskState[args.TaskIndex] = Done

			c.mutexM.Unlock()

			c.mutexR.Lock()
			// mapped file name format: mr-X-Y######
			// X is the map task number, Y is the reduce task number
			for _, task := range args.Task {
				path := filepath.Join(filepath.Dir(task.Filename), filepath.Base(task.Filename)[0:6])
				os.Rename(task.Filename, path)
				temp := Task{
					Filename:  path,
					TaskIndex: task.TaskIndex,
				}
				c.reduceQueue[task.TaskIndex] = append(c.reduceQueue[task.TaskIndex], temp)
			}
			c.mutexR.Unlock()

			c.mutexM.Lock()
			c.numMapTaskDone++
			c.mutexM.Unlock()
		} else {

			// timeout; have this worker wait for 2 sec
			c.mutexM.Unlock()
			time.Sleep(2 * time.Second)
		}
	case Reduce: // return with reduce result

		c.mutexR.Lock()
		inputState := c.reduceTaskState[args.TaskIndex]
		// job done in 10 sec
		if inputState == InProgress {

			c.reduceTaskState[args.TaskIndex] = Done
			c.numReduceTaskDone++

			path := filepath.Join(filepath.Dir(args.Task[0].Filename), filepath.Base(args.Task[0].Filename)[0:8])
			os.Rename(args.Task[0].Filename, path)
			c.mutexR.Unlock()
		} else {

			// timeout; have this worker wait for 2 sec
			c.mutexR.Unlock()
			time.Sleep(2 * time.Second)
		}
	default:
		// no return args; do nothing
	}

	// assign task
	reply.NReduce = c.nReduce
	for {
		c.mutexM.Lock()
		if c.numMapTaskDone == len(c.mapTaskState) {
			// all map tasks done
			c.mutexM.Unlock()
			c.mutexR.Lock()
			if c.numReduceTaskDone != c.nReduce {
				if len(c.reduceQueue) == 0 {
					// no reduce task available
					c.mutexR.Unlock()
					time.Sleep(2 * time.Second)
				} else { // reduce task available
					task := c.reduceQueue[0]
					c.reduceQueue = c.reduceQueue[1:]

					reply.TaskType = Reduce
					reply.Task = task

					c.reduceTaskState[task[0].TaskIndex] = InProgress

					defer time.AfterFunc(10*time.Second, func() {
						c.mutexR.Lock()
						defer c.mutexR.Unlock()

						taskState := c.reduceTaskState[task[0].TaskIndex]
						if taskState == InProgress {
							c.reduceQueue = append(c.reduceQueue, task)
							c.reduceTaskState[task[0].TaskIndex] = Ready
						}
					})
					c.mutexR.Unlock()
					return nil
				}
			} else {
				c.mutexR.Unlock()
				reply.TaskType = Exit

				return nil
			}
		} else {
			if len(c.mapQueue) == 0 {
				// no map task available
				c.mutexM.Unlock()

				time.Sleep(2 * time.Second)
			} else {
				// do next available map task
				task := c.mapQueue[0]
				c.mapQueue = c.mapQueue[1:]

				reply.TaskType = Map
				reply.Task = append(reply.Task, task)
				c.mapTaskState[task.TaskIndex] = InProgress

				// get a new thread to count 10s timeout
				defer time.AfterFunc(10*time.Second, func() {
					c.mutexM.Lock()
					defer c.mutexM.Unlock()

					taskState := c.mapTaskState[task.TaskIndex]
					if taskState == InProgress {
						c.mapQueue = append(c.mapQueue, task)
						c.mapTaskState[task.TaskIndex] = Ready
					}
				})

				c.mutexM.Unlock()
				return nil
			}
		}
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutexR.Lock()
	defer c.mutexR.Unlock()
	return c.numReduceTaskDone == c.nReduce
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce

	mapTaskState := []TaskState{}
	mapQueue := []Task{}
	for i, file := range files {
		mapTaskState = append(mapTaskState, Ready)
		mapQueue = append(mapQueue, Task{Filename: file, TaskIndex: i})
	}
	c.mapTaskState = mapTaskState
	c.mapQueue = mapQueue

	for i := 0; i < nReduce; i++ {
		c.reduceTaskState = append(c.reduceTaskState, Ready)
		c.reduceQueue = append(c.reduceQueue, []Task{})
	}

	c.server()
	return &c
}
