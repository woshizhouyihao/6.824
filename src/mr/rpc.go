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

type Task struct {
	Filename  string
	TaskIndex int
}

type TaskType int

const (
	None TaskType = iota
	Map
	Reduce
	Exit
)

type TaskState int

const (
	Ready TaskState = iota
	InProgress
	Done
)

type MRArgs struct {
	TaskType  TaskType
	TaskIndex int
	Task      []Task

	Pid int
}

type MRReply struct {
	TaskType TaskType
	Task     []Task
	NReduce  int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
