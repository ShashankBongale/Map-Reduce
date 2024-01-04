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

// example to show how to declare the arguments
// and reply for an RPC.
type TaskType int

const (
	NoTask TaskType = iota
	MapTask
	ReduceTask
	ExitTask
)

type GetTaskRequest struct {
	Processid string //works as an identifier for worker
}

type Task struct {
	FileName      string
	TaskType      TaskType
	ReducersCount int
	Partitions    []string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
