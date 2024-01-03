package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

type TaskStatus int

const (
	NotProcessed TaskStatus = iota
	InProcess
	Completed
)

type TaskInfo struct {
	status         TaskStatus
	assignedWorker string
}

type Coordinator struct {
	partitionsCount  int
	mapTaskStatus    map[string]TaskInfo
	reduceTaskStatus map[string]TaskInfo
}

var mapTaskLock sync.Mutex
var reduceTaskLock sync.Mutex

func (c *Coordinator) GetTask(taskRequest *GetTaskRequest, task *Task) error {

	if c.Done() {

		fmt.Println("All tasks are completed sending exit to worker with process id", taskRequest.Processid)
		task.TaskType = ExitTask
		return nil
	}

	mapTaskLock.Lock()
	defer mapTaskLock.Unlock()

	for taskFile, taskInfo := range c.mapTaskStatus {

		if taskInfo.status == NotProcessed {

			task.FileName = taskFile
			task.TaskType = MapTask
			task.ReducersCount = c.partitionsCount

			c.mapTaskStatus[taskFile] = TaskInfo{assignedWorker: taskRequest.Processid, status: InProcess}

			fmt.Println("Assigning map task for file", taskFile, "for worker with process id", taskRequest.Processid)

			return nil
		}
	}

	//Wait for all the map tasks to be completed
	for _, taskInfo := range c.mapTaskStatus {

		if taskInfo.status != Completed {

			task.TaskType = NoTask
			task.FileName = ""

			return nil
		}
	}

	reduceTaskLock.Lock()
	defer reduceTaskLock.Unlock()

	//Need to handle reprocessing of tasks

	for partitionNumber, taskInfo := range c.reduceTaskStatus {

		if taskInfo.status == NotProcessed {

			task.FileName = partitionNumber //reduce will process all the files with this partition number
			task.TaskType = ReduceTask

			c.reduceTaskStatus[partitionNumber] = TaskInfo{assignedWorker: taskRequest.Processid, status: InProcess}

			fmt.Println("Assigning reduce task for partition", partitionNumber, "for worker with process id", taskRequest.Processid)

			return nil
		}
	}

	task.TaskType = NoTask //saying the worker that there are no more tasks as of now. Worker will retry to get work
	task.FileName = ""

	return nil
}

func (c *Coordinator) TaskCompleted(completedTask *Task, response *string) error {

	taskType := completedTask.TaskType

	if taskType == MapTask {

		mapTaskLock.Lock()

		c.mapTaskStatus[completedTask.FileName] = TaskInfo{status: Completed, assignedWorker: ""}

		mapTaskLock.Unlock()

		fmt.Println("Map task completed for file", completedTask.FileName)

		partitionList := completedTask.Partitions

		reduceTaskLock.Lock()

		for _, partition := range partitionList {
			c.reduceTaskStatus[strconv.Itoa(partition)] = TaskInfo{status: NotProcessed, assignedWorker: ""}
		}

		reduceTaskLock.Unlock()

	} else {

		reduceTaskLock.Lock()

		c.reduceTaskStatus[completedTask.FileName] = TaskInfo{status: Completed, assignedWorker: ""}

		reduceTaskLock.Unlock()

		fmt.Println("Reduce task completed for partition", completedTask.FileName)
	}

	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	mapTaskLock.Lock()
	defer mapTaskLock.Unlock()

	for _, taskInfo := range c.mapTaskStatus {
		if taskInfo.status != Completed {
			return false
		}
	}

	reduceTaskLock.Lock()
	defer reduceTaskLock.Unlock()

	for _, taskInfo := range c.reduceTaskStatus {
		if taskInfo.status != Completed {
			return false
		}
	}

	return true
}

// func (c *Coordinator) GetTask(
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

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}

	c.mapTaskStatus = make(map[string]TaskInfo)
	for _, fileName := range files {
		c.mapTaskStatus[fileName] = TaskInfo{status: NotProcessed, assignedWorker: ""}
	}

	c.partitionsCount = nReduce
	c.reduceTaskStatus = make(map[string]TaskInfo) //this will be filled as and when map tasks are completed

	c.server() //calls a goroutine to listen to RPC calls from worker

	return &c
}
