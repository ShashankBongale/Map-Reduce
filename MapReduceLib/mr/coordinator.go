package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"sync"
	"time"
)

type TaskStatus int

const (
	NotProcessed TaskStatus = iota
	InProcess
	Completed
)

type MapTaskInfo struct {
	status           TaskStatus
	assignedWorker   string
	taskAssignedTime time.Time
}

type ReduceTaskInfo struct {
	status            TaskStatus
	assignedWorker    string
	partitionFileList []string
	taskAssignedTime  time.Time
}

type Coordinator struct {
	partitionsCount  int
	mapTaskStatus    map[string]MapTaskInfo
	reduceTaskStatus map[string]ReduceTaskInfo
}

var mapTaskLock sync.Mutex
var reduceTaskLock sync.Mutex

func (c *Coordinator) GetTask(taskRequest *GetTaskRequest, task *Task) error {

	//time.Sleep(10 * time.Second)

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

			c.mapTaskStatus[taskFile] = MapTaskInfo{assignedWorker: taskRequest.Processid, status: InProcess, taskAssignedTime: time.Now()}

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

			task.Partitions = taskInfo.partitionFileList

			c.reduceTaskStatus[partitionNumber] = ReduceTaskInfo{assignedWorker: taskRequest.Processid, status: InProcess, partitionFileList: taskInfo.partitionFileList, taskAssignedTime: time.Now()}

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

		if c.mapTaskStatus[completedTask.FileName].status == Completed {

			fmt.Println("Received complete message for already completed map task for file", completedTask.FileName, ".Ignoring the message")

			mapTaskLock.Unlock()

			return nil
		}

		c.mapTaskStatus[completedTask.FileName] = MapTaskInfo{status: Completed, assignedWorker: ""}

		mapTaskLock.Unlock()

		fmt.Println("Map task completed for file", completedTask.FileName)

		partitionList := completedTask.Partitions

		for _, partitionFileName := range partitionList {

			partitionStartIndex := strings.Index(partitionFileName, ".")
			partitionEndIndex := strings.Index(partitionFileName, ".mapoutput")

			partitionNumber := partitionFileName[partitionStartIndex+1 : partitionEndIndex]

			reduceTaskLock.Lock()

			if reduceTaskInfo, exists := c.reduceTaskStatus[partitionNumber]; exists == true {

				exists := false
				for _, currPartitionFile := range reduceTaskInfo.partitionFileList {

					if currPartitionFile == partitionFileName {
						exists = true
						break
					}
				}

				if exists == false {
					updatedPartitons := append(reduceTaskInfo.partitionFileList, partitionFileName)
					c.reduceTaskStatus[partitionNumber] = ReduceTaskInfo{status: NotProcessed, assignedWorker: "", partitionFileList: updatedPartitons}
				}
			} else {

				partitionList := []string{partitionFileName}
				c.reduceTaskStatus[partitionNumber] = ReduceTaskInfo{status: NotProcessed, assignedWorker: "", partitionFileList: partitionList}

			}

			reduceTaskLock.Unlock()
		}

	} else {

		reduceTaskLock.Lock()

		c.reduceTaskStatus[completedTask.FileName] = ReduceTaskInfo{status: Completed, assignedWorker: "", partitionFileList: []string{}}

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

func (c *Coordinator) TaskCompletionChecker() {

	pendingTaskExist := true

	for pendingTaskExist == true {

		pendingTaskExist = false

		mapTaskLock.Lock()

		for taskFileName, mapTaskInfo := range c.mapTaskStatus {

			if mapTaskInfo.status == InProcess {

				pendingTaskExist = true
				timeNow := time.Now()
				timeElapsedInSecs := timeNow.Sub(mapTaskInfo.taskAssignedTime)

				if timeElapsedInSecs.Seconds() > float64(10) {

					fmt.Println("Map task for file", taskFileName, "is not completed in threshold time, reassigning it")
					c.mapTaskStatus[taskFileName] = MapTaskInfo{status: NotProcessed, assignedWorker: "", taskAssignedTime: time.Time{}}

				}
			} else if mapTaskInfo.status == NotProcessed {
				pendingTaskExist = true
			}
		}

		mapTaskLock.Unlock()

		reduceTaskLock.Lock()

		for taskFileName, reduceTaskInfo := range c.reduceTaskStatus {

			if reduceTaskInfo.status == InProcess {

				pendingTaskExist = true
				timeNow := time.Now()
				timeElapsedInSecs := timeNow.Sub(reduceTaskInfo.taskAssignedTime)

				if timeElapsedInSecs.Seconds() > float64(10) {

					fmt.Println("Reduce task for file", taskFileName, "is not completed in threshold time, reassigning it")
					c.reduceTaskStatus[taskFileName] = ReduceTaskInfo{status: NotProcessed, assignedWorker: "", taskAssignedTime: time.Time{}, partitionFileList: reduceTaskInfo.partitionFileList}

				}
			} else if reduceTaskInfo.status == NotProcessed {
				pendingTaskExist = true
			}
		}

		reduceTaskLock.Unlock()

		if pendingTaskExist == true {
			fmt.Println("Pending task exists. Will sleep for 3 secs")
			time.Sleep(3 * time.Second)
		}

	}

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

	go c.TaskCompletionChecker()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	c := Coordinator{}

	c.mapTaskStatus = make(map[string]MapTaskInfo)
	for _, fileName := range files {
		c.mapTaskStatus[fileName] = MapTaskInfo{status: NotProcessed, assignedWorker: ""}
	}

	c.partitionsCount = nReduce
	c.reduceTaskStatus = make(map[string]ReduceTaskInfo) //this will be filled as and when map tasks are completed

	c.server() //calls a goroutine to listen to RPC calls from worker

	return &c
}
