package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var processId int = os.Getpid()

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	GetTaskFromCoordinator(mapf, reducef)
}

func RunMapTask(fileName string, partitionCount int, mapf func(string, string) []KeyValue) []int {

	fmt.Println("Got Map task with file", fileName)

	var partitionList []int

	file, err := os.Open(fileName)
	if err != nil {
		fmt.Println("Failed to open file", fileName, "for map task. Exiting the map task")
		return partitionList
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Println("Failed to read file", fileName, "for map task. Exiting the map task")
		return partitionList
	}

	file.Close()

	keyValuePairs := mapf(fileName, string(content))

	var partitions map[int][]KeyValue = make(map[int][]KeyValue)

	for _, kvpair := range keyValuePairs {

		key := kvpair.Key
		hashedVal := ihash(key)

		partitions[hashedVal%partitionCount] = append(partitions[hashedVal%partitionCount], kvpair)
	}

	for partition, kvpairlist := range partitions {
		partitionFileName := strconv.Itoa(processId) + "." + strconv.Itoa(partition) + ".mapoutput"
		fileHandler, err := os.OpenFile(partitionFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

		if err != nil {
			fmt.Println("Failed to create partition file", partitionFileName)
			continue
		}

		jsonEncoder := json.NewEncoder(fileHandler)

		for _, kvpair := range kvpairlist {
			err = jsonEncoder.Encode(&kvpair)
		}

		partitionList = append(partitionList, partition)

		fileHandler.Close()
	}

	fmt.Println("Done with map task for file", fileName)

	return partitionList
}

func RunReduceTask(partitionName string, reducef func(string, []string) string) {

	fmt.Println("Got reduce task with partition", partitionName)

	folderHandler, err := os.Open(".")
	if err != nil {
		fmt.Println("Failed to read partition file. Exiting reduce task")
		return
	}

	defer folderHandler.Close()

	fileList, err := folderHandler.Readdirnames(0)
	if err != nil {
		fmt.Println("Failed to read partition file. Exiting reduce task")
		return
	}

	reqFileSubString := "." + partitionName + ".mapoutput"
	var keyValueList []KeyValue

	for _, fileName := range fileList {

		if strings.Contains(fileName, reqFileSubString) {

			//fmt.Println("Processing file", fileName)

			fileHander, err := os.Open(fileName)

			if err != nil {
				fmt.Println("Failed to open partition file", fileName)
				continue
			}

			jsonDecoder := json.NewDecoder(fileHander)

			for {
				var keyValue KeyValue
				if err := jsonDecoder.Decode(&keyValue); err != nil {
					break
				}
				keyValueList = append(keyValueList, keyValue)
			}

			fileHander.Close()
			os.Remove(fileName)
		}
	}

	//sort the keyValue based on keys and pass the keyvalue to reduce for each unique reduce
	sort.Sort(ByKey(keyValueList))
	outputFileName := "mr-out-" + partitionName

	fileHandler, err := os.Create(outputFileName)
	if err != nil {
		fmt.Println("Failed to create reduce output file", outputFileName)
		return
	}

	itr1 := 0

	for itr1 < len(keyValueList) {

		itr2 := itr1 + 1

		for itr2 < len(keyValueList) && keyValueList[itr1].Key == keyValueList[itr2].Key {
			itr2 += 1
		}

		valueList := []string{}

		for valueItr := itr1; valueItr < itr2; valueItr++ {
			valueList = append(valueList, keyValueList[valueItr].Value)
		}

		outputFromReduce := reducef(keyValueList[itr1].Key, valueList)

		fmt.Fprintf(fileHandler, "%v %v\n", keyValueList[itr1].Key, outputFromReduce)

		itr1 = itr2
	}

	fileHandler.Close()

	fmt.Println("Done with reduce task for partition", partitionName)
}

func GetTaskFromCoordinator(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	lastTaskReceivedTime := time.Now()

	timeNow := time.Now()
	elapsed := timeNow.Sub(lastTaskReceivedTime)

	for elapsed.Minutes() < float64(1) {
		taskRequest := GetTaskRequest{Processid: strconv.Itoa(processId)}
		task := Task{}

		ok := call("Coordinator.GetTask", &taskRequest, &task)

		if ok {
			if task.TaskType == NoTask {

				fmt.Println("No task from coordinator, will retry in a second")

				time.Sleep(1 * time.Second)

				timeNow = time.Now()
				elapsed = timeNow.Sub(lastTaskReceivedTime)

			} else if task.TaskType == MapTask {

				partitionList := RunMapTask(task.FileName, task.ReducersCount, mapf)

				timeNow = time.Now()
				lastTaskReceivedTime = time.Now()
				elapsed = timeNow.Sub(lastTaskReceivedTime)

				var response string = ""
				var completedTask Task = Task{FileName: task.FileName, TaskType: MapTask, Partitions: partitionList}
				responseStatus := call("Coordinator.TaskCompleted", &completedTask, &response)
				if responseStatus {
				}

			} else if task.TaskType == ReduceTask {

				RunReduceTask(task.FileName, reducef)

				timeNow = time.Now()
				lastTaskReceivedTime = time.Now()
				elapsed = timeNow.Sub(lastTaskReceivedTime)

				var response string = ""
				var completedTask Task = Task{FileName: task.FileName, TaskType: ReduceTask}
				responseStatus := call("Coordinator.TaskCompleted", &completedTask, &response)
				if responseStatus {
				}

			} else {
				fmt.Println("Exiting worker")
				return
			}

		} else {
			fmt.Println("Failed to get task from coordinator. Exiting the worker.")
			return
		}
	}

	fmt.Println("Exiting worker as there are no more tasks from coordinator even after retrying")
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

	fmt.Println(err)
	return false
}
