package mr

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

var ProcessedTasksNum = 0
var Id = uuid.New()
var TimeStartTask time.Time

const MaxRetryTimes int = 3

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	log.Printf("[Worker] Worker %s is running!\n", Id)

	for {
		task := CallFetchTask()
		TimeStartTask = time.Now()
		switch task.Type {
		case MapTask:
			DoMapTask(task, mapf)
			if !CallSubmitTask(task) {
				log.Println("[Worker] Exiting...")
				return
			}
		case ReduceTask:
			DoReduceTask(task, reducef)
			if !CallSubmitTask(task) {
				log.Println("[Worker] Exiting...")
				return
			}
		case WaitingTask:
			time.Sleep(time.Second)
		case CompleteTask:
			log.Println("[Worker] Exiting...")
			return
		default:
			log.Fatalf("Unexpected task type %v", task.Type)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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

func CallFetchTask() *Task {

	// declare an argument structure.
	args := FetchTaskArgs{
		Msg:    fmt.Sprintf("processed %d tasks", ProcessedTasksNum),
		NodeId: Id,
	}

	// declare a reply structure.
	reply := FetchTaskReply{}

	for i := 0; i < MaxRetryTimes; i++ {
		ok := call("Coordinator.FetchTask", &args, &reply)
		if ok {
			log.Printf("[Worker] Ask Coordinator %s for task. Msg: %s\n", reply.NodeId, reply.Msg)
			return reply.Task
		}
		log.Printf("[Worker] Unable to call Coordinator, tried %d time(s)...\n", i+1)
	}
	log.Printf("[Worker] Failed to call Coordinator, tried %d times. Exiting gracefully.", MaxRetryTimes)
	return nil
}

func CallSubmitTask(task *Task) bool {

	// declare an argument structure.
	args := SubmitTaskArgs{
		Msg:    fmt.Sprintf("took %d millseconds", time.Since(TimeStartTask).Microseconds()),
		NodeId: Id,
		Task:   task,
	}

	// declare a reply structure.
	reply := SubmitTaskReply{}

	for i := 0; i < MaxRetryTimes; i++ {
		ok := call("Coordinator.SubmitTask", &args, &reply)
		if ok {
			log.Printf("[Worker] Submit task to Coordinator %s. Msg: %s\n", reply.NodeId, reply.Msg)
			return true
		}
		log.Printf("[Worker] Unable to call Coordinator, tried %d time(s)...\n", i+1)
	}
	log.Printf("[Worker] Failed to call Coordinator, tried %d times. Exiting gracefully.", MaxRetryTimes)
	return false
}

func DoMapTask(task *Task, mapf func(string, string) []KeyValue) {
	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	filename := task.InputFile[0]
	log.Printf("[Worker] Working on map task. File: %s, TaskId: %d\n", filename, task.ID)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))

	NReduce := task.NReduce
	hash := make(map[int][]KeyValue, NReduce)
	for _, kv := range intermediate {
		index := ihash(kv.Key) % NReduce
		hash[index] = append(hash[index], kv)
	}

	for i := 0; i < NReduce; i++ {
		filename = "mr-" + strconv.Itoa(task.ID) + "-" + strconv.Itoa(i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create %v", filename)
		}
		enc := json.NewEncoder(file)
		for _, kv := range hash[i] {
			err := enc.Encode(kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		file.Close()
	}
}

func DoReduceTask(task *Task, reducef func(string, []string) string) {
	log.Printf("[Worker] Working on reduce task. TaskId: %d\n", task.ID)

	var intermediate []KeyValue

	for _, filename := range task.InputFile {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("[Worker] cannot open file %v\n", filename)
		}
		defer file.Close()
		kv := KeyValue{}
		enc := json.NewDecoder(file)
		for enc.Decode(&kv) == nil {
			intermediate = append(intermediate, kv)
		}
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%d", task.ID)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}
