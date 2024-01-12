package mr

import (
	"fmt"
	"github.com/google/uuid"
	"log"
	"sync"
	"time"
)

import "net"
import "os"
import "net/rpc"
import "net/http"

type CoordinatorStatus int

const (
	MapStage CoordinatorStatus = iota
	ReduceStage
	ExitStage
)

type Coordinator struct {
	Id       uuid.UUID
	Mutex    sync.Mutex
	Status   CoordinatorStatus
	TaskSet  *TaskSet
	TaskChan chan *Task
	NMap     int
	NReduce  int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
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
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.Status != ExitStage {
		return false
	}
	log.Printf("[Coordinator] All task done. Exit gracefully.\n")
	time.Sleep(2 * 2 * time.Second)
	log.Printf("[Coordinator] Exiting...\n")
	return true
}

func (c *Coordinator) FetchTask(args *FetchTaskArgs, reply *FetchTaskReply) error {
	log.Printf("[Coordinator] Worker node %s asks for a task. Msg: %s\n", args.NodeId, args.Msg)
	reply.NodeId = c.Id
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	select {
	case task := <-c.TaskChan:
		c.TaskSet.StartTask(task)
		reply.Task = task
		reply.Msg = fmt.Sprintf("Fetched Task Type: %s, ID: %d", task.Type, task.ID)
	default:
		log.Println("[Coordinator] No task is available right now.")
		c.updateStatus()
		if c.Status == ExitStage {
			reply.Task = &Task{Type: CompleteTask}
			reply.Msg = "All tasks done. Exit gracefully."
		} else {
			reply.Task = &Task{Type: WaitingTask}
			reply.Msg = "No available task to distribute. Wait for a while and retry."
		}
	}
	return nil
}

func (c *Coordinator) updateStatus() {
	switch c.Status {
	case MapStage:
		if c.TaskSet.IfAllTaskDone(MapTask) {
			log.Printf("[Coordinator] All map tasks finished.\n")
			c.Status = ReduceStage
			c.registerReduceTasks()
		}
	case ReduceStage:
		if c.TaskSet.IfAllTaskDone(ReduceTask) {
			log.Printf("[Coordinator] All reduce tasks finished.\n")
			c.Status = ExitStage
		}
	case ExitStage:
		log.Printf("[Coordinator] All tasks finished.\n")
		// do nothing
	default:
		log.Fatalf("Cannot check unsupported coordinator stage type.")
	}
}

func (c *Coordinator) registerReduceTasks() {
	for i := 0; i < c.NReduce; i++ {
		var inputFile []string
		for j := 0; j < c.NMap; j++ {
			filename := fmt.Sprintf("mr-%d-%d", j, i)
			inputFile = append(inputFile, filename)
		}
		task := NewReduceeTask(i, inputFile)
		c.TaskSet.RegisterTask(task)
		go func() { c.TaskChan <- task }()
	}
	log.Println("[Coordinator] Successfully registered reduce tasks.")
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	log.Printf(
		"[Coordinator] Worker node %s submits finished task. TaskType: %d, TaskId: %d, Msg: %s\n",
		args.NodeId, args.Task.Type, args.Task.ID, args.Msg,
	)
	reply.NodeId = c.Id
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if c.TaskSet.DoneTask(args.Task) {
		reply.Msg = fmt.Sprintf("Submitted Task Type: %s, ID: %d", args.Task.Type, args.Task.ID)
	} else {
		reply.Msg = fmt.Sprintf("Task Type: %s, ID: %d already completed by another worker, no worry.", args.Task.Type, args.Task.ID)
	}
	return nil
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Id:       uuid.New(),
		Status:   MapStage,
		TaskSet:  NewTaskSet(),
		TaskChan: make(chan *Task, 10),
		NMap:     len(files),
		NReduce:  nReduce,
	}

	// 初始化 Map 任务
	for i, file := range files {
		mapTask := NewMapTask(i, []string{file}, nReduce)
		c.TaskSet.RegisterTask(mapTask)
		go func() { c.TaskChan <- mapTask }()
	}

	go c.timeoutChecker()

	c.server()

	log.Printf("[Coordinator] Coordinator %s is running!\n", c.Id)
	return &c
}

func (c *Coordinator) timeoutChecker() {
	for {
		c.Mutex.Lock()
		log.Printf("%d", len(c.TaskChan))
		var tasks []*Task
		switch c.Status {
		case MapStage:
			tasks = c.TaskSet.IfTimeOut(MapTask)
		case ReduceStage:
			tasks = c.TaskSet.IfTimeOut(ReduceTask)
		case ExitStage:
			c.Mutex.Unlock()
			return
		default:
			log.Fatalf("Unexpected Coordinator status %v", c.Status)
		}
		for _, task := range tasks {
			log.Printf("[Coordinator] Task timed out, register again. TaskType: %d, TaskId: %d\n",
				task.Type, task.ID)
			c.TaskSet.RegisterTask(task)
			go func(task *Task) { c.TaskChan <- task }(task)
		}
		c.Mutex.Unlock()
		time.Sleep(2 * time.Second)
	}
}
