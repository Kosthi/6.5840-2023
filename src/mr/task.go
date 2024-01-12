package mr

import (
	"log"
	"time"
)

// TaskType represents the type of MapReduce task.
type TaskType int

const (
	MapTask      TaskType = 0
	ReduceTask   TaskType = 1
	WaitingTask  TaskType = 2
	CompleteTask TaskType = 3
)

type TaskStatus int

const (
	TaskPending TaskStatus = iota
	TaskInProgress
	TaskCompleted
)

// Task represents a generic MapReduce task.
type Task struct {
	ID        int      // Task identifier
	Type      TaskType // Task type (Map or Reduce)
	InputFile []string // Input file for Map tasks
	NReduce   int      // Number of Reduce tasks
}

func NewReduceeTask(id int, inputFile []string) *Task {
	return &Task{
		ID:        id,
		Type:      ReduceTask,
		InputFile: inputFile,
	}
}

// NewMapTask creates a new Map task.
func NewMapTask(id int, inputFile []string, nReduce int) *Task {
	return &Task{
		ID:        id,
		Type:      MapTask,
		InputFile: inputFile,
		NReduce:   nReduce,
	}
}

type TaskMetaData struct {
	StartTime  time.Time
	TaskStatus TaskStatus
	Task       *Task
}

func NewTaskMetaData(task *Task) *TaskMetaData {
	return &TaskMetaData{
		TaskStatus: TaskPending,
		Task:       task,
	}
}

type TaskSet struct {
	mapTask    map[int]*TaskMetaData
	reduceTask map[int]*TaskMetaData
}

func NewTaskSet() *TaskSet {
	return &TaskSet{
		mapTask:    make(map[int]*TaskMetaData),
		reduceTask: make(map[int]*TaskMetaData),
	}
}

func (ts *TaskSet) RegisterTask(task *Task) {
	switch task.Type {
	case MapTask:
		ts.mapTask[task.ID] = NewTaskMetaData(task)
	case ReduceTask:
		ts.reduceTask[task.ID] = NewTaskMetaData(task)
	default:
		log.Fatalf("Cannot add unsupported task to TaskSet.")
	}
}

func (ts *TaskSet) StartTask(task *Task) {
	var taskMetaData *TaskMetaData
	switch task.Type {
	case MapTask:
		taskMetaData = ts.mapTask[task.ID]
	case ReduceTask:
		taskMetaData = ts.reduceTask[task.ID]
	default:
		log.Fatalf("Cannot get unsupported task from TaskSet.")
	}
	taskMetaData.StartTime = time.Now()
	taskMetaData.TaskStatus = TaskInProgress
}

func (ts *TaskSet) DoneTask(task *Task) bool {
	var taskMetaData *TaskMetaData
	switch task.Type {
	case MapTask:
		taskMetaData = ts.mapTask[task.ID]
	case ReduceTask:
		taskMetaData = ts.reduceTask[task.ID]
	default:
		log.Fatalf("Cannot get unsupported task from TaskSet.")
	}
	if taskMetaData.TaskStatus == TaskCompleted {
		log.Printf("Task alreay completed, thus result abandoned. Task: %v\n", taskMetaData)
		return false
	}
	taskMetaData.TaskStatus = TaskCompleted
	return true
}

func (ts *TaskSet) IfAllTaskDone(taskType TaskType) bool {
	var taskMap map[int]*TaskMetaData
	switch taskType {
	case MapTask:
		taskMap = ts.mapTask
	case ReduceTask:
		taskMap = ts.reduceTask
	default:
		log.Fatalf("Cannot get unsupported task from TaskSet.")
	}
	for _, taskInfo := range taskMap {
		if taskInfo.TaskStatus != TaskCompleted {
			return false
		}
	}
	return true
}

const TaskTimeOut = 10 * time.Second

func (ts *TaskSet) IfTimeOut(taskType TaskType) []*Task {
	var res []*Task
	var taskMap map[int]*TaskMetaData
	switch taskType {
	case MapTask:
		taskMap = ts.mapTask
	case ReduceTask:
		taskMap = ts.reduceTask
	default:
		log.Fatalf("Cannot get unsupported task from TaskSet.")
	}
	for _, taskInfo := range taskMap {
		if time.Since(taskInfo.StartTime) > TaskTimeOut && taskInfo.TaskStatus == TaskInProgress {
			res = append(res, taskInfo.Task)
		}
	}
	return res
}
