package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "github.com/google/uuid"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type FetchTaskArgs struct {
	Msg    string
	NodeId uuid.UUID
}

type FetchTaskReply struct {
	Msg    string
	NodeId uuid.UUID
	Task   *Task
}

type SubmitTaskArgs struct {
	Msg    string
	NodeId uuid.UUID
	Task   *Task
}

type SubmitTaskReply struct {
	Msg    string
	NodeId uuid.UUID
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
