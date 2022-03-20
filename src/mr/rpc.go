package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"time"
)
import "strconv"

// 占位符
type Placeholder struct{}

type MapTask struct {
	TaskMeta
	Filename string
}

type ReduceTask struct {
	TaskMeta
	IntermediateFilenames []string
}

type TaskMeta struct {
	State     TaskState
	StartTime time.Time
	Id        int
}

type TaskState int

const (
	Pending   TaskState = iota // 等待
	Executing                  //执行
	Finished                   // 完成
)

type Task struct {
	Operation TaskOperation
	IsMap     bool
	NReduce   int
	Map       MapTask
	Reduce    ReduceTask
}

type TaskOperation int

const (
	ToWait TaskOperation = iota // 等待
	ToRun                       // 执行
)

type FinishArgs struct {
	IsMap bool
	Id    int
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
