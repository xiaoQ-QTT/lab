package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType int
type TaskStatus int

const (
	MAP    TaskType = 0
	REDUCE TaskType = 1
	OVER   TaskType = 2
	SLEEP  TaskType = 3

	IDLE       TaskStatus = 4
	PROCESSING TaskStatus = 5
	FINISH     TaskStatus = 6
)

type TaskOverReply struct{}

type RequestTaskArgs struct{}

type GetTaskReply struct {
	TaskType     TaskType    // MAP、REDUCE、OVER、SLEEP
	FileName     interface{} // 待处理文件名，map阶段为string类型，reduce阶段为[]string类型
	ReduceNumber int         // reduce任务数
	TaskKey      int         // map任务的key
}

type MapTaskOverArgs struct {
	Id          string
	FileNameMap map[int]string
	MapTaskKey  int
}

type ReduceTaskOverArgs struct {
	Id            string
	ReduceTaskKey int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
